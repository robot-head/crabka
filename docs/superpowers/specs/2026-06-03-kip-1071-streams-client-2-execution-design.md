# KIP-1071 Streams Client — Sub-project #2: Processor API + stateless execution engine

**Date:** 2026-06-03
**Status:** Design approved, pending spec review
**Scope:** The second sub-project of the Crabka Streams client-runtime program.
**Builds on:** #1 (merged) — `docs/superpowers/specs/2026-06-03-kip-1071-streams-client-membership-design.md`

## 1. Context

Sub-project #1 (merged: PRs #373, #377) delivered the `crabka-client-streams`
crate — a `StreamsGroupHeartbeat` membership client + a byte-exact topology
builder. In #1, processors are **structural placeholders**: `NodeKind::Processor
{ predecessors }` carries no executable logic, and the topology only feeds the
wire `Topology`. #1 also exposes the seams #2 consumes: `BuiltTopology`,
`StreamsAssignment { active, standby, warmup }` / `TaskAssignment {
subtopology_id, partitions, source_topic_partitions }`, and the
`StreamsMembership` event stream (`StreamsEvent::Assigned/NotReady/Fenced`).

#2 makes records **flow**: a typed Processor API and a stateless execution
engine that fetches a task's source partitions, runs records through the
processor graph, produces to sink/repartition topics, and commits source offsets
(at-least-once). It extends the existing crate (no new crate).

Program roadmap (from #1 §1): #3 state stores + changelog, #4 DSL, #5
standby/warmup, #6 interactive queries, #7 EOS. #2 is the foundation #3/#4 build
on.

## 2. Goal and non-goals

### Goal

Extend `crabka-client-streams` so a Rust application can:

1. **Attach executable logic** to a topology via a typed Processor API
   (`Processor<KIn,VIn,KOut,VOut>`, `ProcessorContext::forward`, serdes at topic
   boundaries) — mirroring the JVM `org.apache.kafka.streams.processor.api`.
2. **Run it** as a managed `KafkaStreams` handle that joins the streams group
   (via #1), fetches owned active-task partitions, runs records through the
   graph, produces to sink/repartition topics, and commits source offsets at
   **at-least-once**, reacting correctly to rebalances.
3. **Test it deterministically** with a broker-free `TopologyTestDriver`
   (`pipe_input`/`read_output`).

### Non-goals (deferred)

- **State stores / changelog** — `ProcessorContext` has no store access (#3).
- **DSL** (KStream/KTable) — #4. (#2 is the Processor API the DSL builds on.)
- **Standby/warmup processing** — only **active** tasks run; standby/warmup
  assignments are ignored (#5).
- **Interactive queries** (#6).
- **EOS / transactions** — produce+commit is plain at-least-once (#7).
- **Punctuation / wall-clock & stream-time timers** — used mainly with stateful
  ops; deferred.
- **Cross-partition stream-time synchronization** — each partition is processed
  in offset order; matters for windowed/stateful ops (#3+).

## 3. Typing model and the graph (the core)

**Typed public API, `dyn Any` erasure internally** (mirrors the JVM's
`Object`-erased in-memory typed-record graph). Confirmed during brainstorming
over the alternatives (compile-time typestate — can't express the dynamic
by-name PAPI graph; Bytes-between-nodes — over-serializes and demands serdes for
all intermediate types).

### 3.1 Public traits (`processor/api.rs`, `record.rs`, `serde.rs`)

```rust
pub struct Record<K, V> { pub key: Option<K>, pub value: V, pub timestamp: i64 }

pub trait Serde<T>: Send + Sync + 'static {
    fn serialize(&self, value: &T) -> bytes::Bytes;
    fn deserialize(&self, bytes: &[u8]) -> Result<T, SerdeError>;
}
// Built-ins: BytesSerde, StringSerde, I64Serde, … (matching Serdes.String()/Long()).

pub trait Processor<KIn, VIn, KOut, VOut>: Send + 'static {
    fn init(&mut self, _ctx: &mut ProcessorContext<KOut, VOut>) {}
    fn process(&mut self, ctx: &mut ProcessorContext<KOut, VOut>, record: Record<KIn, VIn>);
    fn close(&mut self) {}
}
pub trait ProcessorSupplier<KIn, VIn, KOut, VOut>: Send + Sync + 'static {
    fn get(&self) -> Box<dyn Processor<KIn, VIn, KOut, VOut>>;   // one instance per task
}
// ProcessorContext<KOut,VOut>: forward(Record<KOut,VOut>), forward_to(child, record),
//   record_context() -> RecordContext { topic, partition, offset, timestamp }.
```

### 3.2 Erased records + driver-loop forwarding (`processor/erased.rs`, `graph.rs`)

- **`ErasedRecord`**: `{ key: Option<Box<dyn Any + Send>>, value: Box<dyn Any +
  Send>, timestamp }`, with a **Bytes fast-path** enum variant at source/sink
  boundaries to avoid needless boxing.
- **No recursive forwarding.** `ProcessorContext::forward` does **not** invoke
  children (that would alias `&mut` across nodes). It **appends** the erased
  output to a `forward_buffer: VecDeque<(child_node_idx, ErasedRecord)>` owned by
  the graph driver. The driver owns `nodes: Vec<ErasedNode>` + `children:
  Vec<Vec<usize>>` and runs, per input record:
  ```
  push (source_node_idx, erased); while let Some((idx, rec)) = buffer.pop():
      nodes[idx].process(&mut Ctx { buffer, children: &children[idx], record_ctx }, rec)
  ```
  Each node borrows only the buffer (never sibling nodes) → no aliasing; a
  DAG drained per record terminates; DFS in child-registration order preserves
  JVM semantics.
- **Three `ErasedNode` adapters:**
  - **Source** — holds `Serde<K>, Serde<V>`; deserializes incoming
    `(key_bytes, value_bytes)` → erases → pushes to children. Runs no user code.
  - **Processor** — wraps `Box<dyn Processor<KIn,VIn,KOut,VOut>>`; **downcasts**
    the erased input to `KIn,VIn`, runs `process`; the typed
    `ProcessorContext<KOut,VOut>` boxes each `forward` to erased for children.
  - **Sink** — holds `Serde<K>, Serde<V>` + topic; **downcasts** to `Record<K,V>`,
    serializes, emits `(topic, key_bytes, value_bytes)` to the output collector.
    No children.

Boxing/serialization happen exactly where the JVM does: deserialize→box at
sources, box at each `forward`, downcast→serialize at sinks/repartition.

### 3.3 Build-time wiring diagnostics (no compile-time check with PAPI)

The PAPI graph is assembled **by name at runtime**, so `rustc` cannot check
cross-node wiring types (literal compile-time safety only arrives with the typed
DSL, #4 — the JVM has the same limitation, throwing `ClassCastException` at
runtime). Instead, every `ErasedNode` records the `TypeId` + `type_name` of the
`Record<K,V>` it consumes and produces, and **`build()` eagerly validates every
parent→child edge**, returning a rich diagnostic on mismatch:

```rust
TopologyError::TypeMismatch {
    parent: String, parent_kind: NodeKind, produces: &'static str,   // type_name
    child:  String, child_kind:  NodeKind, expects:  &'static str,
}
```
whose `Display` reads like an `rustc` error:
```
topology wiring type error: sink `out` expects `Record<String, String>`,
  but its parent processor `upcase` forwards `Record<String, i64>`
  = help: a node's output type (KOut, VOut) must match every child's input type (KIn, VIn)
  = note: checked at build() because the Processor API wires nodes by name;
          use the typed DSL (sub-project #4) for compile-time wiring safety
```
The per-record runtime downcast remains only as an unreachable-in-practice
backstop (returns `ProcessorError`).

## 4. Builder evolution (`topology/builder.rs`)

Greenfield — the structural `add_*` methods become **typed** (no compat shim):

```rust
let mut topo = Topology::new();
topo.add_source("src", ["input-topic"], StringSerde, StringSerde);          // K=String, V=String
topo.add_processor("upcase", UpcaseSupplier, ["src"]);                       // Processor<String,String,String,String>
topo.add_sink("out", "output-topic", ["upcase"], StringSerde, StringSerde);
let built = topo.build("my-app")?;
```

- `add_source<K,V>(name, topics, key_serde, value_serde)`
- `add_processor<KIn,VIn,KOut,VOut>(name, supplier, predecessors)`
- `add_sink<K,V>(name, topic, predecessors, key_serde, value_serde)`
- `add_state_store` / `add_repartition_topic` retained from #1.

Serdes and processor logic are **runtime metadata** — they do **not** appear in
the `StreamsGroupHeartbeat.Topology` wire shape, so `build().to_wire()` is
byte-identical to #1 and the golden-frame test still passes. The builder stores
the erased node factories (`Box<dyn ErasedNodeFactory>`) keyed by name alongside
the existing structural node info (which still feeds #1's grouping/wire). `#1`'s
grouping (`grouping.rs`) and wire serialization (`wire.rs`) are **unchanged**.
`BuiltTopology` gains a runtime side (instantiable erased graph per subtopology)
alongside its existing wire side.

`#1`'s own builder call sites and tests (golden frame, integration, builder unit
tests) migrate to the typed API; structure (hence wire output) is unchanged.

## 5. Runtime (`runtime/`)

### 5.1 `StreamTask` (`task.rs`)

One per assigned **active** task `(subtopology_id, partition)`. Owns: an
instantiated erased graph for that subtopology (its own processor instances via
`ProcessorSupplier::get()` — per-task isolation), the fetch position per source
partition, and pending source offsets to commit.
- `process(batch)` — for each fetched record (offset order), feed `(topic,
  key_bytes, value_bytes, ts)` to the matching source node → drive the graph to
  completion → sinks serialize and **buffer** into the shared producer; advance
  the in-memory position.
- `commit()` — **flush the producer** (sink/repartition records durable), then
  `OffsetCommit` source positions under the streams group. Flush-before-commit =
  at-least-once.

### 5.2 `StreamThread` (`thread.rs`)

A single tokio task (num.stream.threads = 1 for #2) owning the active-task set.
Loop: round-robin `crabka_client_core::fetch_partition` each task's source
partitions at the tracked offset → `process` → every `commit.interval.ms`,
`commit()` all tasks. On task creation, seek to the committed offset
(`OffsetFetch`) or `auto.offset.reset` (earliest/latest) if none. Uses its own
`Client` connection(s) for fetch/produce/commit (separate from the heartbeat
connection — broker is serial per-connection).

### 5.3 Rebalance integration

Reacts to `StreamsEvent` from #1's membership:
- `Assigned(a)` → diff `a.active` vs current tasks: **close revoked** (flush +
  commit + `Processor::close()`), **create added** (build graph, seek to
  committed). `standby`/`warmup` **ignored** in #2.
- `Fenced` → flush+commit+close all tasks; rebuild on next `Assigned`.
- `NotReady(_)` → hold (no tasks until topics exist).

### 5.4 `KafkaStreams` handle (`app.rs`)

Owns the `StreamsMembership` (#1) + the `StreamThread` + the shared `Producer`.
- `start()` → joins the group, spawns a supervisor pumping `membership.next_event()`
  into the thread's task-management + the poll loop.
- `close()` → stop the thread (flush + commit + close all tasks), then
  `membership.close()` (leave, epoch −1).
- `state()` → Created / Running / Rebalancing / Error (observability).

Repartition needs **no special case**: a repartition sink produces to the
internal topic; the downstream subtopology's task consumes it as an ordinary
source (uniform per-task model).

## 6. TopologyTestDriver (`runtime/test_driver.rs`)

Synchronous, broker-free; the primary correctness gate. Built from a
`BuiltTopology`; instantiates the erased graph(s) at partition 0 with an
in-memory output collector. JVM-faithful typed handles:

```rust
let mut driver = TopologyTestDriver::new(built);
let mut input  = driver.input_topic::<String, String>("input-topic");   // serdes from the topology
let mut output = driver.output_topic::<String, String>("output-topic");
input.pipe("k", "hello", 0);
assert_eq!(output.read(), Some(("k".into(), "HELLO".into())));
```

`pipe` serializes via the source's serde, runs the graph to completion (§3.2),
sinks deserialize-back into the typed output queue. **Repartition is looped
internally**: a record produced to an internal repartition topic that is also a
source is fed back into the matching source node, so multi-subtopology
topologies are testable end-to-end in one driver (as the JVM
`TopologyTestDriver` does). Fully deterministic (no timers — stateless).

## 7. Testing strategy (gates)

1. **TopologyTestDriver unit tests** (primary) — map / filter / flatMap / branch
   (multi-child) / re-key+repartition topologies: pipe inputs, assert outputs;
   serde round-trips; **assert the build-time `TypeMismatch` diagnostic text** for
   a deliberately mis-wired graph.
2. **Erased-graph unit tests** — the forward-buffer driver loop, downcast
   adapters, multi-child child-order, source/sink (de)serialization, the
   build-time `TypeId` validation.
3. **In-process broker integration** — a `KafkaStreams` app against a real
   `crabka-broker` (reuse #1's harness + `streams.version` enablement): produce
   input to the source topic, run the app, assert the transformed records land on
   the sink topic and committed offsets advance, then clean `close()`. The only
   end-to-end (fetch→process→produce→commit) gate; runs under the existing
   **`client-streams-integration` codecov flag** (it's `--tests`, covering
   `runtime/`). See [[project-codecov-per-crate-integration-flag]].
4. **At-least-once / rebalance** (flagged) — restart-resumes-from-committed-offset
   and two-instance partition split; partial coverage acceptable for #2, fuller
   as a follow-on.

## 8. Open points to pin down in the plan

- **`ProcessorContext::forward_to(child, …)`** vs broadcast `forward` — confirm
  the JVM 4.x `forward(record, childName)` semantics and child-name resolution.
- **Headers** on `Record` — JVM has them; defer to keep #2 lean unless a sink/
  source needs them. Decide explicitly (lean toward omitting in #2).
- **Commit cadence + batch sizing** — `commit.interval.ms`, fetch max bytes;
  pick sane defaults matching the consumer client.
- **`auto.offset.reset` default** — match the consumer (`latest` is Kafka's
  default; Streams effectively uses `earliest` for app source topics — confirm and
  document).
- **Producer sharing** — one `Producer` shared across tasks vs per-task; one
  shared producer is simpler for at-least-once (per-task matters for EOS, #7).
- **Multiple source topics in one subtopology** — feed each source partition's
  records to its source node; #2 processes per-partition in offset order (no
  cross-source time merge — fine for stateless).

## 9. Success criteria

- `cargo test -p crabka-client-streams` green: TopologyTestDriver unit tests +
  erased-graph unit tests + the in-process broker integration test + the doctest.
- `cargo clippy --workspace --all-targets -- -D warnings` and
  `cargo fmt --check` clean.
- A documented `lib.rs` example: a small map/filter `KafkaStreams` app plus a
  `TopologyTestDriver` test.
- `#1`'s golden-frame byte test still passes (wire `Topology` unchanged after the
  builder migration).
