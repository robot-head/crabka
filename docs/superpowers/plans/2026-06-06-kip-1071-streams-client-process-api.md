# `process` / `process_values` (Processor-API nodes in the DSL) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `KStream::process` / `KStream::process_values` (custom Processor-API nodes with connected state stores) + `StreamsBuilder::add_state_store` to the DSL, with a full FixedKey type system for `process_values`.

**Architecture:** `process` lowers to a `KSTREAM-PROCESSOR-` node fed by a user `ProcessorSupplier`, connecting named stores registered via `add_state_store` (serde-carrying thunks stored on the builder, invoked + connected during lowering). `process_values` is the fixed-key variant: a `FixedKeyProcessor` (new typed surface) adapted to the existing `Processor` runtime by a thin `FixedKeyAdapter`, lowered to a `KSTREAM-PROCESSVALUES-` node. `process` is key-changing; `process_values` is not.

**Tech Stack:** Rust, `async-trait`; reuses #2 `Processor`/`ProcessorSupplier`/`ProcessorContext`/state stores, #4 DSL lowering, the JVM-capture golden harness.

**Branch / worktree:** `streams-process-api` (branched from `main`) in `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`. Spec: `docs/superpowers/specs/2026-06-06-kip-1071-streams-client-process-api-design.md`.

**Git discipline:** all git via `git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl …`; assert branch `== streams-process-api` before each commit; commit `-c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com"`; never `git config`; no push until asked. **Edit ONLY the worktree, never the main repo `/Users/mattstone/git/crabka/crates/...`.**

---

## File Structure

**New files:**
- `src/processor/fixed_key.rs` — `FixedKeyRecord` / `FixedKeyProcessor` / `FixedKeyProcessorContext` / `FixedKeyProcessorSupplier` / `FixedKeyAdapter`.
- `tests/testdata/golden/dsl/process.topology.json`, `tests/testdata/golden/dsl/process_values.topology.json` — goldens (capture-first).

**Modified files:**
- `src/dsl/names.rs` — `KSTREAM_PROCESSOR` / `KSTREAM_PROCESSVALUES` prefixes.
- `src/dsl/builder.rs` — `InternalStreamsBuilder.store_thunks` + `StreamsBuilder::add_state_store`.
- `src/dsl/graph.rs` — `LowerState` already carries `topology` + `handle_name`; no change expected (confirm).
- `src/dsl/kstream.rs` — `KStream::process` + `process_values`.
- `src/processor/mod.rs`, `src/processor/api.rs` — re-export FixedKey types; `ProcessorContext` accessor reuse.
- `src/lib.rs` — re-export the new public types.
- `tests/jvm-capture/.../Capture.java` + `run.sh` — fixtures.
- `tests/dsl_golden_frame.rs`, `tests/dsl_execution.rs`.

## Execution phases
- **P-i** (T1–T3): capture both goldens (controller) → `add_state_store` infra → `KStream::process` + golden + exec.
- **P-ii** (T4–T6): FixedKey types → `KStream::process_values` + golden + exec → docs + final verify.

T1 is **capture-first** (controller Docker). Golden indices are name-based; this main-based branch has 14 prior goldens.

---

## Task 1: JVM capture — `process` + `process_values` topologies (CONTROLLER, capture-first)

**Files:**
- Modify: `tests/jvm-capture/src/main/java/crabka/capture/Capture.java`, `tests/jvm-capture/run.sh`
- Create: `tests/testdata/golden/dsl/process.topology.json`, `tests/testdata/golden/dsl/process_values.topology.json`

- [ ] **Step 1: Add the two capture topologies.** In `Capture.java`, add (imports: `org.apache.kafka.streams.processor.api.*`, `org.apache.kafka.streams.state.Stores`, `org.apache.kafka.streams.state.StoreBuilder`, `org.apache.kafka.streams.state.KeyValueStore`):

```java
/** N. process: addStateStore + process(supplier, "store") -> to("out"). */
static Topology processTopology() {
    StreamsBuilder b = new StreamsBuilder();
    StoreBuilder<KeyValueStore<String,String>> sb = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore("store"), Serdes.String(), Serdes.String());
    b.addStateStore(sb);
    b.<String,String>stream("in", Consumed.with(Serdes.String(), Serdes.String()))
        .process(() -> new org.apache.kafka.streams.processor.api.ContextualProcessor<String,String,String,String>() {
            public void process(org.apache.kafka.streams.processor.api.Record<String,String> r) { context().forward(r); }
        }, "store")
        .to("out", Produced.with(Serdes.String(), Serdes.String()));
    return b.build(optimizedProps());
}

/** N+1. process_values: addStateStore + processValues(supplier, "store") -> to("out"). */
static Topology processValuesTopology() {
    StreamsBuilder b = new StreamsBuilder();
    StoreBuilder<KeyValueStore<String,String>> sb = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore("store"), Serdes.String(), Serdes.String());
    b.addStateStore(sb);
    b.<String,String>stream("in", Consumed.with(Serdes.String(), Serdes.String()))
        .processValues(() -> new org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor<String,String,String>() {
            public void process(org.apache.kafka.streams.processor.api.FixedKeyRecord<String,String> r) { context().forward(r); }
        }, "store")
        .to("out", Produced.with(Serdes.String(), Serdes.String()));
    return b.build(optimizedProps());
}
```
Register both: `write(outDir, "process", processTopology());` + `write(outDir, "process_values", processValuesTopology());`. Bump the "Wrote N fixtures" count (14 → 16) + the run.sh fixture-name list.

- [ ] **Step 2: Run the capture (CONTROLLER).**
```
cd /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl/crates/client-streams/tests/jvm-capture
./run.sh --gradle
```
Writes `process.topology.json` + `process_values.topology.json`.

- [ ] **Step 3: Read + record the shapes.** Inspect both:
```
python3 -c "import json;print(json.dumps(json.load(open('../testdata/golden/dsl/process.topology.json')),indent=1))"
python3 -c "import json;print(json.dumps(json.load(open('../testdata/golden/dsl/process_values.topology.json')),indent=1))"
```
Record: (a) the single subtopology's `source_topics` (`["in"]`) + `state_changelog_topics` (the connected store's changelog — likely `app-store-changelog`, `cleanup.policy=compact`); (b) whether `process`/`process_values` differ in the wire (they should be identical here — same source/sink/store; the node *kind* + name are not wire-visible). **These pin Tasks 3 + 5's golden expectations.**

- [ ] **Step 4: Commit.**
```
git -C <worktree> add crates/client-streams/tests/jvm-capture/ crates/client-streams/tests/testdata/golden/dsl/process.topology.json crates/client-streams/tests/testdata/golden/dsl/process_values.topology.json
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(streams): capture JVM process + process_values topologies (capture-first)"
```

---

## Task 2: `StreamsBuilder::add_state_store` + store thunks

**Files:**
- Modify: `src/dsl/builder.rs`, `src/dsl/names.rs`

- [ ] **Step 1: name prefixes** (`src/dsl/names.rs`):
```rust
/// JVM `KStream.process` node prefix.
pub(crate) const KSTREAM_PROCESSOR: &str = "KSTREAM-PROCESSOR-";
/// JVM `KStream.processValues` node prefix.
pub(crate) const KSTREAM_PROCESSVALUES: &str = "KSTREAM-PROCESSVALUES-";
```
(Confirm against the T1 capture if the wire ever exposes them — it does not here, but use the JVM prefixes for fidelity.)

- [ ] **Step 2: `store_thunks` on `InternalStreamsBuilder`** (`src/dsl/builder.rs`). Add a field + type alias:
```rust
/// A serde-carrying thunk that registers + connects a DSL-added state store to a
/// processor during lowering. `Arc<… + Send + Sync>` because the graph's lowering
/// thunks (which invoke it) are `Send`, and the captured serdes are `Send + Sync`.
pub(crate) type StoreConnectThunk =
    std::sync::Arc<dyn Fn(&mut crate::dsl::graph::LowerState, &str) + Send + Sync>;
```
On `InternalStreamsBuilder` add `pub store_thunks: std::collections::HashMap<String, StoreConnectThunk>` (init empty in `new`) + a helper `pub fn store_thunk(&self, name: &str) -> Option<StoreConnectThunk>` (clones the `Arc`).

- [ ] **Step 3: `StreamsBuilder::add_state_store`** (`src/dsl/builder.rs`). Register the thunk:
```rust
/// Register a state store the DSL can connect to a `process`/`process_values` node
/// by name. The store is registered + its changelog emitted when a `process` call
/// connects it (Kafka requires every added store to be connected).
pub fn add_state_store<K, V, KS, VS>(&self, name: impl Into<String>, key_serde: KS, value_serde: VS) -> &Self
where K: Any + Send + Sync + Clone, V: Any + Send + Clone, KS: Serde<K> + Clone + 'static, VS: Serde<V> + Clone + 'static {
    let name: String = name.into();
    let key = name.clone();                      // map key (the closure moves `name`)
    let thunk: StoreConnectThunk = std::sync::Arc::new(move |state, processor: &str| {
        state.topology.add_state_store::<K, V, KS, VS>(name.clone(), key_serde.clone(), value_serde.clone(), [processor.to_string()]);
    });
    self.internal.borrow_mut().store_thunks.insert(key, thunk);
    self
}
```
Read `Topology::add_state_store`'s real signature in `src/topology/builder.rs` and match the generics/serde-clone exactly.

- [ ] **Step 4: unit test** that `add_state_store` records a thunk (`internal.borrow().store_thunk("s").is_some()`). `cargo test -p crabka-client-streams --lib add_state_store` + clippy/fmt.

- [ ] **Step 5: Commit** `feat(streams-dsl): StreamsBuilder::add_state_store + store-connect thunks`.

---

## Task 3: `KStream::process` + golden + execution

**Files:**
- Modify: `src/dsl/kstream.rs`, `tests/dsl_golden_frame.rs`, `tests/dsl_execution.rs`, `src/lib.rs`

- [ ] **Step 1: `KStream::process`** (`src/dsl/kstream.rs`). Mirror `map_values`'s lowering (read it: mint name, `g.graph.add(name, GraphNodeKind::StatelessProcessor { repartition_required: false }, [parent])`, set `lower` thunk → `add_processor` + `handle_name.insert`). Differences: take a user supplier + store names; connect each store; result is **key-changing**.

```rust
pub fn process<KOut, VOut, PS>(&self, supplier: PS, store_names: impl IntoIterator<Item = impl Into<String>>) -> KStream<KOut, VOut>
where KOut: Any + Send + Sync + Clone, VOut: Any + Send + Clone,
      PS: crate::processor::api::ProcessorSupplier<K, V, KOut, VOut> + Clone + 'static {
    let stores: Vec<String> = store_names.into_iter().map(Into::into).collect();
    // Look up each store's connect thunk now (add_state_store must precede process).
    let thunks: Vec<crate::dsl::builder::StoreConnectThunk> = {
        let g = self.builder.borrow();
        stores.iter().map(|s| g.store_thunk(s)
            .unwrap_or_else(|| panic!("process references store '{s}' that was not added via add_state_store"))).collect()
    };
    let parent_id = self.node;
    let mut g = self.builder.borrow_mut();
    let name = g.new_processor_name(crate::dsl::names::KSTREAM_PROCESSOR);
    let id = g.graph.add(name.clone(), GraphNodeKind::StatelessProcessor { repartition_required: false }, vec![parent_id]);
    let sup = supplier.clone();
    g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
        let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
        let h = state.topology.add_processor::<K, V, KOut, VOut, _, _, _>(name.clone(), sup.clone(), [parent]);
        let proc_name = h.name().to_string();
        for t in &thunks { t(state, &proc_name); }   // register + connect each store
        state.handle_name.insert(id, proc_name);
    }));
    drop(g);
    // process MAY change the key → key-changing; source-topic lineage broken.
    KStream::new_with_key_changing(Rc::clone(&self.builder), id, true)
}
```
NOTE on the supplier bound: `add_processor` takes `S: ProcessorSupplier<…>`; the user passes `impl ProcessorSupplier + Clone` (a closure `|| MyProc` works via the blanket impl). Confirm `add_processor`'s exact signature in `src/topology/builder.rs` and that a cloned supplier satisfies it. Re-export nothing new for `process` (Processor/ProcessorSupplier already public).

- [ ] **Step 2: golden test** (`tests/dsl_golden_frame.rs`) `process_matches_jvm`:
```rust
#[test]
fn process_matches_jvm() {
    use crabka_client_streams::{Consumed, Processor, ProcessorContext, Record, Produced, StringSerde};
    struct Fwd;
    #[async_trait::async_trait]
    impl Processor<String,String,String,String> for Fwd {
        async fn process(&mut self, ctx: &mut ProcessorContext<'_,'_,String,String>, r: Record<String,String>) { ctx.forward(r); }
    }
    let b = StreamsBuilder::new();
    b.add_state_store::<String,String,_,_>("store", StringSerde, StringSerde);
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .process(|| Fwd, ["store"])
        .to("out", Produced::with(StringSerde, StringSerde));
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "process");
}
```
(Adjust `async_trait` import path / `Processor` re-export to the crate's actual surface — read `src/lib.rs` re-exports + an existing processor test for the exact form.) If the bytes differ, the bug is the store changelog name or the node counter — investigate; do NOT edit the fixture.

- [ ] **Step 3: execution tests** (`tests/dsl_execution.rs`): (a) a `process` with a per-key **counter** `Processor` that reads/writes the connected store + forwards the count — pipe records, assert the forwarded counts; (b) `process(...).group_by_key().count()` inserts a repartition (the result is key-changing) — assert the wire has a repartition topic (or that a subsequent group does not panic on the key-changing path; mirror how existing tests assert repartition).

- [ ] **Step 4: P-i verify + commit.** `cargo test -p crabka-client-streams` (all green; `process` golden byte-identical; 14 prior byte-identical), clippy `--all-targets -D warnings`, fmt. Commit `feat(streams-dsl): KStream::process + connected state stores + golden`.

---

## Task 4: FixedKey type system

**Files:**
- Create: `src/processor/fixed_key.rs`
- Modify: `src/processor/mod.rs`, `src/lib.rs`

- [ ] **Step 1: the types** (`src/processor/fixed_key.rs`). Read `src/processor/api.rs` for the exact `Processor`/`ProcessorContext`/`ProcessorSupplier` shapes + lifetimes, then write the FixedKey facade:

```rust
//! Fixed-key Processor API (KIP-820 `processValues`): a processor that may change
//! the value but NOT the key. A thin typed facade over the regular `Processor`
//! runtime — `FixedKeyAdapter` bridges a `FixedKeyProcessor` into `Processor<KIn,VIn,KIn,VOut>`.
use async_trait::async_trait;
use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;

/// A record whose KEY is immutable. `with_value` changes the value/keeps key + ts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedKeyRecord<K, V> { pub key: K, pub value: V, pub timestamp: i64 }
impl<K, V> FixedKeyRecord<K, V> {
    #[must_use] pub fn with_value<V2>(self, value: V2) -> FixedKeyRecord<K, V2> {
        FixedKeyRecord { key: self.key, value, timestamp: self.timestamp }
    }
}

/// Handed to a `FixedKeyProcessor`; the only `forward` re-attaches the (unchanged) key.
pub struct FixedKeyProcessorContext<'a, 'ctx, 'd, K, VOut> {
    inner: &'a mut ProcessorContext<'ctx, 'd, K, VOut>,
}
impl<'a, 'ctx, 'd, K: Send + 'static, VOut: Send + 'static> FixedKeyProcessorContext<'a, 'ctx, 'd, K, VOut> {
    pub(crate) fn new(inner: &'a mut ProcessorContext<'ctx, 'd, K, VOut>) -> Self { Self { inner } }
    pub fn forward(&mut self, r: FixedKeyRecord<K, VOut>) {
        self.inner.forward(Record::new(Some(r.key), r.value, r.timestamp));
    }
    // Delegate store access + record context to `inner`:
    pub fn get_state_store<K2: Send + Sync + 'static, V2: Send + 'static>(&mut self, name: &str)
        -> Option<&mut dyn crate::store::api::KeyValueStore<K2, V2>> { self.inner.get_state_store::<K2, V2>(name) }
    #[must_use] pub fn record_context(&self) -> &crate::processor::record::RecordContext { self.inner.record_context() }
}

#[async_trait]
pub trait FixedKeyProcessor<KIn: Send, VIn: Send, VOut: Send>: Send + 'static {
    async fn process(&mut self, ctx: &mut FixedKeyProcessorContext<'_, '_, '_, KIn, VOut>, record: FixedKeyRecord<KIn, VIn>);
}

/// Factory for `FixedKeyProcessor`s (one per task). Blanket `Fn() -> P` impl mirrors `ProcessorSupplier`.
pub trait FixedKeyProcessorSupplier<KIn, VIn, VOut>: Send + Sync + 'static {
    fn get(&self) -> Box<dyn FixedKeyProcessor<KIn, VIn, VOut>>;
}
impl<F, P, KIn, VIn, VOut> FixedKeyProcessorSupplier<KIn, VIn, VOut> for F
where F: Fn() -> P + Send + Sync + 'static, KIn: Send, VIn: Send, VOut: Send, P: FixedKeyProcessor<KIn, VIn, VOut> {
    fn get(&self) -> Box<dyn FixedKeyProcessor<KIn, VIn, VOut>> { Box::new(self()) }
}

/// Bridges a `FixedKeyProcessor` into the regular `Processor` runtime (KOut = KIn).
pub(crate) struct FixedKeyAdapter<P> { pub inner: P }
#[async_trait]
impl<P, KIn, VIn, VOut> Processor<KIn, VIn, KIn, VOut> for FixedKeyAdapter<P>
where KIn: Send + 'static, VIn: Send + 'static, VOut: Send + 'static, P: FixedKeyProcessor<KIn, VIn, VOut> {
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, KIn, VOut>, r: Record<KIn, VIn>) {
        let key = r.key.expect("process_values requires a non-null key");
        let fkr = FixedKeyRecord { key, value: r.value, timestamp: r.timestamp };
        let mut fk = FixedKeyProcessorContext::new(ctx);
        self.inner.process(&mut fk, fkr).await;
    }
}
```
**Lifetimes are illustrative** — the real `ProcessorContext` is `ProcessorContext<'ctx,'d,KOut,VOut>` with two lifetimes + store accessors taking `&mut self`. Adjust `FixedKeyProcessorContext`'s lifetimes/bounds + the delegated accessor set to whatever compiles against the actual `ProcessorContext` API (read it). The delegated accessors should cover at least `get_state_store`/`get_kv` + `record_context` (match what `ProcessorContext` exposes; a fixed-key processor only needs reads/writes + forward).

Register `pub mod fixed_key;` in `src/processor/mod.rs`. Re-export `FixedKeyRecord`, `FixedKeyProcessor`, `FixedKeyProcessorContext`, `FixedKeyProcessorSupplier` from `src/lib.rs`.

- [ ] **Step 2: unit tests** (in `fixed_key.rs`): `with_value` keeps key + timestamp; a `FixedKeyProcessor` driven through `FixedKeyAdapter` over a `ProcessorContext` (mirror an existing processor test's dispatch construction) forwards a `Record` with the SAME key + the transformed value. `cargo test -p crabka-client-streams --lib fixed_key` + clippy/fmt.

- [ ] **Step 3: Commit** `feat(streams): FixedKey Processor API (FixedKeyRecord/Processor/Context/Supplier + adapter)`.

---

## Task 5: `KStream::process_values` + golden + execution

**Files:**
- Modify: `src/dsl/kstream.rs`, `tests/dsl_golden_frame.rs`, `tests/dsl_execution.rs`

- [ ] **Step 1: `KStream::process_values`** (`src/dsl/kstream.rs`). Like `process` (Task 3) but: take a `FixedKeyProcessorSupplier`, wrap it in `FixedKeyAdapter` for `add_processor::<K,V,K,VOut,_,_,_>`, mint `KSTREAM_PROCESSVALUES`, and the result is **non-key-changing**.

```rust
pub fn process_values<VOut, PS>(&self, supplier: PS, store_names: impl IntoIterator<Item = impl Into<String>>) -> KStream<K, VOut>
where VOut: Any + Send + Clone, PS: crate::processor::fixed_key::FixedKeyProcessorSupplier<K, V, VOut> + Clone + 'static {
    let stores: Vec<String> = store_names.into_iter().map(Into::into).collect();
    let thunks: Vec<_> = { let g = self.builder.borrow();
        stores.iter().map(|s| g.store_thunk(s).unwrap_or_else(|| panic!("process_values references store '{s}' not added via add_state_store"))).collect::<Vec<_>>() };
    let parent_id = self.node;
    let mut g = self.builder.borrow_mut();
    let name = g.new_processor_name(crate::dsl::names::KSTREAM_PROCESSVALUES);
    let id = g.graph.add(name.clone(), GraphNodeKind::StatelessProcessor { repartition_required: false }, vec![parent_id]);
    let sup = supplier.clone();
    g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
        let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
        // Wrap the FixedKey supplier → a regular ProcessorSupplier producing FixedKeyAdapter.
        let sup2 = sup.clone();
        let h = state.topology.add_processor::<K, V, K, VOut, _, _, _>(
            name.clone(),
            move || crate::processor::fixed_key::FixedKeyAdapter { inner: sup2.get() },
            [parent],
        );
        let proc_name = h.name().to_string();
        for t in &thunks { t(state, &proc_name); }
        state.handle_name.insert(id, proc_name);
    }));
    drop(g);
    // process_values keeps the key → NOT key-changing; carry source-topic lineage.
    KStream::new_with_key_changing(Rc::clone(&self.builder), id, self.key_changing)
        .with_source_topic(self.source_topic.clone())
}
```
NOTE: `FixedKeyAdapter { inner: sup2.get() }` — `sup2.get()` returns `Box<dyn FixedKeyProcessor>`, and `FixedKeyAdapter<Box<dyn FixedKeyProcessor<…>>>` must impl `Processor` — confirm the `Box<dyn FixedKeyProcessor>` itself impls `FixedKeyProcessor` (add a blanket `impl FixedKeyProcessor for Box<dyn FixedKeyProcessor>` in `fixed_key.rs` if needed, mirroring how `Box<dyn Processor>` is handled in `api.rs`). Adjust until it compiles.

- [ ] **Step 2: golden test** (`tests/dsl_golden_frame.rs`) `process_values_matches_jvm` — mirror Task 3 Step 2 but `.process_values(|| FixedFwd, ["store"])` with a `FixedKeyProcessor` that forwards `r` unchanged. `assert_matches_fixture(&wire, "process_values")`.

- [ ] **Step 3: execution tests** (`tests/dsl_execution.rs`): (a) a `FixedKeyProcessor` value transform (e.g. uppercase the value) preserves the key — pipe `("k","v")`, assert output `("k","V")`; (b) `process_values(...).group_by_key()` does NOT insert a repartition (non-key-changing) — assert no repartition topic in the wire (contrast with Task 3's `process` case).

- [ ] **Step 4: verify + commit.** `cargo test -p crabka-client-streams` (process_values golden byte-identical; all prior byte-identical), clippy/fmt. Commit `feat(streams-dsl): KStream::process_values (fixed-key) + golden + execution`.

---

## Task 6: docs + final verification

**Files:**
- Modify: `src/lib.rs`

- [ ] **Step 1: docs.** `lib.rs` prose: `process` / `process_values` (custom Processor-API nodes), `add_state_store` (connectable stores), the FixedKey types + the no-key-change guarantee, key-changing vs non-key-changing. Mirror the existing DSL prose style.

- [ ] **Step 2: final verify.** `cargo test -p crabka-client-streams` + `--doc` + `cargo clippy --all-targets -D warnings` + `cargo fmt --check`. All green; `process` + `process_values` goldens byte-identical + 14 prior byte-identical. Commit `test(streams): process-api docs + final verification`.

---

## Done criteria
- `KStream::process` (key-changing custom processor + connected stores) + `KStream::process_values` (fixed-key) work; `StreamsBuilder::add_state_store` registers + connects stores.
- FixedKey type system (`FixedKeyRecord`/`FixedKeyProcessor`/`FixedKeyProcessorContext`/`FixedKeyProcessorSupplier`) via the `FixedKeyAdapter` facade.
- `process` + `process_values` goldens byte-match JVM 4.1; **14 prior goldens byte-identical**.
- Full suite + doctests + clippy `--all-targets -D warnings` + fmt green.

## Notes for the implementer
- **T1 is capture-first** — read the captured `process.topology.json`/`process_values.topology.json` before asserting golden expectations; the node *kind*/name is not wire-visible but the connected-store changelog topic is.
- The store-connect thunk MUST be `Arc<… + Send + Sync>` (the graph lowering thunks are `Send`; serdes are `Send + Sync` via the `Serde` supertrait).
- `add_state_store` must be called BEFORE `process`/`process_values` for the same store (the thunk is looked up at `process` construction time). Single-connect this slice; multi-connect (one store, many processors) is a follow-up.
- `process` is key-changing (result `key_changing = true`, source-topic lineage dropped); `process_values` is not (carry `self.key_changing` + `self.source_topic`).
- **Scope deviation from the spec:** the spec lists "an added-but-unconnected store is a build error (matches Kafka)". This slice **defers** that check — an `add_state_store` store that no `process`/`process_values` connects is simply never registered (its thunk goes unused; no changelog, no runtime store). This is harmless (the store just doesn't exist) and avoids a build-time connected-set pass; the unconnected-store error is a follow-up. Note this in the final-review/PR.
