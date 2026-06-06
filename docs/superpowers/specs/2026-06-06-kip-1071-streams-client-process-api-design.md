# KIP-1071 streams client — `process` / `process_values` (custom Processor-API nodes in the DSL)

**Status:** design approved (brainstorm)
**Builds on:** #2 Processor API (`Processor`/`ProcessorSupplier`/`ProcessorContext`/`Record`/state stores), #4 DSL. Branches from `main` (independent of the open GlobalKTable PR #415; rebase onto post-#415 main when that merges).
**Ground truth:** Apache Kafka Streams 4.1 (Docker JVM capture) for byte-exact wire topology; KIP-820 semantics for `process`/`processValues`.

## 1. Goal

Let a DSL pipeline drop in a **custom Processor-API node** — the user's own `Processor` (or fixed-key processor) with connected state stores — via `KStream::process` / `KStream::process_values` (KIP-820), plus `StreamsBuilder::add_state_store` to register a connectable store. This bridges the typed DSL to the raw Processor API (already built in #2) and is a foundation for later interactive-query work.

## 2. Scope

### In scope
1. **`KStream::process<KOut, VOut, PS>(supplier, store_names)`** — a custom processor node that reads/writes connected state stores and forwards records, possibly changing the key. Result `KStream<KOut, VOut>` is **key-changing**.
2. **`KStream::process_values<VOut, PS>(supplier, store_names)`** — a **fixed-key** custom processor (cannot change the key, even by value). Result `KStream<K, VOut>` is **non-key-changing**.
3. **The FixedKey type system** (full, JVM-faithful): `FixedKeyRecord<K,V>`, `FixedKeyProcessor<KIn,VIn,VOut>`, `FixedKeyProcessorContext`, `FixedKeyProcessorSupplier` — implemented as a **typed facade over the existing `Processor`** machinery (an internal adapter), not a parallel runtime hierarchy.
4. **`StreamsBuilder::add_state_store<K,V,KS,VS>(name, key_serde, value_serde)`** — register a store the DSL can connect to a `process`/`process_values` node by name.
5. Two byte-exact goldens (`process`, `process_values`) + TopologyTestDriver execution.

### Non-goals (deferred)
- **Punctuation / `ProcessorContext::schedule`** (wall-clock / stream-time timer callbacks) — needs a runtime scheduler + stream-time tracking; its own slice.
- The **supplier-`stores()` auto-connect** form (KIP-401) — stores are connected explicitly by name.
- **Multi-store / multi-processor** connection topologies (one store connected to several processors). Single-connect this slice; multi-connect is a follow-up (via `connect_processor_store`).
- Foreign-key join, cogroup, record caching (separate slices).

## 3. FixedKey type system (facade over `Processor`)

The user-facing fixed-key surface is new *types*, but the runtime is the existing erased `Processor` machinery — an internal adapter bridges them, so the dispatch / graph driver / store access are reused.

```rust
// A record whose KEY is immutable: value/timestamp can change, the key cannot.
pub struct FixedKeyRecord<K, V> { pub key: K, pub value: V, pub timestamp: i64 }
impl<K, V> FixedKeyRecord<K, V> {
    pub fn with_value<V2>(self, value: V2) -> FixedKeyRecord<K, V2>;  // same key + timestamp
}

#[async_trait] pub trait FixedKeyProcessor<KIn: Send, VIn: Send, VOut: Send>: Send + 'static {
    async fn process(&mut self, ctx: &mut FixedKeyProcessorContext<'_, '_, KIn, VOut>, r: FixedKeyRecord<KIn, VIn>);
}
pub trait FixedKeyProcessorSupplier<KIn, VIn, VOut>: Send + Sync + 'static {
    fn get(&self) -> Box<dyn FixedKeyProcessor<KIn, VIn, VOut>>;
}   // + blanket `Fn() -> P` impl, mirroring ProcessorSupplier

// Wraps a regular ProcessorContext<K, VOut>; the only forward path re-attaches the key.
pub struct FixedKeyProcessorContext<'ctx, 'd, K, VOut> { inner: &'ctx mut ProcessorContext<'ctx, 'd, K, VOut> }
impl<'ctx,'d, K, VOut> FixedKeyProcessorContext<'ctx,'d, K, VOut> {
    pub fn forward(&mut self, r: FixedKeyRecord<K, VOut>);  // → inner.forward(Record::new(Some(r.key), r.value, r.timestamp))
    // store accessors (get_state_store/get_kv/…) + record_context delegate to `inner`.
}

// Internal bridge into the runtime: a FixedKeyProcessor IS a Processor<KIn,VIn,KIn,VOut>.
pub(crate) struct FixedKeyAdapter<P> { inner: P }
impl<P, KIn, VIn, VOut> Processor<KIn, VIn, KIn, VOut> for FixedKeyAdapter<P>
where P: FixedKeyProcessor<KIn, VIn, VOut>, … {
    async fn process(&mut self, ctx: &mut ProcessorContext<'_,'_, KIn, VOut>, r: Record<KIn, VIn>) {
        let k = r.key.expect("process_values requires a non-null key");
        let fkr = FixedKeyRecord { key: k, value: r.value, timestamp: r.timestamp };
        let mut fk_ctx = FixedKeyProcessorContext::new(ctx);
        self.inner.process(&mut fk_ctx, fkr).await;
    }
}
```

The no-key-change guarantee is structural: `FixedKeyProcessor` only ever receives/produces `FixedKeyRecord`, whose key is set once (from the input) and carried through `with_value`; the context's only `forward` re-attaches that key. The user cannot emit a different key.

## 4. DSL

`KStream<K, V>`:
```rust
pub fn process<KOut, VOut, PS>(&self, supplier: PS, store_names: impl IntoIterator<Item = impl Into<String>>) -> KStream<KOut, VOut>
where KOut: Any+Send+Sync+Clone, VOut: Any+Send+Clone, PS: ProcessorSupplier<K, V, KOut, VOut> + Clone + 'static;

pub fn process_values<VOut, PS>(&self, supplier: PS, store_names: impl IntoIterator<Item = impl Into<String>>) -> KStream<K, VOut>
where VOut: Any+Send+Clone, PS: FixedKeyProcessorSupplier<K, V, VOut> + Clone + 'static;
```

Lowering (mirrors the existing stateless-processor lowering):
- `process` → a `KSTREAM-PROCESSOR-` node wired to this stream's node; in the thunk, `state.topology.add_processor::<K, V, KOut, VOut, _, _, _>(name, supplier, [parent])`, then for each store name invoke the `add_state_store` thunk to register + connect the store to this node. Result `KStream` is **key-changing** (`key_changing = true`), so a downstream `group_by_key`/join inserts a repartition (conservative — `process` may rewrite the key).
- `process_values` → a `KSTREAM-PROCESSVALUES-` node; wrap the `FixedKeyProcessorSupplier` in `FixedKeyAdapter` and `add_processor::<K, V, K, VOut, _, _, _>(name, …)`. Result `KStream` is **non-key-changing** (`key_changing = false`).

`StreamsBuilder::add_state_store<K, V, KS, VS>(name, key_serde, value_serde)` records, in the `InternalStreamsBuilder`, a name → thunk `Fn(&mut LowerState, processor_name: &str)` that calls `Topology::add_state_store::<K,V,KS,VS>(name, ks, vs, [processor_name])` (a compacted KV changelog, like a materialized table store). `process`/`process_values` invoke the thunk per connected name during lowering. An added store that is never connected is a build-time error (matches Kafka's `InvalidTopologyException`); a connected store's changelog topic appears in the wire.

## 5. Wire / golden (capture-first)

`stream.process(supplier, "store").to("out")` with `builder.add_state_store("store", …)` → the JVM emits a `KSTREAM-PROCESSOR-000…` processor node + the store's `cleanup.policy=compact` changelog topic `app-store-changelog`. `process_values` → `KSTREAM-PROCESSVALUES-000…`. The processor node name + the store changelog are wire-visible; the node *kind* is not (the wire lists topics, not processor kinds), but the **name counter** + the connected-store changelog are. Resolved **capture-first**: capture the JVM topology for both before writing the Rust wire, match byte-for-byte.

Two new goldens (`process`, `process_values`); **all prior goldens stay byte-identical** (14 on this main-based branch — `process` is the next fixture; the count shifts by one if #415 merges first).

## 6. Slice decomposition (phases)

One feature, one spec, a phased plan, one PR:
- **P-i — `process` + `add_state_store`.** `KStream::process` + `StreamsBuilder::add_state_store` + the connect-by-name lowering + golden `process` + TestDriver execution (a stateful counter processor that reads/writes a connected store and forwards a transformed record; verify key-changing forces a downstream repartition).
- **P-ii — FixedKey + `process_values`.** The `FixedKeyRecord`/`FixedKeyProcessor`/`FixedKeyProcessorContext`/`FixedKeyProcessorSupplier` + `FixedKeyAdapter` + `KStream::process_values` + golden `process_values` + TestDriver execution (a fixed-key value-transform that keeps the key; verify non-key-changing → no repartition).

## 7. Testing

- **Goldens:** `process` + `process_values`, byte-exact vs JVM 4.1; all prior byte-identical.
- **TestDriver execution:** (P-i) a custom `Processor` using a connected store (e.g. a per-key counter) forwards transformed records; a `process(...).group_by_key().count()` inserts a repartition (key-changing). (P-ii) a `FixedKeyProcessor` value transform preserves the key; `process_values(...).group_by_key()` does NOT repartition (non-key-changing).
- **Unit:** `FixedKeyRecord::with_value` keeps key+timestamp; the adapter bridges a `FixedKeyProcessor` to the runtime; `add_state_store` + `process` connect the store (the processor reads it).

## 8. Error handling

- `process`/`process_values` referencing an unknown store name → build-time error (store not added).
- An `add_state_store` store never connected by a `process` call → build-time error (matches Kafka).
- A `process_values` record with a null key → panic (`process_values requires a non-null key`), consistent with the fixed-key contract.

## 9. Open question resolved capture-first

The exact `KSTREAM-PROCESSOR-`/`KSTREAM-PROCESSVALUES-` name-counter positions + the connected-store changelog naming in the KIP-1071 wire. P-i/P-ii each capture the JVM topology first and match it before writing the Rust wire.
