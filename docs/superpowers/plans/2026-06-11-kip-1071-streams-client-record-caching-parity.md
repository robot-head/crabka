# Record-cache parity for `cogroup` + `to_table` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable `statestore.cache.max.bytes` write-back caching on the two materialized stores Kafka caches but Crabka currently forces uncached — the `KStream.to_table` store and the cogroup result store (all four kinds) — closing the last two Kafka record-cache parity gaps.

**Architecture:** No new cache machinery. The `TupleForwarder` forward-suppression seam, cache-aware `Backing::{Plain,Cached}` stores, `StateStore::flush_cache_into`, and `cache_owner`-rooted flush all exist from the merged record-caching slice (#491). This plan (a) gives `KStreamToTableProcessor` the `TupleForwarder` every other KTable materializer already has, and (b) flips `mark_store_caching(store, false)` → `mark_store_caching(store, caching)` at five DSL lowering sites. cogroup's per-input aggregate processors already suppress when their store is cached and the merge passthrough only relays, so caching the cogroup store is a mark-flip plus tests.

**Tech Stack:** Rust 2024, `crabka-client-streams`, `assert2::check`, `tokio::test` / `pollster::block_on`, in-memory `TopologyTestDriver`-style `Graph` instantiation.

**Spec:** `docs/superpowers/specs/2026-06-11-kip-1071-streams-client-record-caching-parity-design.md`

---

## File / responsibility map

| File | Responsibility | Tasks |
|------|----------------|-------|
| `crates/client-streams/src/dsl/processors/table.rs` | `KStreamToTableProcessor`: add `TupleForwarder` + suppression; processor unit tests | Task 1 |
| `crates/client-streams/src/dsl/kstream.rs` | `to_table_explicit`: construct processor with forwarder, flip the caching mark; in-crate behavioral test | Task 2 |
| `crates/client-streams/src/dsl/cogrouped.rs` | non-windowed cogroup: flip mark; cross-input read-your-writes behavioral test | Task 3 |
| `crates/client-streams/src/dsl/time_windowed_cogrouped.rs`, `session_windowed_cogrouped.rs`, `sliding_windowed_cogrouped.rs` | windowed/session/sliding cogroup: add mark; per-kind mark tests | Task 4 |
| (verification only) | combined fmt/clippy/test/build + disabled-path regression | Task 5 |

## Execution batching

Per CLAUDE.md, dispatch the disjoint-file tasks in parallel:

- **Batch 1 (parallel — 3 agents, disjoint files):** Task 1 (`table.rs`), Task 3 (`cogrouped.rs`), Task 4 (3 windowed-cogroup files).
- **Batch 2 (after Task 1 — its behavioral test needs the new forwarder):** Task 2 (`kstream.rs`).
- **Batch 3:** Task 5 (combined verification gate).

> Reviewer note: parallel agents in the same worktree do **not** cross-verify combined state — Task 5's combined gate is mandatory after all implementation tasks land.

---

## Task 1: `KStreamToTableProcessor` — add `TupleForwarder` suppression

**Files:**
- Modify: `crates/client-streams/src/dsl/processors/table.rs` (struct + impl at lines 79-107; new tests in the existing `#[cfg(test)] mod tests`)

`table.rs` already imports `TupleForwarder` (line 26) and the test module already has `source_registry(cached)` (a `"tbl"` `String→i64` store, optionally cached), `rc()`, `Dispatch`, and `flush_cache_into` helpers — reuse them.

- [ ] **Step 1: Write the failing processor tests**

Add to the `#[cfg(test)] mod tests` block in `table.rs` (after the existing `cached_source_suppresses_immediate_forward` test):

```rust
// ── KStream.to_table cache suppression ───────────────────────────────────

/// Run `init` then two same-key `process` calls through the to_table
/// processor, returning how many records reached the downstream buffer.
async fn to_table_run_two(stores: &mut StoreRegistry) -> usize {
    let children = [0usize];
    let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
    let mut output = Vec::new();
    let rc = rc();
    let mut proc = KStreamToTableProcessor::<String, i64> {
        store_name: "tbl".into(),
        forwarder: TupleForwarder::default(),
        _pd: PhantomData,
    };
    for v in 1..3i64 {
        let globals = crate::runtime::global::GlobalStateManager::default();
        let mut scheds = Vec::new();
        let mut dispatch = Dispatch {
            buffer: &mut buffer,
            children: &children,
            output: &mut output,
            record_ctx: &rc,
            stores,
            globals: &globals,
            node_idx: 0,
            schedules: &mut scheds,
            sched_stream_time: i64::MIN,
            sched_wall_clock: 0,
        };
        let mut ctx = ProcessorContext::<'_, '_, String, Change<i64>>::new(&mut dispatch);
        if v == 1 {
            proc.init(&mut ctx).await;
        }
        proc.process(&mut ctx, Record::new(Some("k".into()), v, v))
            .await;
    }
    buffer.len()
}

/// Uncached → forwards each record immediately (today's behavior): two
/// records → two forwards.
#[tokio::test]
async fn uncached_to_table_forwards_each_record() {
    let mut stores = source_registry(false);
    check!(to_table_run_two(&mut stores).await == 2);
}

/// Cached → the immediate forward is suppressed; the cache buffers the dirty
/// entry and flushing emits exactly ONE deduped change. Read-your-writes: the
/// cached store reflects the latest value (2) before flush.
#[tokio::test]
async fn cached_to_table_suppresses_immediate_forward() {
    let mut stores = source_registry(true);
    check!(to_table_run_two(&mut stores).await == 0);
    check!(stores.kv_is_cached("tbl"));
    let store = stores.get_kv::<String, i64>("tbl").unwrap();
    check!(store.get(&"k".to_string()).await == Some(2));
    let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
    stores
        .get_mut("tbl")
        .unwrap()
        .flush_cache_into(&mut buffer, &[0])
        .await;
    check!(buffer.len() == 1);
}
```

- [ ] **Step 2: Run the tests to verify they fail (compile error)**

Run: `cargo test -p crabka-client-streams --lib dsl::processors::table::tests::cached_to_table -- --nocapture`
Expected: FAIL — `KStreamToTableProcessor` has no field `forwarder` and no method `init` (compile error E0560/E0599).

- [ ] **Step 3: Add the `forwarder` field + `init` + suppression to the processor**

In `table.rs`, change the struct (currently lines 79-83):

```rust
#[allow(dead_code)]
pub(crate) struct KStreamToTableProcessor<K, V> {
    pub store_name: String,
    pub forwarder: TupleForwarder,
    pub _pd: Marker<(K, V)>,
}
```

And replace the impl body (currently lines 85-107) with:

```rust
#[async_trait]
impl<K, V> Processor<K, V, K, Change<V>> for KStreamToTableProcessor<K, V>
where
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Clone,
{
    async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, K, Change<V>>) {
        self.forwarder = TupleForwarder::resolve(ctx.store_is_cached(&self.store_name));
    }

    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, Change<V>>, r: Record<K, V>) {
        let key = r.key.expect("to_table requires a non-null key");
        // Stash the source record context BEFORE the store borrow so a cached
        // store attaches it to the deduped change it forwards on flush.
        let rc = ctx.record_context().clone();
        let old = {
            let store = ctx
                .get_state_store::<K, V>(&self.store_name)
                .expect("to_table store not found");
            store.set_record_context(rc);
            let old = store.get(&key).await;
            store.put(key.clone(), r.value.clone()).await;
            old
        };
        self.forwarder
            .maybe_forward(ctx, key, old, r.value, r.timestamp);
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p crabka-client-streams --lib dsl::processors::table::tests -- --nocapture`
Expected: PASS — all `table::tests` (existing + the two new `*_to_table_*`).

- [ ] **Step 5: Commit**

```bash
git add crates/client-streams/src/dsl/processors/table.rs
git commit -m "feat(client-streams): to_table processor suppresses forward when store is cached"
```

---

## Task 2: `to_table_explicit` — construct the forwarder + flip the caching mark

**Files:**
- Modify: `crates/client-streams/src/dsl/kstream.rs` (`to_table_explicit`, lines 1867-1947; add an in-crate test module at end of file)

> Depends on Task 1 (the behavioral test asserts emit-dedup, which needs the new `forwarder`).

`KStream::to_table` (default-serde, line 1950) delegates to `to_table_explicit`, so only `to_table_explicit` changes.

- [ ] **Step 1: Write the failing behavioral + mark tests**

Append a test module to the end of `kstream.rs`:

```rust
#[cfg(test)]
mod to_table_caching_tests {
    use assert2::check;

    use crate::dsl::StreamsBuilder;
    use crate::store::backend::StoreBackend;
    use crate::{I64Serde, Materialized, Produced, StringSerde};

    /// Caching ON: the to_table store is marked cached (`cache_owner` rooted),
    /// two same-key updates are suppressed until flush, and the flush emits a
    /// single deduped record carrying the latest value.
    #[test]
    fn to_table_caches_marks_and_dedups_emit() {
        let b = StreamsBuilder::new();
        b.stream::<String, i64>(["in"])
            .to_table_explicit(Materialized::with(StringSerde, I64Serde).as_store("t"))
            .to_stream()
            .to_explicit("out", Produced::with(StringSerde, I64Serde));
        let built = b.build("app").unwrap();
        let mut g =
            pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app", 1024)).unwrap();
        check!(g.cache_owner.contains_key("t"));
        pollster::block_on(g.init_processors()).unwrap();

        // Two same-key updates: 7 @0 then 9 @1.
        pollster::block_on(g.pipe("in", Some(b"k"), &7i64.to_be_bytes(), 0)).unwrap();
        pollster::block_on(g.pipe("in", Some(b"k"), &9i64.to_be_bytes(), 1)).unwrap();
        // Suppressed: nothing forwarded downstream until the cache flushes.
        check!(g.take_output().is_empty());

        pollster::block_on(g.flush_caches()).unwrap();
        let out = g.take_output();
        check!(out.len() == 1);
        check!(out[0].topic == "out");
        // to_stream forwards the deduped `new` value = 9 (BE i64).
        check!(out[0].value.as_ref().unwrap().as_ref() == 9i64.to_be_bytes());
    }

    /// `with_caching(false)`: the store is NOT cached even with a positive
    /// budget (mark opted out → absent from `cache_owner`).
    #[test]
    fn to_table_uncached_when_caching_off() {
        let b = StreamsBuilder::new();
        b.stream::<String, i64>(["in"])
            .to_table_explicit(
                Materialized::with(StringSerde, I64Serde)
                    .as_store("t")
                    .with_caching(false),
            )
            .to_stream()
            .to_explicit("out", Produced::with(StringSerde, I64Serde));
        let built = b.build("app").unwrap();
        let g =
            pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app", 1024)).unwrap();
        check!(!g.cache_owner.contains_key("t"));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p crabka-client-streams --lib to_table_caching_tests -- --nocapture`
Expected: FAIL — `to_table_caches_marks_and_dedups_emit` panics at `check!(g.cache_owner.contains_key("t"))` (store never marked cached) and the construction of `KStreamToTableProcessor` in `kstream.rs` won't compile until the `forwarder` field is supplied (see Step 3).

- [ ] **Step 3: Add the import, construct the forwarder, destructure `caching`, and flip the mark**

In `kstream.rs`, add to the imports near the other processor imports:

```rust
use crate::dsl::processors::tuple_forwarder::TupleForwarder;
```

In `to_table_explicit`, change the `Materialized` destructure (currently lines 1886-1891) to capture `caching`:

```rust
let Materialized {
    key_serde,
    value_serde,
    logging,
    caching,
    ..
} = materialized;
```

Construct the processor with a default forwarder (currently lines 1911-1914):

```rust
move || KStreamToTableProcessor {
    store_name: store_for_proc.clone(),
    forwarder: TupleForwarder::default(),
    _pd: PhantomData,
},
```

And immediately after the `if logging { … } else { … }` store-registration block (currently lines 1920-1935), before `state.handle_name.insert(id, …)`, add:

```rust
// Mark the store cached per `Materialized::with_caching` (default true);
// the to_table processor's TupleForwarder suppresses immediate forwards
// when cached and the cache flush forwards the deduped change.
state.topology.mark_store_caching(&store_for_thunk, caching);
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p crabka-client-streams --lib to_table_caching_tests -- --nocapture`
Expected: PASS — both tests.

- [ ] **Step 5: Commit**

```bash
git add crates/client-streams/src/dsl/kstream.rs
git commit -m "feat(client-streams): cache the KStream.to_table store (KIP-1071 record-cache parity)"
```

---

## Task 3: non-windowed cogroup — flip the mark + cross-input read-your-writes test

**Files:**
- Modify: `crates/client-streams/src/dsl/cogrouped.rs` (mark site line 337; add an in-crate test module)

The per-input `KStreamAggregateProcessor`s already carry a `TupleForwarder` and suppress when the store is cached; `cache_owner` roots the flush at the first aggregate node whose child is the merge passthrough. So the only production change is the mark flip. The test proves the unique cogroup property: **cross-input read-your-writes** — input B's aggregator reads input A's not-yet-flushed accumulator within one batch.

- [ ] **Step 1: Write the failing behavioral test**

Append a test module to the end of `cogrouped.rs`:

```rust
#[cfg(test)]
mod cogroup_caching_tests {
    use assert2::check;

    use crate::dsl::StreamsBuilder;
    use crate::store::backend::StoreBackend;
    use crate::{I64Serde, Materialized, Produced, StringSerde};

    /// Two co-grouped inputs aggregating into one cached KV store. Within a
    /// single batch, in1 adds `len(value)` and in2 adds 1; the cached store is
    /// marked (`cache_owner` rooted), both per-input forwards are suppressed,
    /// and flush emits ONE deduped record whose value (3 = 2 + 1) proves in2's
    /// aggregator read in1's buffered accumulator (cross-input read-your-writes).
    #[test]
    fn cogroup_caches_marks_and_dedups_cross_input() {
        let b = StreamsBuilder::new();
        let g1 = b.stream::<String, String>(["in1"]).group_by_key();
        let g2 = b.stream::<String, String>(["in2"]).group_by_key();
        g1.cogroup::<i64, _>(|_k, v: &String, acc| acc + i64::try_from(v.len()).unwrap_or(i64::MAX))
            .cogroup(g2, |_k, _v: &String, acc| acc + 1)
            .aggregate_explicit(
                || 0i64,
                Materialized::with(StringSerde, I64Serde).as_store("cg"),
            )
            .to_stream()
            .to_explicit("out", Produced::with(StringSerde, I64Serde));
        let built = b.build("app").unwrap();
        let mut g =
            pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app", 1024)).unwrap();
        check!(g.cache_owner.contains_key("cg"));
        pollster::block_on(g.init_processors()).unwrap();

        // in1: key "a" value "xx" (len 2) → acc 2 ; in2: key "a" value "z" → acc 3.
        pollster::block_on(g.pipe("in1", Some(b"a"), b"xx", 0)).unwrap();
        pollster::block_on(g.pipe("in2", Some(b"a"), b"z", 1)).unwrap();
        // Both per-input forwards suppressed until flush.
        check!(g.take_output().is_empty());

        pollster::block_on(g.flush_caches()).unwrap();
        let out = g.take_output();
        check!(out.len() == 1);
        check!(out[0].topic == "out");
        // 3 = in1(+2) then in2(+1) — in2 read in1's buffered accumulator.
        check!(out[0].value.as_ref().unwrap().as_ref() == 3i64.to_be_bytes());
    }

    /// `with_caching(false)`: the cogroup store stays uncached even with budget.
    #[test]
    fn cogroup_uncached_when_caching_off() {
        let b = StreamsBuilder::new();
        let g1 = b.stream::<String, String>(["in1"]).group_by_key();
        let g2 = b.stream::<String, String>(["in2"]).group_by_key();
        g1.cogroup::<i64, _>(|_k, v: &String, acc| acc + i64::try_from(v.len()).unwrap_or(i64::MAX))
            .cogroup(g2, |_k, _v: &String, acc| acc + 1)
            .aggregate_explicit(
                || 0i64,
                Materialized::with(StringSerde, I64Serde)
                    .as_store("cg")
                    .with_caching(false),
            )
            .to_stream()
            .to_explicit("out", Produced::with(StringSerde, I64Serde));
        let built = b.build("app").unwrap();
        let g =
            pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app", 1024)).unwrap();
        check!(!g.cache_owner.contains_key("cg"));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p crabka-client-streams --lib cogroup_caching_tests -- --nocapture`
Expected: FAIL — `cogroup_caches_marks_and_dedups_cross_input` panics at `check!(g.cache_owner.contains_key("cg"))` (mark is hard-`false`).

- [ ] **Step 3: Flip the mark**

In `cogrouped.rs`, replace the deferral block (currently lines 332-337):

```rust
            // cogroup caching deferred — needs suppression on the cogroup forward
            // path (the merge passthrough forwards unconditionally, so a cached
            // store would double-emit: once on the immediate forward, once on the
            // flush of the deduped change). Uncached = correct emit-on-update.
            let _ = caching;
            state.topology.mark_store_caching(&store_for_reg, false);
```

with:

```rust
            // Per-input aggregators suppress their immediate forward when this
            // store is cached; the merge passthrough then only relays the cache
            // flush's deduped change. Honor Materialized::with_caching.
            state.topology.mark_store_caching(&store_for_reg, caching);
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p crabka-client-streams --lib cogroup_caching_tests -- --nocapture`
Expected: PASS — both tests.

- [ ] **Step 5: Commit**

```bash
git add crates/client-streams/src/dsl/cogrouped.rs
git commit -m "feat(client-streams): cache the non-windowed cogroup store (record-cache parity)"
```

---

## Task 4: windowed / session / sliding cogroup — add the mark + per-kind tests

**Files:**
- Modify: `crates/client-streams/src/dsl/time_windowed_cogrouped.rs` (registrar ~line 87)
- Modify: `crates/client-streams/src/dsl/session_windowed_cogrouped.rs` (registrar ~line 101)
- Modify: `crates/client-streams/src/dsl/sliding_windowed_cogrouped.rs` (registrar ~line 101)
- Each: add a small in-crate test module asserting the mark.

These registrars currently never call `mark_store_caching`, so their window/session stores default uncached. cogroup always uses emit-on-update, so the windowed aggregate processors' `caching && emit.is_on_update()` suppression gate is satisfied — marking is sufficient. Each registrar already passes the correct `window_size_ms` to `add_window_store` (sliding passes `window_size = time_difference_ms` distinct from the `2×` retention basis), so the flush reconstructs `Windowed<K>` at the right size.

- [ ] **Step 1: Write the failing per-kind mark tests**

Append to the end of `time_windowed_cogrouped.rs`:

```rust
#[cfg(test)]
mod caching_tests {
    use assert2::check;

    use crate::dsl::StreamsBuilder;
    use crate::store::backend::StoreBackend;
    use crate::{I64Serde, Materialized, Produced, StringSerde, TimeWindows};

    #[test]
    fn time_windowed_cogroup_marks_store_cached() {
        let b = StreamsBuilder::new();
        let g1 = b.stream::<String, String>(["in1"]).group_by_key();
        let g2 = b.stream::<String, String>(["in2"]).group_by_key();
        g1.cogroup::<i64, _>(|_k, v: &String, acc| acc + i64::try_from(v.len()).unwrap_or(i64::MAX))
            .cogroup(g2, |_k, _v: &String, acc| acc + 1)
            .windowed_by(TimeWindows::of_size(100))
            .aggregate_explicit(
                || 0i64,
                Materialized::with(StringSerde, I64Serde).as_store("cg"),
            )
            .to_stream()
            .to_explicit("out", Produced::with(StringSerde, I64Serde));
        let built = b.build("app").unwrap();
        let g =
            pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app", 1024)).unwrap();
        check!(g.cache_owner.contains_key("cg"));
    }

    #[test]
    fn time_windowed_cogroup_uncached_when_off() {
        let b = StreamsBuilder::new();
        let g1 = b.stream::<String, String>(["in1"]).group_by_key();
        let g2 = b.stream::<String, String>(["in2"]).group_by_key();
        g1.cogroup::<i64, _>(|_k, v: &String, acc| acc + i64::try_from(v.len()).unwrap_or(i64::MAX))
            .cogroup(g2, |_k, _v: &String, acc| acc + 1)
            .windowed_by(TimeWindows::of_size(100))
            .aggregate_explicit(
                || 0i64,
                Materialized::with(StringSerde, I64Serde)
                    .as_store("cg")
                    .with_caching(false),
            )
            .to_stream()
            .to_explicit("out", Produced::with(StringSerde, I64Serde));
        let built = b.build("app").unwrap();
        let g =
            pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app", 1024)).unwrap();
        check!(!g.cache_owner.contains_key("cg"));
    }
}
```

Append to the end of `session_windowed_cogrouped.rs` (note `windowed_by_session`, the `SessionWindows` import, and that session `aggregate_explicit` takes the merger as its **second** argument: `aggregate_explicit(init, merger, materialized)`):

```rust
#[cfg(test)]
mod caching_tests {
    use assert2::check;

    use crate::dsl::StreamsBuilder;
    use crate::store::backend::StoreBackend;
    use crate::{I64Serde, Materialized, Produced, SessionWindows, StringSerde};

    #[test]
    fn session_windowed_cogroup_marks_store_cached() {
        let b = StreamsBuilder::new();
        let g1 = b.stream::<String, String>(["in1"]).group_by_key();
        let g2 = b.stream::<String, String>(["in2"]).group_by_key();
        g1.cogroup::<i64, _>(|_k, v: &String, acc| acc + i64::try_from(v.len()).unwrap_or(i64::MAX))
            .cogroup(g2, |_k, _v: &String, acc| acc + 1)
            .windowed_by_session(SessionWindows::of_inactivity_gap(100))
            .aggregate_explicit(
                || 0i64,
                |_k: &String, a: i64, b: i64| a + b,
                Materialized::with(StringSerde, I64Serde).as_store("cg"),
            )
            .to_stream()
            .to_explicit("out", Produced::with(StringSerde, I64Serde));
        let built = b.build("app").unwrap();
        let g =
            pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app", 1024)).unwrap();
        check!(g.cache_owner.contains_key("cg"));
    }

    #[test]
    fn session_windowed_cogroup_uncached_when_off() {
        let b = StreamsBuilder::new();
        let g1 = b.stream::<String, String>(["in1"]).group_by_key();
        let g2 = b.stream::<String, String>(["in2"]).group_by_key();
        g1.cogroup::<i64, _>(|_k, v: &String, acc| acc + i64::try_from(v.len()).unwrap_or(i64::MAX))
            .cogroup(g2, |_k, _v: &String, acc| acc + 1)
            .windowed_by_session(SessionWindows::of_inactivity_gap(100))
            .aggregate_explicit(
                || 0i64,
                |_k: &String, a: i64, b: i64| a + b,
                Materialized::with(StringSerde, I64Serde)
                    .as_store("cg")
                    .with_caching(false),
            )
            .to_stream()
            .to_explicit("out", Produced::with(StringSerde, I64Serde));
        let built = b.build("app").unwrap();
        let g =
            pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app", 1024)).unwrap();
        check!(!g.cache_owner.contains_key("cg"));
    }
}
```

Append to the end of `sliding_windowed_cogrouped.rs`:

```rust
#[cfg(test)]
mod caching_tests {
    use assert2::check;

    use crate::dsl::StreamsBuilder;
    use crate::store::backend::StoreBackend;
    use crate::{I64Serde, Materialized, Produced, SlidingWindows, StringSerde};

    #[test]
    fn sliding_windowed_cogroup_marks_store_cached() {
        let b = StreamsBuilder::new();
        let g1 = b.stream::<String, String>(["in1"]).group_by_key();
        let g2 = b.stream::<String, String>(["in2"]).group_by_key();
        g1.cogroup::<i64, _>(|_k, v: &String, acc| acc + i64::try_from(v.len()).unwrap_or(i64::MAX))
            .cogroup(g2, |_k, _v: &String, acc| acc + 1)
            .windowed_by_sliding(SlidingWindows::of_time_difference_with_no_grace(100))
            .aggregate_explicit(
                || 0i64,
                Materialized::with(StringSerde, I64Serde).as_store("cg"),
            )
            .to_stream()
            .to_explicit("out", Produced::with(StringSerde, I64Serde));
        let built = b.build("app").unwrap();
        let g =
            pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app", 1024)).unwrap();
        check!(g.cache_owner.contains_key("cg"));
    }

    #[test]
    fn sliding_windowed_cogroup_uncached_when_off() {
        let b = StreamsBuilder::new();
        let g1 = b.stream::<String, String>(["in1"]).group_by_key();
        let g2 = b.stream::<String, String>(["in2"]).group_by_key();
        g1.cogroup::<i64, _>(|_k, v: &String, acc| acc + i64::try_from(v.len()).unwrap_or(i64::MAX))
            .cogroup(g2, |_k, _v: &String, acc| acc + 1)
            .windowed_by_sliding(SlidingWindows::of_time_difference_with_no_grace(100))
            .aggregate_explicit(
                || 0i64,
                Materialized::with(StringSerde, I64Serde)
                    .as_store("cg")
                    .with_caching(false),
            )
            .to_stream()
            .to_explicit("out", Produced::with(StringSerde, I64Serde));
        let built = b.build("app").unwrap();
        let g =
            pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app", 1024)).unwrap();
        check!(!g.cache_owner.contains_key("cg"));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p crabka-client-streams --lib caching_tests -- --nocapture`
Expected: FAIL — each `*_marks_store_cached` panics at `check!(g.cache_owner.contains_key("cg"))` (no mark call yet).

- [ ] **Step 3: Add the mark in each registrar**

In `time_windowed_cogrouped.rs`: add `caching` to the `Materialized` destructure (currently lines 71-76 use `..`):

```rust
        let Materialized {
            key_serde,
            value_serde,
            logging,
            caching,
            ..
        } = materialized;
```

and inside the registrar closure, after the `add_window_store(...)` call (currently lines 88-97), add:

```rust
            state.topology.mark_store_caching(&store_for_reg, caching);
```

In `session_windowed_cogrouped.rs`: add `caching` to the `Materialized` destructure (~lines 85-90), and after the `add_session_store(...)` call (~lines 102-110) add the same `mark_store_caching(&store_for_reg, caching)` line inside the registrar.

In `sliding_windowed_cogrouped.rs`: add `caching` to the `Materialized` destructure (~lines 78-83), and after the `add_window_store(...)` call (~lines 102-110) add the same `mark_store_caching(&store_for_reg, caching)` line inside the registrar.

> The registrar closures are `move` and currently capture `ks`, `vs`, `store_for_reg`, `size`, `grace` (and `window_size` for sliding). `caching` is a `Copy` `bool`, so adding it to the closure body captures it automatically — no extra `let` clone needed.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p crabka-client-streams --lib caching_tests -- --nocapture`
Expected: PASS — all six tests across the three modules.

- [ ] **Step 5: Commit**

```bash
git add crates/client-streams/src/dsl/time_windowed_cogrouped.rs \
        crates/client-streams/src/dsl/session_windowed_cogrouped.rs \
        crates/client-streams/src/dsl/sliding_windowed_cogrouped.rs
git commit -m "feat(client-streams): cache windowed/session/sliding cogroup stores (record-cache parity)"
```

---

## Task 5: combined verification gate (disabled-path regression + full suite)

**Files:** none (verification only).

This is the mandatory cross-verification after the parallel batches. It also proves the **disabled-path regression**: every existing golden and DSL-integration test stays byte-identical because `TopologyTestDriver` forces `cache_max_bytes = 0`, so both newly-marked stores fall back to immediate forward.

- [ ] **Step 1: Format**

Run: `cargo fmt --check`
Expected: clean (no diff). If it fails, run `cargo fmt` and amend the relevant commit.

- [ ] **Step 2: Clippy (CI gate is `--all-targets`)**

Run: `cargo clippy -p crabka-client-streams --all-targets -- -D warnings`
Expected: no warnings. (If a previously-cached clean result is suspected, `touch` the four edited source files first to force a re-lint.)

- [ ] **Step 3: Full crate test run (includes the existing goldens = disabled-path regression)**

Run: `cargo test -p crabka-client-streams`
Expected: PASS — all unit + integration tests, including `cogroup_nonwindowed`, `cogroup_time_windowed`, `cogroup_session_windowed`, `cogroup_sliding_windowed`, and `dsl_integration` (all unchanged: TTD forces cache off). No golden `.topology.json` diffs.

- [ ] **Step 4: Workspace build**

Run: `cargo build --workspace`
Expected: success.

- [ ] **Step 5: Commit (only if fmt/clippy required a fixup)**

```bash
git add -A
git commit -m "chore(client-streams): fmt/clippy fixups for cogroup + to_table caching"
```

---

## Self-review — spec coverage

- Spec §3 `to_table` suppression + mark → Task 1 (processor) + Task 2 (DSL mark) ✓; tests §5.1 → Task 1 unit tests, §5.5 → Task 5.
- Spec §4 cogroup mark flips (4 kinds) → Task 3 (non-windowed) + Task 4 (windowed/session/sliding) ✓.
- Spec §4.2 cross-input read-your-writes → Task 3 `cogroup_caches_marks_and_dedups_cross_input` ✓.
- Spec §5.1 to_table unit → Task 1 ✓. §5.2 cogroup cross-input → Task 3 ✓. §5.3 windowed/session/sliding → Task 4 (mark-level; behavioral dedup mechanism shared with §5.2) ✓. §5.5 disabled-path regression → Task 5 Step 3 ✓.
- Spec §5.4 (embedded-broker changelog-at-flush + restore): **intentionally not a separate task.** The changelog-at-flush + dedup behavior is proven broker-free by the `flush_caches` + `take_output`/`drain_changelogs` driver tests (Tasks 2-3), and the underlying `CachingKeyValueStore`/`CachingWindowStore` changelog-at-flush + restore path is already covered by #491's `dsl_count_restart_restore_caching_on`. If a reviewer wants a cogroup-specific embedded-broker restore test, add it as a follow-up using that test as the template (it is heavy harness; out of this slice's critical path).
- Spec §6 non-goals (versioned / emit-final / windowed-cogroup-suppress) → no code; the existing `caching && emit.is_on_update()` gates and the absence of a versioned mark are left untouched ✓.

## Risks / watch-items for the implementer

1. **`init_processors()` ordering** — the behavioral tests MUST call `g.init_processors()` after `instantiate(...)` and before `pipe(...)`, or the `TupleForwarder` never resolves `cached=true` and suppression won't engage. (The real runtime does this at `runtime/task.rs:95`.)
2. **`g.pipe` key type** is `Option<&[u8]>` — pass `Some(b"k")`, not a `Bytes`.
3. **Session cogroup arity** (confirmed) — `aggregate_explicit(init, merger, materialized)`: the merger `Fn(&K, VOut, VOut) -> VOut` is the **second** positional argument, not a trailing closure. The plan's session test reflects this.
4. **Clippy cache masking** — a local `-p crate` clippy can serve stale-clean results; `touch` edited files and check the real `$?` (see the project's clippy-cache memory).
