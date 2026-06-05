# KIP-1071 Streams Client — Sub-project #4d-ii: Turso-backed WindowStore + windowed aggregations

**Date:** 2026-06-05
**Status:** Design approved, pending spec review
**Scope:** Second windowing slice — a window store + `windowedBy(TimeWindows)`
tumbling/hopping `count`/`reduce`/`aggregate` → `KTable<Windowed<K>, V>`, byte-exact
vs JVM 4.1.
**Builds on:** 4d-i (async execution path + the `ByteKeyValueStore` byte-store seam
with `range` scans; PR #391). Branch `streams-4d-ii-windowstore` (stacked on
`streams-4d-async-stores`; rebase onto `main` once #391 merges). Worktree
`/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`.

## 1. Context & program decomposition

Windowing (4d) decomposed: 4d-i (async + pluggable byte-store backend, DONE PR
#391) → **4d-ii** (this spec — window store + time-windowed aggregations) → 4d-iii
(windowed KStream-KStream join) → 4d-iv (session store + session aggregations). 4d-i
delivered the async `ByteKeyValueStore` backend with an ordered half-open
`range(lo, hi)` scan — exactly the window-fetch primitive. 4d-ii builds the window
store + DSL on top, with no change to the byte backend (Turso/in-memory) itself.

## 2. Goal & non-goals

### Goal
Time-windowed aggregations in the DSL:
```rust
KGroupedStream<K,V>::windowed_by(TimeWindows) -> TimeWindowedKGroupedStream<K,V>;
TimeWindowedKGroupedStream<K,V>::count(Materialized)  -> KTable<Windowed<K>, i64>;
TimeWindowedKGroupedStream<K,V>::reduce(reducer, M)   -> KTable<Windowed<K>, V>;
TimeWindowedKGroupedStream<K,V>::aggregate(init, agg, M) -> KTable<Windowed<K>, VA>;
```
- Tumbling **and** hopping windows, byte-exact window assignment vs JVM 4.1.
- A **`WindowStore`** over the existing byte backend, with JVM-exact
  `WindowKeySchema` keys + `ValueAndTimestamp` values (mixed-group changelog interop).
- **Windowed changelog** topic config byte-exact (`cleanup.policy=compact,delete` +
  `retention.ms`).
- Topology byte-matches captured JVM 4.1 (a 9th golden frame); the **8 prior golden
  frames stay byte-identical**.

### Non-goals (deferred)
- **Window closing / grace / late-record drop / stream-time tracking** +
  `suppress(untilWindowCloses)` — 4d-ii **emits on every update** and never drops
  records into closed windows. Results are correct for in-order data.
- **Local window expiry / retention enforcement** — the store keeps all windows; the
  changelog `retention.ms` is declared on the wire but Crabka does not prune locally
  yet.
- **Session windows** → 4d-iv. **Windowed KStream-KStream join** → 4d-iii. **Sliding
  windows** (KIP-450) → later.
- **`Windowed<K>` deserialization on a source** — only the output/sink direction +
  the store encode `Windowed<K>`.

## 3. The window-store storage layer

A new typed store beside `KeyValueBytesStore`, over the **same** `ByteKeyValueStore`
backend (Turso/in-memory).

- **`WindowStore<K,V>` trait** (`store/api.rs`, async, `: StateStore`):
  ```rust
  #[async_trait]
  pub trait WindowStore<K: Send + Sync, V: Send>: StateStore {
      async fn fetch(&self, key: &K, time_from: i64, time_to: i64) -> Vec<(i64, V)>; // (windowStart, value)
      async fn fetch_single(&self, key: &K, window_start: i64) -> Option<(i64, V)>;  // (storedTs, value)
      async fn put(&mut self, key: K, window_start: i64, value: V, record_ts: i64);
  }
  ```
- **`WindowBytesStore<K,V>`** (`store/window.rs`, NEW) — wraps `Box<dyn
  ByteKeyValueStore>` + boxed serdes + the changelog `Vec` + `logging`. Byte encoding
  (JVM-exact):
  - **key** = `key_serde(k) ‖ window_start:8B BE ‖ 0u32:4B BE` (`WindowKeySchema.toStoreKeyBinary`; seqnum is always 0 for aggregations, `retainDuplicates=false`).
  - **value** = `record_ts:8B BE ‖ value_serde(v)` (`ValueAndTimestampSerializer`); `None` → tombstone.
  - `fetch(k, from, to)` = `backend.range( k‖from‖0 , k‖(to+1)‖0 )`, decode each composite key's `windowStart` (bytes `[len-12 .. len-4)`), unwrap the value's `(ts, v)`.
  - `fetch_single(k, ws)` = `backend.get(k‖ws‖0)` → unwrap.
  - `put` writes the composite key + wrapped value; buffers `(composite_key_bytes, Some(wrapped_value_bytes))` (or `None`) on the changelog when `logging`.
  - `StateStore::{take_changelog, apply_changelog, changelog_topic, set_logging, flush, close, name, as_any_mut}` work unchanged — they are byte-level and carry the composite key/wrapped value transparently. Restore replays composite-key bytes into the byte backend (clean-slate + replay, unchanged from 4d-i).
- **Registry:** `StoreRegistry::get_window::<K,V>(name) -> Option<&mut dyn
  WindowStore<K,V>>` downcasts `as_any_mut()` to `WindowBytesStore<K,V>` — the second
  concrete downcast target (mirrors `get_kv`). `ProcessorContext::get_window_store::<K,V>(name)`
  exposes it.
- **Instantiation:** the same `StoreBackend::open` byte backend; the store factory
  builds a `WindowBytesStore` instead of a `KeyValueBytesStore`.

## 4. Window types + `Windowed<K>` + serde (`dsl/windows.rs`, NEW)

- **`Window { start: i64, end: i64 }`**, **`Windowed<K> { key: K, window: Window }`**
  (the windowed output key; `Clone + Any + Send`).
- **`TimeWindows { size_ms, advance_ms, grace_ms }`**: `TimeWindows::of_size(size)`
  (tumbling — `advance=size`, grace 0), `.advance_by(advance)` (→ hopping),
  `.grace(g)`. Core: `windows_for(t) -> Vec<i64>` (window starts a timestamp falls
  into), the exact JVM formula:
  ```
  let mut start = max(0, t - size_ms + advance_ms) / advance_ms * advance_ms;
  let mut out = vec![];
  while start <= t { out.push(start); start += advance_ms; }
  ```
  Tumbling → one start; hopping → several. Each window is `[start, start + size_ms)`.
- **`TimeWindowedSerde<K, KS: Serde<K>>`** — a `Serde<Windowed<K>>` producing the
  **output-topic** format `key_serde(k) ‖ window_start:8B BE` (no end, no seqnum —
  `TimeWindowedSerializer`); deserialization reconstructs `end = start + size`, so the
  serde carries the window size. Lets `KTable<Windowed<K>,V>.to_stream().to("out",
  Produced::with(TimeWindowedSerde::new(ks, size), vs))` be byte-exact and lets
  execution tests read results.

**Two distinct layouts (a known trap):** the store/changelog key has the **12-byte**
`windowStart+seqnum` suffix (§3); the output-topic serde has only the **8-byte**
`windowStart` suffix (§4). Do not conflate them.

## 5. `windowedBy` DSL + aggregation processor + lowering

- **`KGroupedStream::windowed_by(TimeWindows) -> TimeWindowedKGroupedStream<K,V>`**
  (`dsl/kgrouped.rs`) — a new handle carrying the parent node, the upstream
  key-changing bit, and the `TimeWindows`.
- **`TimeWindowedKGroupedStream::{count, reduce, aggregate}(…, Materialized) ->
  KTable<Windowed<K>, V>`** — same shapes as the non-windowed `KGroupedStream`
  aggregations, output key `Windowed<K>`.
- **`KStreamWindowAggregateProcessor<K,V,VA,I,A>`** (`dsl/processors/window_aggregate.rs`,
  NEW; `Processor<K, V, Windowed<K>, Change<VA>>`): per record at ts `t`, for each `ws
  in time_windows.windows_for(t)`:
  ```
  let (old_ts, old) = match store.fetch_single(&k, ws).await { Some((ts,v)) => (ts, Some(v)), None => (-1, None) };
  let new = agg(&k, &v, old.clone().unwrap_or_else(&init));
  let new_ts = max(t, old_ts);
  store.put(k.clone(), ws, new.clone(), new_ts).await;
  ctx.forward(Record::new(Some(Windowed { key: k.clone(), window: Window { start: ws, end: ws + size } }), Change::update(old, new), new_ts));
  ```
  Emit-on-every-update; **no** closed-window drop. `count`/`reduce`/`aggregate` differ
  only in init+agg (reduce: first value seeds, no init; matches the non-windowed forms).
- **Lowering** (`dsl/kgrouped.rs`): reuse the `KSTREAM-AGGREGATE-STATE-STORE-<idx>`
  store name + the **`count`-only counter "burn"** (JVM bumps the name index by one on
  the `count` path when the store name is unset — replicate for byte-exactness);
  record the aggregate node; register the store via a new **`Topology::add_window_store::<K,V,KS,VS>(name,
  key_serde, value_serde, size_ms, grace_ms, procs)`** (flags it windowed + carries the
  retention inputs); thunk calls `add_processor::<K, V, Windowed<K>, Change<VA>, _,_,_>`.
  Repartition inserted only if the key changed upstream (reuse existing machinery). The
  result `KTable<Windowed<K>, V>` roots at the aggregate node; downstream `to_stream()`
  forwards `Windowed<K>` records.

## 6. Windowed changelog wire config (`topology/node.rs`, `wire.rs`)

- `add_window_store` records the `StoreEntry` with a `windowed: true` flag + `retention_ms
  = size_ms + grace_ms + 86_400_000` (the JVM `windowstore.changelog.additional.retention.ms`
  default = 1 day).
- `wire.rs` picks the changelog `topic_configs` per store: KV stays
  `cleanup.policy=compact` + `message.timestamp.type=CreateTime`; **windowed** emits
  `cleanup.policy=compact,delete` + `message.timestamp.type=CreateTime` +
  `retention.ms=<size+grace+86400000>` (sorted by key, as the existing encoder does).
  Topic name `<app>-<store>-changelog`, partitions 0, rf −1 — unchanged.

## 7. JVM capture & golden frames

Add `windowedCount()` to `tests/jvm-capture/.../Capture.java`:
`builder.stream("in").groupByKey().windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60))).count().toStream().to("out")`
(String serdes, app id "app", optimization=all). Capture via the Docker Kafka-Streams
4.1 harness → `testdata/golden/dsl/windowed_count.topology.json`. Expected (pinned by
capture): one subtopology, `source_topics: ["in"]`, the aggregate store name (with the
count **burn** — likely `KSTREAM-AGGREGATE-STATE-STORE-0000000003`), one
`state_changelog_topics` entry with `cleanup.policy=compact,delete` +
`retention.ms=86460000` (60s + 0 grace + 1 day). The **8 prior golden frames stay
byte-identical**.

## 8. Testing strategy (gates)

1. **Unit:** `windows_for` (tumbling = one start; hopping size=10/advance=5 at t=12 →
   `{5,10}`; epoch alignment + the `max(0,…)` clamp at small t); `WindowKeySchema` key
   round-trip (`key‖8BE‖0`, byte layout asserted); `ValueAndTimestamp` value round-trip
   (`ts‖agg`, tombstone → `None`); `WindowBytesStore` put/fetch_single/fetch over a
   range (in-memory backend).
2. **Golden:** `windowed_count` byte-matches the JVM fixture; the **8 prior goldens
   stay byte-identical**.
3. **Execution** (`TopologyTestDriver`, reading via `TimeWindowedSerde`): tumbling
   count (two records in `[0,60s)` → count 2 at that window; a third in `[60s,120s)` →
   separate count 1); hopping (one record in two overlapping windows → two emits with
   the right window bounds); reduce + aggregate variants; assert the emitted key is
   `Windowed{k, [start,end)}` and the value progression.
4. **Regression:** all prior 4d-i / #4 / #2 / #3 tests stay green.

## 9. Success criteria
- `windowed_by(TimeWindows).{count,reduce,aggregate}` over tumbling+hopping works
  (execution) and the topology + windowed changelog config byte-match captured JVM 4.1
  output; the store/changelog records use byte-exact `WindowKeySchema` +
  `ValueAndTimestamp`.
- The 8 prior golden frames unchanged.
- `cargo test -p crabka-client-streams` green; `cargo clippy --workspace --all-targets
  -- -D warnings` + `cargo fmt --check` clean; `cargo build --workspace`.
- A documented windowed-aggregation note/example in `lib.rs`.

## 10. Open points for the plan
- **The `count` burn position** — the exact store-name index is pinned by the captured
  fixture (Step 7 captures first). Confirm whether `reduce`/`aggregate` differ (they
  don't burn).
- **`get_window` vs `get_kv` downcast** — `WindowBytesStore<K,V>` is a second concrete
  downcast target; confirm a windowed store is never accessed via `get_kv` (distinct
  processor → distinct accessor).
- **`Windowed<K>` erasure** — the aggregate processor forwards `Record<Windowed<K>,
  Change<VA>>`; downstream `to_stream`/sink reconstruct the parent handle as
  `NodeHandle<Windowed<K>, …>`; a type mismatch is a RUNTIME downcast error → the full
  suite is the gate (same erasure discipline as the 4c slices).
- **Retention arithmetic** — `retention.ms = size + grace + 86_400_000`; the fixture is
  the oracle for the exact value + whether any floor applies.
- **Materialized store-name minting for windowed stores** — reuse `mint_store_name`;
  confirm the windowed store registers via `add_window_store` (not `add_state_store`) so
  the changelog config is windowed.
