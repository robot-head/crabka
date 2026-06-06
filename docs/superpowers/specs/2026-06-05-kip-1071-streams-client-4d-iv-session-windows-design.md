# KIP-1071 Streams Client #4d-iv — Session windows + session aggregations

**Status:** design approved (2026-06-05)
**Branch:** `streams-4d-iv-session-windows` — stacks on `streams-4d-iii-stream-join`
(PR #399), because it extends the `ChangelogKind` enum that 4d-iii introduced (it is
not in `main`) and shares `names.rs` / `wire.rs` / `builder.rs`. Rebase onto `main`
when #399 merges (the established stacked-slice cadence).
**Worktree:** `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`
**Ground truth:** Docker JVM Kafka-Streams 4.1.0 golden capture (`tests/jvm-capture/run.sh --gradle`)

The final slice of the 4d windowing arc. Adds **session windows**: `KGroupedStream`
grouped by data-driven sessions (an inactivity *gap* between records), with
`count` / `reduce` / `aggregate` terminal ops yielding `KTable<Windowed<K>, VA>`.
Unlike time windows, a session window `[start, end]` is defined by the data, and a
new record **merges** any sessions within `gap` of it into one — tombstoning the
merged-away sessions and emitting the new merged session.

Builds on:
- **4d-i** — pluggable async byte stores (`ByteKeyValueStore`: `InMemoryBytes` /
  `TursoBytes`; `StoreBackend`), `KeyValueStore::range`.
- **4d-ii** — `WindowBytesStore` (a second typed store over the byte backend),
  `Change<V>` aggregation output, the windowed changelog wire config + the `count`
  name-burn, and the `TimeWindowedKGroupedStream` terminal-op shape this mirrors.

Per `CLAUDE.md`: greenfield (no compat shims); Apache Kafka wire-protocol byte
exactness; match Kafka semantics (KIP session-window behavior).

## 1. Scope (decided)

- **Ops:** `count` + `reduce` + `aggregate`. `aggregate` takes the session
  `Merger<K, VA>` (count's merger = `a + b`; reduce's = the reducer).
- **Merge fidelity:** full JVM session-merge, **emit-on-update** (no window
  closing / suppression — consistent with 4d-ii). A record merges all sessions in
  `[ts - gap, ts + gap]`, tombstones each merged-away session, emits the new one.
- **Byte fidelity:** match JVM exactly — `SessionKeySchema` store key, session
  changelog config, `SessionWindowedSerde` output key. The store **value** layout
  (raw aggregate vs `ValueAndTimestamp`) is confirmed against the capture during
  implementation (belief: raw aggregate — the session end *is* the timestamp).

Out of scope: window closing / `suppress` / wall-clock emission (deferred, as in
all prior windowing slices); session **join** (Kafka has none).

## 2. Architecture

A new **third typed store** over the pluggable byte backend, beside
`KeyValueBytesStore` (4d-i) and `WindowBytesStore` (4d-ii): `SessionBytesStore<K,V>`.
Session merge is more complex than window aggregation, so it earns a typed,
independently-testable boundary (rejected: reusing `WindowBytesStore`, whose
`fetch_single(key, windowStart)` API doesn't fit end+start ranges and removal;
rejected: raw `KeyValueStore<Bytes,Bytes>` with the codec leaked into the
processor).

Data flow mirrors 4d-ii: `groupByKey → windowed_by_session(SessionWindows) →
count/reduce/aggregate → KTable<Windowed<K>, VA> → toStream → to`. The aggregate
node reads/writes the session store and forwards `Change<VA>` keyed by
`Windowed<K>`; the merge logic lives entirely in the processor.

## 3. Store layer

### 3.1 `store/session_schema.rs` — `SessionKeySchema` byte codec

JVM `SessionKeySchema.toBinary`: **end-first** so the store sorts by `(key, end,
start)` (the merge fetch scans by end time).

```
session_key(key_bytes, end, start) = key_bytes ‖ end:8B BE ‖ start:8B BE
```

- `session_key(kb, end, start) -> Bytes`
- `session_end_of(k) -> i64`   (`k[len-16 .. len-8]`)
- `session_start_of(k) -> i64` (`k[len-8 .. len]`)
- `session_key_bytes_of(k) -> &[u8]` (`k[.. len-16]`)

Value = the raw serialized aggregate bytes (no `ValueAndTimestamp` wrap — the
session end carries the time). Confirm against the captured changelog during
Phase C; if 4.1 wraps, switch to `wrap_value`/`unwrap_value` (already in
`window_schema.rs`).

### 3.2 `store/session.rs` — `SessionStore` trait + `SessionBytesStore`

```rust
#[async_trait]
pub trait SessionStore<K: Send + Sync, V: Send>: StateStore {
    /// Sessions for `key` overlapping `[earliest_end, latest_start]`:
    /// `session.end >= earliest_end && session.start <= latest_start`.
    async fn find_sessions(&self, key: &K, earliest_end: i64, latest_start: i64)
        -> Vec<(i64 /*start*/, i64 /*end*/, V)>;
    async fn put(&mut self, key: K, start: i64, end: i64, value: V);
    async fn remove(&mut self, key: &K, start: i64, end: i64);
}
```

`SessionBytesStore<K,V>` mirrors `WindowBytesStore`: holds `Box<dyn
ByteKeyValueStore>` + key/value serdes + changelog buffer + `logging` flag;
`in_memory(..)` test ctor; `StateStore` impl (`take_changelog` / `apply_changelog`
for restore). `find_sessions` ranges `[session_key(kb, earliest_end, i64::MIN..)]`
— concretely lower = `kb ‖ earliest_end:8BE ‖ 0i64` lower-bounded on end, upper =
`kb ‖ i64::MAX:8BE` — over `backend.range`, then filters `start <= latest_start`
and guards the inner-key prefix (as `WindowBytesStore::fetch` does). `put` writes
`session_key(kb, end, start) → raw` and logs `Some`; `remove` deletes + logs a
tombstone (`None`).

(Timestamps are non-negative epoch-millis; the same `0`-lower-bound discipline as
the join store avoids negative-ts byte-ordering hazards.)

### 3.3 Registry + context accessor

- `StoreRegistry::get_session<K,V>(name) -> Option<&mut dyn SessionStore<K,V>>` (a
  single downcast, like `get_window` / `get_join_window`).
- `ProcessorContext::get_session_store<K,V>(name)` — fetch per-record, not held
  across `process` calls.

## 4. Processor — the merge engine (`dsl/processors/session_aggregate.rs`)

Two processors (mirroring 4d-ii's aggregate + separate reduce, to keep `V` clean):

### 4.1 `KStreamSessionAggregateProcessor<K, V, VA, I, A, M>` (count + aggregate)

`Processor<K, V, Windowed<K>, Change<VA>>`. Fields: `store_name`, `gap_ms`, `init:
I`, `agg: A`, `merger: M` where `I: Fn() -> VA`, `A: Fn(&K, &V, VA) -> VA`, `M:
Fn(&K, VA, VA) -> VA`. On `process(ctx, r{key, value, ts})`:

1. `cands = store.find_sessions(&key, ts - gap, ts + gap).await` →
   `Vec<(start, end, VA)>`.
2. `let mut new_start = ts; let mut new_end = ts; let mut acc = (init)();`
   `for (s, e, v) in &cands { acc = (merger)(&key, acc, v.clone()); new_start =
   min(new_start, s); new_end = max(new_end, e); }`
   `acc = (agg)(&key, &value, acc);`
3. For each `(s, e, v) in cands`: `store.remove(&key, s, e).await;`
   `ctx.forward(Record::new(Some(Windowed{key, Window{start:s, end:e}}),
   Change::tombstone(Some(v)), e))` — tombstone the merged-away session
   (`Change::tombstone` = `old: Some(v), new: None`).
4. `store.put(key.clone(), new_start, new_end, acc.clone()).await;`
   `ctx.forward(Record::new(Some(Windowed{key, Window{start:new_start,
   end:new_end}}), Change::update(None, acc), new_end))`.

Store borrow is scoped and dropped before each `ctx.forward` (the 4d-ii / join
discipline). Emit timestamps: merged-away tombstone at its own session `end`; new
session at `new_end`. (These match the session's own time; verified against
execution expectations — the wire golden does not constrain emit timestamps.)

`count`: `init = || 0i64`, `agg = |_k,_v,a| a + 1`, `merger = |_k,a,b| a + b`.
`aggregate`: caller's `init` / `agg` + the explicit `Merger<K,VA>`.

### 4.2 `KStreamSessionReduceProcessor<K, V, R>` (reduce)

`Processor<K, V, Windowed<K>, Change<V>>`, `R: Fn(&V, &V) -> V`. Same merge flow
with `VA = V`: the **first** contribution seeds (no `init`); folding old sessions
uses `reducer(&acc, &v)`, and folding the new record uses `reducer(&acc, &value)`
(or seeds with `value` when there are no candidates). Keeps the public value type
`V` (no `Option`/sentinel leak), exactly as `KStreamWindowReduceProcessor`.

## 5. DSL surface

### 5.1 `dsl/windows.rs` — `SessionWindows`

```rust
#[derive(Debug, Clone, Copy)]
pub struct SessionWindows { pub gap_ms: i64, pub grace_ms: i64 }
impl SessionWindows {
    pub fn of_inactivity_gap(gap_ms: i64) -> Self;       // grace 0; gap > 0
    pub fn grace(self, grace_ms: i64) -> Self;           // grace >= 0
}
```

Plus `Merger<K, VA>` is just a `Fn(&K, VA, VA) -> VA` bound on `aggregate` (no new
type needed).

### 5.2 `SessionWindowedSerde<KS>` (output key)

JVM `SessionWindowedSerializer`: `key_bytes ‖ end:8B BE ‖ start:8B BE` (both bounds
in the bytes; `deserialize` reconstructs the full `Window`). Distinct from
`TimeWindowedSerde` (`key ‖ start`, end derived from size). Re-exported.

### 5.3 `KGroupedStream::windowed_by_session` + `SessionWindowedKGroupedStream`

Rust can't reuse `windowed_by` for both `TimeWindows` and `SessionWindows` (no
arg-type overload) — add a distinct method:

```rust
pub fn windowed_by_session(self, windows: SessionWindows)
    -> SessionWindowedKGroupedStream<K, V>;
```

(Mirrors the stream-stream-join naming precedent; a generic `windowed_by<W:
Windows>` sealed-trait approach was considered but is heavier than a named method
for greenfield.)

`SessionWindowedKGroupedStream<K,V>` mirrors `TimeWindowedKGroupedStream`: holds
the grouped lineage (parent node, key-changing flag, `Grouped` name, repartition
thunk) + `SessionWindows`. Terminal ops:
- `count(Materialized) -> KTable<Windowed<K>, i64>`
- `reduce(reducer, Materialized) -> KTable<Windowed<K>, V>`
- `aggregate(init, agg, merger, Materialized) -> KTable<Windowed<K>, VA>`

Lowering mirrors `lower_aggregate_windowed`: mint the session store name at the
JVM counter position (+ the `count` extra name-burn when `Materialized` is
unnamed), record the optional repartition + a session aggregate node
(`GraphNodeKind::Aggregate { store_name, changelog: true }`), and register the
store via `add_session_store`. Output `KTable<Windowed<K>, VA>` (the result is
materialized in the session store).

## 6. Wire (`topology/{node,wire,builder}.rs`)

- `ChangelogKind::Session { retention_ms }` — 4th variant beside `Kv` /
  `AggWindow` / `JoinWindow`.
- `add_session_store<K,V,KS,VS>(name, ks, vs, gap_ms, grace_ms, procs)` — builds
  the `SessionBytesStore` via the selected `StoreBackend`, `StoreEntry` with
  `ChangelogKind::Session { retention_ms = gap_ms + grace_ms + 86_400_000 }`.
- `session_changelog_topic_configs(retention_ms)` — expected (pinned by the
  capture): `cleanup.policy=compact,delete`, `message.timestamp.type=CreateTime`,
  `retention.ms=<gap+grace+1d>` (same family as the windowed changelog; if 4.1's
  session changelog differs, this is tuned to the capture). `state_changelog_topics`
  dispatches the 4 `ChangelogKind` variants.

The session store auto-name prefix (JVM `KSTREAM-SESSION-STATE-STORE-` vs the
`count` name-burn at the aggregate-store prefix) is **pinned by the capture** — the
DSL store-name minting is tuned in Phase C to byte-match.

## 7. Golden capture + tests

### 7.1 Capture (`tests/jvm-capture/.../Capture.java`)

Add fixture #12:

```java
static Topology sessionCount() {
    StreamsBuilder b = new StreamsBuilder();
    b.<String, String>stream("in")
        .groupByKey()
        .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(60)))
        .count()
        .toStream()
        .to("out");
    return b.build(optimizedProps());
}
```

`run.sh --gradle` → `tests/testdata/golden/dsl/session_count.topology.json` (ground
truth for the store name + changelog config + the single subtopology / source
`["in"]`). Update the run.sh / Capture comments to 12 fixtures.

### 7.2 Golden frame test

`session_count_matches_jvm` in `dsl_golden_frame.rs` — the DSL `sessionCount`
lowering must byte-match. **The 11 prior goldens stay byte-identical.**

### 7.3 Execution tests (`dsl_execution.rs`)

- **merge within gap:** records at t=0 and t=30 (gap 60) → after the 2nd, a
  tombstone for `[0,0]` and an update for the merged `[0,30]` session.
- **separate beyond gap:** t=0 and t=200 → two independent sessions, no merge.
- **three-way merge:** a record bridging two existing sessions merges all three.
- **count / reduce / aggregate** value correctness.
- **restart-restore:** session changelog round-trip rebuilds the store.

### 7.4 Store unit tests (`store/session.rs`, `store/session_schema.rs`)

`session_key` round-trip + end-first sort; `find_sessions` range + `start` filter +
prefix guard; `put`/`remove` changelog capture; merge candidate selection.

## 8. Phasing (no per-task file overlap within a batch)

- **Phase A — store + wire primitives.** `session_schema.rs`, `session.rs`,
  `registry.rs` (`get_session`), `processor/api.rs` (`get_session_store`),
  `topology/{node,wire,builder}.rs` (`ChangelogKind::Session` + `add_session_store`
  + session changelog config) + unit tests.
- **Phase B — processor + DSL.** `SessionWindows` + `SessionWindowedSerde` +
  `session_aggregate.rs` (aggregate + reduce) + `session_windowed_kgrouped.rs` +
  `windowed_by_session` + execution tests.
- **Phase C — golden + verify.** Capture.java `sessionCount` + Docker capture +
  `session_count` golden test + docs (lib.rs prose) + final verification (full
  suite, 12 goldens, clippy `--all-targets`, fmt).

## 9. Risks / open items

- **Session changelog value layout** (raw agg vs `ValueAndTimestamp`) and **store
  auto-name prefix** are confirmed by the Phase C capture; the Phase A value codec
  + Phase B store-name minting are tuned then. Both are isolated (value layout →
  execution/restore only; store name → the golden).
- **`windowed_by_session` naming** — accepted divergence from the JVM's overloaded
  `windowedBy` (Rust limitation), consistent with the `join_table` precedent.
- Emit timestamps for tombstones / new session match the session bounds; not
  wire-constrained, validated by execution tests against expected JVM output.
