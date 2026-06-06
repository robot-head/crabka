# KIP-1071 Streams Client — Suppress Slice A: core buffer + `untilWindowCloses` + `unbounded`

**Status:** design approved (2026-06-06)
**Branch:** `streams-suppress-a` — stacks on `streams-4d-iv-session-windows` (PR #402),
because the grace plumbing touches the session aggregations in #402. Rebase onto
`main` when #402 merges (the established stacked-slice cadence).
**Worktree:** `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`
**Ground truth:** Docker JVM Kafka-Streams 4.1.0 golden capture (`tests/jvm-capture/run.sh --gradle`)

The foundational slice of the **suppress** program (A → B → C → D). It adds
`KTable.suppress(Suppressed.untilWindowCloses(unbounded()))` — **final-results
emission** for windowed/session tables: buffer the per-window updates and emit each
window's final value exactly once, when stream-time passes `window.end + grace`.
This is the capstone of the windowing arc — emit-on-update was used throughout
4d-ii/iii/iv; suppress is the first emit-on-close path.

Subsequent slices (separate specs): **B** bounded `BufferConfig`
(`max_records`/`max_bytes` + `emit_early`/`shut_down`), **C** `untilTimeLimitElapsed`
(rate-limiter for any KTable), **D** fault tolerance (buffer changelog + restore).

Per `CLAUDE.md`: greenfield (no compat shims); Apache Kafka wire byte-exactness;
match Kafka semantics.

## 1. Scope (decided)

- **In:** `Suppressed::until_window_closes(BufferConfig)`, `BufferConfig::unbounded()`,
  `KTable<Windowed<KInner>, V>::suppress(...)`, the in-memory `TimeOrderedKeyValueBuffer`,
  the `KTableSuppressProcessor` (stream-time-driven window-close eviction), grace
  plumbed from the windowed/session aggregations.
- **Out (later slices):** bounded buffers + overflow behavior (B), `untilTimeLimitElapsed`
  (C), buffer changelog + restart-restore + the `withLoggingDisabled` toggle as
  anything other than the Slice-A default (D).
- **Logging:** Slice A captures the golden with `unbounded().withLoggingDisabled()`,
  so the suppress buffer adds **no changelog topic** — the buffer is purely
  in-memory and rebuilds from the upstream changelog on restart. Slice D adds buffer
  logging + restore and flips the golden to the default (logging-on) config.

## 2. Architecture

Suppress is a `KTable<K,V> → KTable<K,V>` operator. For `untilWindowCloses` the
table must be windowed, so it lives on a **specialized `impl<KInner, V>
KTable<Windowed<KInner>, V>`** — Rust enforces at compile time what the JVM enforces
at runtime ("untilWindowCloses needs a windowed KTable"), and the processor can read
`window.end` from the key.

Data flow: `windowedBy(..).count()/reduce/aggregate → suppress(untilWindowCloses(..))
→ toStream → to`. The windowed aggregation forwards a `Change<V>` per update keyed by
`Windowed<KInner>`; the suppress processor **buffers** those, advances a stream-time
clock from the record timestamps, and forwards each window's final `Change<V>` once
its window has closed.

The buffer is a new, independently-tested unit (`TimeOrderedKeyValueBuffer`). The
processor owns the buffer + the stream-time clock + the grace. The DSL `suppress`
method lowers the processor node and threads the grace.

## 3. The buffer — `TimeOrderedKeyValueBuffer<K, V>` (`dsl/processors/suppress_buffer.rs`)

A typed, in-memory, time-ordered buffer with **replace-by-key** semantics:

```rust
struct Entry<K, V> { key: K, value: V, record_ts: i64 }
pub(crate) struct TimeOrderedKeyValueBuffer<K, V> {
    // ordered by (buffer_time, seq) for time-ordered eviction
    entries: BTreeMap<(i64, u64), Entry<K, V>>,
    // locate-and-replace by key
    index: HashMap<K, (i64, u64)>,
    seq: u64,
}
```

Bounds `K: Eq + Hash + Clone` (satisfied by `Windowed<KInner>` when `KInner: Eq +
Hash + Clone`). API:
- `put(key, buffer_time, value, record_ts)` — if `key` is already buffered, remove its
  old `entries` slot; insert `entries[(buffer_time, seq)] = Entry{key, value,
  record_ts}`; `index[key] = (buffer_time, seq)`; `seq += 1`. (For `untilWindowCloses`,
  `buffer_time = window.end` is **constant per windowed key**, so a re-put keeps the
  same `buffer_time` and just refreshes the value — but the implementation handles a
  changing `buffer_time` too, which Slice C needs.)
- `evict_while(threshold) -> Vec<(K, V, i64)>` — pop entries from the front (lowest
  `buffer_time`) while `buffer_time <= threshold`, removing each from `index`,
  returning `(key, value, record_ts)` in eviction order.
- `len()` / `is_empty()` (used by tests; Slice B adds size/byte accounting here).

No serdes — serialization is deferred to Slice D's changelog. Unit-tested in
isolation: replace-by-key keeps one entry per key; eviction is in `buffer_time`
order and respects the threshold; entries above the threshold stay.

## 4. The processor — `KTableSuppressProcessor<KInner, V>` (`dsl/processors/suppress.rs`)

`Processor<Windowed<KInner>, Change<V>, Windowed<KInner>, Change<V>>` (same in/out
types). Fields: `buffer: TimeOrderedKeyValueBuffer<Windowed<KInner>, Change<V>>`,
`observed_stream_time: i64` (init `i64::MIN`), `grace_ms: i64`.

`process(ctx, r{key, value: Change<V>, timestamp})`:
1. `observed_stream_time = observed_stream_time.max(timestamp)`.
2. `let buffer_time = key.window.end;` `self.buffer.put(key, buffer_time, value,
   timestamp)`.
3. `let threshold = observed_stream_time - grace_ms;` for each `(k, change, rts)` in
   `self.buffer.evict_while(threshold)`: `ctx.forward(Record::new(Some(k), change,
   rts))`.

This matches JVM `KTableSuppressProcessor`'s final-results path: a buffered window
emits exactly once, when `observed_stream_time >= window.end + grace` (the
`buffer_time <= observed_stream_time - grace` form). The buffered `Change` is
forwarded **verbatim** (JVM behavior — downstream `toStream` takes `new`; the
suppressed intermediate updates were never emitted, so a downstream that inspects
`old` sees the JVM's same value). No async store access (the buffer is an owned
field), so no borrow-scoping concerns; `forward` is called after `evict_while`
returns an owned `Vec`.

Stream-time is **shared across all keys** flowing through the single suppress node:
a later window's record advances the clock and closes earlier windows. The processor
instance is long-lived per task, so `observed_stream_time` persists across records.

## 5. DSL — `Suppressed` / `BufferConfig` / `suppress` (`dsl/suppress.rs` + `dsl/ktable.rs`)

`dsl/suppress.rs` (new):
```rust
#[derive(Clone, Copy)]
pub struct BufferConfig { /* Slice A: marker for unbounded; B adds max_records/max_bytes/overflow */ }
impl BufferConfig { pub fn unbounded() -> Self; }

#[derive(Clone, Copy)]
pub struct Suppressed { /* kind = UntilWindowCloses; buffer: BufferConfig */ }
impl Suppressed { pub fn until_window_closes(buffer: BufferConfig) -> Self; }
```
(Modeled to extend cleanly: Slice B grows `BufferConfig`; Slice C adds
`Suppressed::until_time_limit(..)`; Slice D adds `.with_logging_disabled()` /
`.with_logging_enabled(..)`.) Re-export `Suppressed`, `BufferConfig` from `dsl/mod.rs`
+ `lib.rs`.

`dsl/ktable.rs` — specialized impl:
```rust
impl<KInner, V> KTable<Windowed<KInner>, V>
where KInner: Any + Send + Sync + Clone + Eq + Hash, V: Any + Send + Clone {
    pub fn suppress(&self, suppressed: Suppressed) -> KTable<Windowed<KInner>, V>;
}
```
Lowers a `KTABLE-SUPPRESS-` processor node (`GraphNodeKind::TableProcessor { store_name:
None }` — no materialized store in Slice A) consuming `Change<V>` from the parent and
forwarding `Change<V>`, constructed with `grace_ms` read from the KTable handle.

**Grace plumbing.** Add `window_grace_ms: Option<i64>` to the `KTable` handle (a new
`KTable::new` parameter, or a private setter). Set it in the windowed + session
aggregations (`windowed_kgrouped.rs` / `session_windowed_kgrouped.rs`) to
`Some(windows.grace_ms)`. Propagate it through the `Change`-preserving KTable ops
(`map_values` / `filter`) so a derived windowed table keeps its grace. `suppress`
reads it; if `None` (a non-windowed-derived `Windowed<K>` table — not produced by any
in-tree op), default to `0` and the window still closes on `window.end`.

`names.rs`: `KTABLE_SUPPRESS = "KTABLE-SUPPRESS-"` (the JVM `KTableImpl.suppress` node
prefix). The exact prefix + index are **pinned by the capture**; if 4.1 differs, tune
the const to match the golden.

## 6. Golden + tests

### 6.1 Capture (`Capture.java`)

Fixture #13:
```java
static Topology suppressUntilWindowCloses() {
    StreamsBuilder b = new StreamsBuilder();
    b.<String, String>stream("in")
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60)))
        .count()
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())
            .withName("sup").withLoggingDisabled())  // logging off → no changelog
        .toStream()
        .to("out");
    return b.build(optimizedProps());
}
```
`run.sh --gradle` → `tests/testdata/golden/dsl/suppress_until_window_closes.topology.json`.
With logging disabled the suppress buffer adds **no changelog topic**, so the wire is
expected byte-identical to `windowed_count` (same single subtopology, the one
aggregate-store changelog, no extra topic). The golden confirms suppress introduces
no spurious topic and doesn't perturb the aggregate-store naming/counter. (If the JVM
*does* surface something with logging disabled, the capture is the oracle and the DSL
is tuned to match.) Bump run.sh/Capture counts to 13.

> Note on `withName`: the JVM requires a name to disable logging on a suppress buffer,
> or auto-names it. Whether `withName("sup")` changes the wire is captured empirically;
> if it perturbs naming, drop it and rely on the auto-name (the capture decides).

### 6.2 Golden frame test

`suppress_until_window_closes_matches_jvm` in `dsl_golden_frame.rs` — the DSL
`…count().suppress(until_window_closes(unbounded())).to_stream().to(..)` lowering must
byte-match. **The 12 prior goldens stay byte-identical.**

### 6.3 Execution tests (`dsl_execution.rs`) — the real validation

- **buffer-then-close:** window `[0,10)` gets records at t=1,3 (count→2) — assert **no
  output** yet; a record at t=25 (window `[20,30)`) advances stream-time to 25 ≥ 10,
  closing `[0,10)` → emit `([0,10), 2)` exactly once; `[20,30)` stays buffered (no
  further output).
- **multi-window ordering:** windows `[0,10)` and `[10,20)` both buffered; a t=35
  record closes both → emitted in window-end order (`[0,10)` then `[10,20)`).
- **graced window:** `TimeWindows::of_size(10).grace(5)` — `[0,10)` closes only when
  stream-time ≥ 15 (a t=12 record does **not** close it; a t=16 record does).

### 6.4 Buffer unit tests (`suppress_buffer.rs`)

Replace-by-key (one entry per key, latest value); `evict_while` returns entries in
`buffer_time` order and only those `<= threshold`; entries above threshold remain.

## 7. Phasing (non-overlapping file sets per batch)

- **Batch 1 (parallel):** T1 `TimeOrderedKeyValueBuffer` (`suppress_buffer.rs`) ∥ T2
  `Suppressed`/`BufferConfig` (`suppress.rs`) + re-exports.
- **Batch 2:** T3 `KTableSuppressProcessor` (`suppress.rs` processor module) — needs T1.
- **Batch 3:** T4 grace plumbing (`ktable.rs` field + `windowed_kgrouped.rs` /
  `session_windowed_kgrouped.rs` set it + `map_values`/`filter` propagate) + the
  `KTable::suppress` method + `names.rs` — needs T2, T3.
- **Batch 4:** T5 execution tests — needs T4.
- **Batch 5 (Phase C):** T6 Capture.java + Docker capture (controller) +
  `suppress_until_window_closes` golden + test; then T7 docs (lib.rs prose) + final
  verification (full suite, 13 goldens, clippy `--all-targets`, fmt).

## 8. Risks / open items

- **Suppress node name prefix + whether logging-disabled is wire-invisible** — pinned
  by the Phase C capture; the `names.rs` prefix + the expectation that the golden
  equals `windowed_count` are confirmed then (Risk: if logging-disabled still emits a
  topic, or the JVM suppress node shifts a wire-visible name, tune to the capture).
- **`old` in the emitted `Change`** — forwarded verbatim from the buffer (matches JVM);
  downstream `toStream` uses `new`. Not wire-constrained; validated by execution.
- **Grace propagation through derived tables** — handled for `map_values`/`filter`;
  the direct `agg → suppress` path is the tested one.
- **Session + suppress** — works through the same processor (session aggregations
  produce `Windowed<K>` keys with `window.end`), but Slice A's golden/execution use
  **time** windows; a session+suppress execution test is a cheap add if desired.
