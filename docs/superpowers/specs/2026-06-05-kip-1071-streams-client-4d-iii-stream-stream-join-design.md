# KIP-1071 Streams Client — Sub-project #4d-iii: windowed KStream-KStream join (inner/left/outer)

**Date:** 2026-06-05
**Status:** Design approved, pending spec review
**Scope:** Third windowing slice — the windowed stream-stream join with inner, left,
and outer variants, byte-exact vs JVM 4.1.
**Builds on:** 4d-ii (window store + `WindowKeySchema` codec + windowed changelog;
PR #396) + 4c-ii/iii join machinery (copartition, `connect_processor_store`, dual
processors + merge). Branch `streams-4d-iii-stream-join` (stacked on
`streams-4d-ii-windowstore`; rebase onto `main` once #396 merges). Worktree
`/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`.

## 1. Context & program decomposition

Windowing (4d): 4d-i (async + pluggable store, #391) → 4d-ii (window store +
windowed aggregations, #396) → **4d-iii** (this spec — windowed stream-stream join)
→ 4d-iv (session store + session aggregations). 4d-ii built the window store + the
`WindowKeySchema` byte codec; 4d-iii extends them for the **`retainDuplicates`** join
stores and adds the dual join processors + the KIP-633 left/outer emit-on-close.

## 2. Goal & non-goals

### Goal
A windowed KStream-KStream join in the DSL:
```rust
KStream<K,V>::join<V2,VO,F>(&self, other: &KStream<K,V2>, joiner: F, windows: JoinWindows) -> KStream<K,VO>      // inner; F: Fn(&V,&V2)->VO
KStream<K,V>::left_join<V2,VO,F>(&self, other, joiner, windows) -> KStream<K,VO>                                  // left;  F: Fn(&V, Option<&V2>)->VO
KStream<K,V>::outer_join<V2,VO,F>(&self, other, joiner, windows) -> KStream<K,VO>                                 // outer; F: Fn(Option<&V>, Option<&V2>)->VO
```
- A record on side A at `tA` matches side-B records with timestamp in `[tA−before,
  tA+after]` (and the mirror for B, **with before/after swapped**); each match emits
  `joiner(a,b)` at `max(tA,tB)`.
- **inner**: emit only on matches. **left/outer**: buffer unmatched records and emit
  the null-padded result when the window **closes** (stream-time-driven).
- Two `retainDuplicates` window stores + (for left/outer) one shared outer-join
  store; both sources declared a **copartition group**.
- Byte-exact vs JVM 4.1: the topology + the window/outer-store changelog configs +
  the `retainDuplicates` `WindowKeySchema` changelog records.

### Non-goals (deferred)
- The JVM **wall-clock emit throttle** (`EMIT_INTERVAL_MS`=1s) — Crabka runs the
  close-scan **stream-time-only** (same result set, deterministic, testable).
- **GlobalKTable** / **foreign-key** / **self** joins; **sliding** windows; the
  windowed-**aggregation** closing (4d-ii's deferral stands — this slice adds only
  *join*-specific close emission).

## 3. `JoinWindows` + the retainDuplicates join store

- **`JoinWindows`** (`dsl/windows.rs`): `{ before_ms, after_ms, grace_ms }` with
  `JoinWindows::of(diff)` (before=after=diff, grace 0), `.before(d)`, `.after(d)`,
  `.grace(g)`. `size() = before + after`; store/changelog retention = `before +
  after + grace + 86_400_000`.
- **Seqnum codec** (`store/window_schema.rs`): `store_key` gains a `seqnum: u32` param
  — `key_bytes ‖ windowStart:8BE ‖ seqnum:4BE`. 4d-ii aggregation callers pass `0`
  (bytes unchanged); the join store passes an incrementing value.
- **`JoinWindowStore<K,V>` trait + `JoinWindowBytesStore<K,V>`** (`store/join_window.rs`,
  NEW) — a second window store beside 4d-ii's `WindowBytesStore`, over the same async
  `ByteKeyValueStore`, with three deliberate differences:
  - `async fn put(&mut self, key: K, timestamp: i64, value: V)` — writes `store_key(kb,
    ts, self.seqnum)` with a **per-store monotonic seqnum** (`self.seqnum =
    (self.seqnum + 1) & 0x7FFF_FFFF` per put; mirrors `RocksDBWindowStore`), so
    duplicates at the same `(key, ts)` coexist.
  - **raw value** — `value_serde.serialize(v)` directly, NO `ValueAndTimestamp` wrap
    (join stores are plain `WindowStore`).
  - `async fn fetch(&self, key, time_from, time_to) -> Vec<(i64, V)>` — returns **all**
    records in `[from, to]` (every seqnum), range `[kb‖from‖0, kb‖(to+1)‖0)` + the
    prefix-collision guard.
  - Changelog buffers `(composite_key_with_seqnum, Some(raw_value))`; restore replays
    into the backend; the seqnum counter resets to 0 on restore (matches the JVM,
    which doesn't persist it — an accepted short-window edge).
  - Registry: `get_join_window::<K,V>` → downcasts to `JoinWindowBytesStore<K,V>`;
    `ProcessorContext::get_join_window_store`.
  - 4d-ii's aggregation `WindowBytesStore` (seqnum 0, ts-wrapped, fetch-single) is
    untouched.

## 4. Inner join processors (dual + merge)

`KStreamKStreamJoinProcessor<K, VThis, VOther, VO, F>` (`dsl/processors/stream_join.rs`,
NEW; `Processor<K, VThis, K, VO>` — output is a plain `KStream` record, not `Change`).
Per record `(k, v_this)` at `t`:
1. **put own** — `own_store.put(k, t, v_this)` (raw value, auto-seqnum into *this
   side's* store).
2. **fetch other** — `other_store.fetch(k, t − fetch_before, t + fetch_after)`.
3. **emit per match** — for each `(t_other, v_other)`: forward `joiner(...)` keyed `k`
   at `max(t, t_other)`.

**Before/after swap:** the joiner is stored in canonical outer form `Fn(Option<&VA>,
Option<&VB>)->VO` (with a `JoinKind{a_required,b_required}` per join type). The **THIS**
processor (drains stream A) uses `fetch_before=before, fetch_after=after`,
`joiner(Some(a_current), Some(b_fetched))`. The **OTHER** processor (drains stream B)
uses `fetch_before=after, fetch_after=before` (**swapped**), `joiner(Some(a_fetched),
Some(b_current))`. Both call `joiner` A-then-B; the asymmetric window is correct
(a B-record at `tB` matches an A-record `tA` iff `tA−before ≤ tB ≤ tA+after`, i.e.
`tB−after ≤ tA ≤ tB+before`).

Two `JoinWindowStore`s (one per side); each processor `connect_processor_store`s both
the store it writes and the store it reads → grouping unions A, B, both stores, and
the join into one copartitioned subtopology. A **MERGE** node unions the two outputs
→ the result `KStream<K,VO>`. The JVM splits put (`KSTREAM-WINDOWED-`) from fetch
(`KSTREAM-JOINTHIS/JOINOTHER`) + `KSTREAM-MERGE`; the wire topology exposes only
stores/changelogs/copartition (not processor node names), so the internal
decomposition is validated against the captured fixture (counter-burn discipline to
land the store-name indices exactly).

## 5. left/outer: window-close emission (KIP-633)

Three new pieces (all gated to left/outer):

1. **Shared outer-join store** (`KSTREAM-OUTERSHARED-`) — a KV store keyed
   `(timestamp:8BE ‖ side:1 ‖ key_bytes)` (sorts by time→side→key), value = a tagged
   `LeftOrRight(VA | VB)` (the unmatched value). Both processors connect to it. Its
   changelog is a standard KV changelog (config pinned by the capture).
2. **Shared stream-time tracker** — `{ stream_time, min_time }` shared between the
   THIS/OTHER processors via one `Arc<Mutex<TimeTracker>>` created in the lowering and
   captured by both processor suppliers (Send+Sync; uncontended — single-threaded
   task). `stream_time` = max record ts across both sides.
3. **Emit-on-close, in each `process()`** (after the inner logic):
   - bump `stream_time = max(stream_time, t)`.
   - **unmatched** (outer/left, no fetch match): if the outer store is empty **or** the
     record's window already closed (`t + fetch_after < stream_time`), emit the
     null-padded result eagerly; else buffer it at `(t, side, k)`.
   - **close scan**: iterate the outer store in timestamp order; for each buffered
     `(ts, side, k)` whose window has closed — `min_time + lookback(side) + grace <
     stream_time`, **lookback = after for a left-side (A) record, before for a
     right-side (B) record** — emit its null-padded result at `ts` and delete it; stop
     at the first still-open entry.

**Simplification (flagged):** the JVM throttles the close-scan with a 1s wall-clock
gate + a system-time punctuator. Crabka drops the wall-clock throttle and runs the
close-scan stream-time-only every `process()` — same emitted result set,
deterministic, testable in `TopologyTestDriver`. Emission *cadence* may differ from a
wall-clock JVM; the result bytes do not.

## 6. DSL ops + lowering + windowed-join changelog config

- **DSL** (`dsl/kstream.rs`): `join`/`left_join`/`outer_join` wrap the user joiner to
  the outer form + `JoinKind` (inner: both `expect`; left: `a.expect`; outer: direct).
- **Lowering**: mint the join node names + the two window-store names (+ the shared
  outer-store name for left/outer) to match the captured fixture; record the THIS/OTHER
  join processors (sharing one `Arc<Mutex<TimeTracker>>` for left/outer) + a MERGE
  node; register two stores via `add_join_window_store` and (left/outer) the shared
  outer store via `add_state_store`; `connect_processor_store` each processor to the
  stores it touches; `add_copartition_group([a_src, b_src])`. Return `KStream<K,VO>`.
  Both inputs must be copartitioned; a key-changed stream must `.repartition(..)`
  first (reuse 4c-ii's eager-panic convention).
- **Windowed-join changelog config** (a *third* variant): `add_join_window_store`
  registers a windowed store whose changelog is `cleanup.policy=delete` (NOT
  `compact,delete` — retainDuplicates can't compact) + `retention.ms = before + after +
  grace + 86_400_000`. Threaded by widening `StoreEntry`'s windowed marker to an enum:
  `Kv` (compact) | `AggWindow` (compact,delete + retention) | `JoinWindow` (delete +
  retention). `wire.rs` picks the config per store kind. The shared outer store is a
  plain KV store (standard `compact` changelog); the exact config is pinned by the
  capture.

## 7. JVM capture & golden frames

Add to `Capture.java`:
- `streamStreamJoin()` = `streamA.join(streamB, (a,b)->a+b, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(60)))` → `to("out")` (inner — two `delete` window-store changelogs + copartition `[0,1]`).
- `streamStreamOuterJoin()` = the same with `outerJoin` (left≈outer topology — + the shared outer KV store + its changelog).

Capture via the Docker Kafka-Streams 4.1 harness → `testdata/golden/dsl/{stream_stream_join,stream_stream_outer_join}.topology.json`. Expected (pinned by capture): one subtopology, `source_topics: ["left","right"]` (or the capture's names), the two window-store changelogs (`cleanup.policy=delete` + `retention.ms`), the copartition group, and (outer) the shared outer-store changelog. The **9 prior golden frames stay byte-identical**.

## 8. Testing strategy (gates)

1. **Unit:** `JoinWindows` (before/after/size/grace + `.before`/`.after` asymmetry);
   the seqnum codec param; `JoinWindowBytesStore` (incrementing seqnum on duplicate
   puts, fetch returns every duplicate, raw value, changelog bytes).
2. **Golden:** `stream_stream_join` + `stream_stream_outer_join` byte-match the JVM
   fixtures; the **9 prior goldens stay byte-identical**.
3. **Execution** (`TopologyTestDriver`): inner (A+B in window → joined; outside → none;
   the before/after **swap** with an asymmetric `.before`/`.after`); duplicates (two A's
   + one B → two results); left (A with no B → `joiner(a,None)` emitted only after
   stream-time closes the window); outer (both sides null-padded on close).
4. **Regression:** all prior 4d / #4 / #2 / #3 tests stay green.

## 9. Success criteria
- `KStream::join`/`left_join`/`outer_join` with `JoinWindows` work (execution:
  inner/left/outer + the swap + duplicates + close emission) and the topology +
  window/outer-store changelog configs byte-match captured JVM 4.1 output; the
  `retainDuplicates` changelog records use byte-exact `WindowKeySchema` (incrementing
  seqnum) + raw values.
- The 9 prior golden frames unchanged.
- `cargo test -p crabka-client-streams` green; `cargo clippy --workspace
  --all-targets -- -D warnings` + `cargo fmt --check` clean; `cargo build
  --workspace`.
- A documented stream-stream-join note in `lib.rs`.

## 10. Plan phasing (largest slice in the program)

The implementation plan phases this so inner is a green, reviewable milestone before
the emit-on-close subsystem:
- **Phase A:** seqnum codec param + `JoinWindowBytesStore` + `add_join_window_store` +
  the `JoinWindow` changelog variant (units + a wire test).
- **Phase B:** `JoinWindows` + the inner dual processors + DSL `join` + inner golden +
  inner/duplicates/swap execution tests.
- **Phase C:** the shared outer store + `Arc` stream-time tracker + emit-on-close +
  `left_join`/`outer_join` DSL + outer golden + left/outer execution tests.
- Stays one spec/PR unless it proves too big at plan time (then Phase C may split into
  its own PR stacked on Phases A+B).

## 11. Open points for the plan
- **Store-name indices / the JVM `KSTREAM-WINDOWED-` nodes** — the capture pins the
  two window-store names + the outer-store name + their counter positions; the
  lowering mints/burns to match (Step 7 captures first).
- **The shared outer-store changelog config** — `compact` (standard KV) vs something
  else; the outer-join fixture is the oracle.
- **`Arc<Mutex<TimeTracker>>` sharing across the two suppliers** — confirm the supplier
  closures (`Fn() -> Processor + Send + Sync`) can both capture a clone of the same
  Arc; the runtime instantiates each processor once per task.
- **`LeftOrRight` tagged-value + composite-key serde** for the outer store — match the
  JVM `LeftOrRightValue` / `TimestampedKeyAndJoinSide` byte layout if the changelog is
  to interop (else a Crabka-internal layout, since the outer store is not the join's
  primary state) — pin against the outer-join fixture / flag in the plan.
- **`Windowed`-vs-plain output** — the join output is `KStream<K,VO>` (plain records),
  so no `Windowed<K>` key here (unlike 4d-ii aggregations); the merge unions plain
  records.
