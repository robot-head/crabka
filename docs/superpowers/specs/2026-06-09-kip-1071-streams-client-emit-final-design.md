# KIP-1071 Streams Client — Emit-Final (KIP-825) Design

**Date:** 2026-06-09
**Status:** Approved (brainstorming)
**Crate:** `crabka-client-streams` (`crates/client-streams`)
**Predecessor slice:** sliding windows (KIP-450), merged 2026-06-09 (#474 feat + #475 goldens)

## Summary

Add native **emit-on-window-close** (`EmitStrategy.ON_WINDOW_CLOSE`, KIP-825) to all
three windowed aggregation surfaces — time (tumbling/hopping), sliding (KIP-450),
and session windows. This closes the last "emit semantics" gap: today every windowed
aggregation is **emit-on-update**, and the only way to get final-only results is the
`suppress(untilWindowCloses)` workaround, which is a *separate downstream node + buffer
store*. Native emit-final is configured **on the windowed aggregation itself** and emits
finals **directly from the existing window store** — no extra node, no extra store.

## Scope

In scope:

- `EmitStrategy` DSL type mirroring the JVM (`on_window_update()` / `on_window_close()`).
- `.emit_strategy(EmitStrategy)` builder method on all three windowed handles.
- Emit-on-close behavior added as a **mode branch** on the four aggregate processors
  (time aggregate, time reduce, sliding aggregate, session aggregate).
- A new cross-key "windows closing in range" scan method on the window store and the
  session store.
- Late-record drop on the emit-close path.
- Three JVM behavioral golden fixtures (time / sliding / session).

Out of scope:

- Versioned KTables (KIP-889/962) — separate, larger follow-on slice.
- Cogroup (KIP-150).
- Persisting the emit watermark across restart (JVM does not either; see Edge Cases).

## Architecture — the core distinction

Native emit-final is fundamentally different from the existing
`suppress(untilWindowCloses)` workaround:

| | `suppress(untilWindowCloses)` (exists) | Native emit-final (this slice) |
|---|---|---|
| Where configured | Downstream `.suppress()` on the result `KTable` | On the windowed handle, *before* the terminal agg |
| Topology | Adds a separate `KSTREAM-SUPPRESS` node + buffer store + changelog | **No new node, no new store** — same `KSTREAM-AGGREGATE` node |
| Emit source | A dedicated suppress buffer | Reads finals straight from the existing window store |
| Aggregate node | Still emits-on-update (just buffered later) | Itself stops emitting on update |

**Fidelity anchor:** emit-final is a *behavioral mode on the aggregate processor*, not a
new pipeline stage. The JVM parameterizes one `KStreamWindowAggregate` class by
`EmitStrategy`; the topology bytes are **identical** whether emit-on-update or
emit-on-close is chosen. Therefore lowering and store registration stay byte-for-byte the
same; only `process()` branches. The goldens must confirm the emit-final topology equals
the emit-on-update topology.

## Components

### 1. `EmitStrategy` DSL type (`src/dsl/emit.rs`, new)

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmitStrategy { kind: EmitKind }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EmitKind { OnWindowUpdate, OnWindowClose }

impl EmitStrategy {
    pub fn on_window_update() -> Self // default
    pub fn on_window_close() -> Self
}
```

### 2. `.emit_strategy()` on the windowed handles

Added to `TimeWindowedKGroupedStream`, the sliding handle
(`SlidingTimeWindowedKGroupedStream`), and the session handle. Stores an
`emit: EmitStrategy` field (default `on_window_update()`), threaded into the processor
constructor at lowering. **Lowering is otherwise untouched** — same node kind, same store
registration, same name minting, so the wire topology is unchanged.

### 3. Processor mode branch

Add three fields to each of the four aggregate processor structs
(`KStreamWindowAggregateProcessor`, `KStreamWindowReduceProcessor`, the sliding
aggregate, the session aggregate):

- `emit: EmitStrategy`
- `observed_stream_time: i64` (init `i64::MIN`)
- `last_emitted_close: i64` (init `i64::MIN`) — the re-emit-prevention watermark.

`process()` branches on `emit`:

- **OnWindowUpdate** → current behavior verbatim (forward `Change(old, new)` per window).
  Regression-guarded.
- **OnWindowClose** → update the store + advance `observed_stream_time`; **do not** forward
  the update; then run `emit_closed_windows()`.

`emit_closed_windows()`:

1. `window_close_time = observed_stream_time - grace_ms`.
2. Scan the store for windows with `window.end <= window_close_time` **and**
   `window.end > last_emitted_close`.
3. Forward each as a final `Change` (see Emit Semantics for the exact shape), in store
   order.
4. `last_emitted_close = window_close_time`.

### 4. New store scan method

The typed window store currently exposes only per-key reads (`fetch_single`,
`fetch(key, from, to)`). Finding closed windows *across all keys* needs a new method —
the byte key layout is `key‖windowStart:8B‖seq:4B` (key-prefixed), so a `range()`
shortcut over window-start is impossible; the scan filters a `scan_all()`:

```rust
// window store
async fn fetch_all_in_range(&self, time_from: i64, time_to: i64)
    -> Vec<(K, i64 /*window_start*/, i64 /*record_ts*/, V)>;
```

Session store gets the analogous scan keyed by `session.end`.

## Emit semantics & fidelity unknowns

- **Emitted `Change` shape (highest risk):** JVM emit-final forwards each final as
  `Change(new, old = null)` — a one-shot, not an update pair. Implement `Change(final,
  None)` but treat the exact shape (old=null vs old=prior) as **capture-decided**; the
  first golden run confirms it.
- **Empty / seed-only windows:** only windows present in the store emit; a window that
  never received a record (would hold only the `init` seed) does not emit.
- **Late-record drop:** the emit-close path drops updates for already-closed windows
  (`record_ts ≤ window_close_time`), matching JVM. Scoped to the **emit-close path only**
  — the emit-update path keeps its current (non-dropping) behavior unchanged.
- **No end-of-stream flush:** finals fire only as later records advance stream-time. JVM
  has no wall-clock punctuator for this and no flush on `close()`. The last window stays
  unemitted until a record pushes stream-time past its close. Goldens pin exactly this.

## Window-type specifics

- **Time (tumbling/hopping):** scan by `window.end ≤ window_close_time`. Straightforward.
- **Sliding (KIP-450):** up to two windows (left/right) per record; scanned identically by
  `window.end`. No special handling beyond reusing the new scan on the sliding store.
- **Session:** sessions merge adjacent entries within the inactivity gap as records arrive;
  a session is final only once `stream_time > session.end + grace` with no further merge
  possible. Scan by `session.end ≤ window_close_time` using the session triplet
  (`store/session.rs` + `store/session_schema.rs`). The merge logic itself is unchanged —
  emit-final only changes *when/whether* the merged result is forwarded.

## Edge cases

- **Watermark not persisted:** `last_emitted_close` lives on the processor (like suppress's
  `observed_stream_time`), not in the changelog. On restart the window store restores from
  changelog but the watermark resets to `i64::MIN` → possible re-emit of already-closed
  windows. This **matches JVM** (re-derives, may re-emit). `TopologyTestDriver` / goldens
  never restart mid-stream, so it is invisible to the gate. Documented as a known,
  JVM-consistent edge.

## Testing

### Unit tests (per processor; fast, deterministic)

Mirror the `window_aggregate.rs` / `suppress.rs` style (hand-built
`Dispatch`/`ProcessorContext`). Per window type:

- emit-on-update path unchanged (regression guard).
- emit-on-close: records in an open window forward **nothing**; a later record advancing
  stream-time past `end + grace` forwards exactly one final per closed window; watermark
  prevents re-emit; late record for a closed window is dropped.
- grace delays close (threshold = `stream_time - grace`).

### Behavioral goldens (the real gate)

Capture from **mirror.gcr.io/apache/kafka:4.1.0** Streams under
`crates/client-streams/tests/jvm-capture/`, one fixture per window type:

1. `emit_final_time_window` — tumbling count, `EmitStrategy.ON_WINDOW_CLOSE`.
2. `emit_final_sliding_window` — sliding aggregate.
3. `emit_final_session_window` — session count (exercises merge-then-close).

Each fixture pins the byte-exact output records (key layout, the `Change` old/new shape,
ordering) **and** the wire topology — confirming emit-final adds **no** extra node/store vs
the emit-on-update topology (the architecture anchor). Cross-validate against a live
`mirror.gcr.io/apache/kafka:4.1.0` broker as the other fixtures do.

### CI / coverage discipline

- Erasure type mismatches are **runtime downcast panics**, not compile errors, so the full
  golden suite is the gate.
- Use `tools/regenerate.sh` discipline for any protocol/codegen touchpoints.
- Add any new `tests/<x>.rs` to the crate's `llvm-cov --test` list in `ci.yml`, or it
  reports 0% patch coverage.

## File touch list

- `src/dsl/emit.rs` — new `EmitStrategy` type.
- `src/dsl/mod.rs` — export `EmitStrategy`.
- `src/dsl/windowed_kgrouped.rs` — `.emit_strategy()` + thread into time aggregate/reduce.
- `src/dsl/sliding_windowed_kgrouped.rs` — `.emit_strategy()` + thread into sliding aggregate.
- `src/dsl/session_windowed_kgrouped.rs` — `.emit_strategy()` + thread into session aggregate.
- `src/dsl/processors/window_aggregate.rs` — mode branch (aggregate + reduce).
- `src/dsl/processors/sliding_window_aggregate.rs` — mode branch.
- `src/dsl/processors/session_aggregate.rs` — mode branch.
- `src/store/window.rs` — `fetch_all_in_range`.
- `src/store/session.rs` — session-end range scan.
- `tests/jvm-capture/…` — three new fixtures + harness wiring.
- `ci.yml` — llvm-cov `--test` entry if a new integration test file is added.
