# KIP-1071 Streams Client — Sliding Windows (KIP-450)

**Date:** 2026-06-08
**Status:** Design approved, pending spec review
**Scope:** A self-contained client-side DSL slice adding sliding-window
aggregations (`count`/`reduce`/`aggregate`) to `crates/client-streams`. No
broker or wire-protocol changes.

## 1. Context

The KIP-1071 streams **client** runtime (`crates/client-streams`, crate
`crabka-client-streams`) is feature-rich: the original 7-sub-project program
plus FK-join, global table, suppress, punctuation, EOS, standby/warmup, and
schema-serde have all merged. Time windows (tumbling/hopping) and session
windows are implemented; **sliding windows (KIP-450) are the remaining
windowing gap** (`cogroup`/KIP-150, versioned KTables/KIP-889, and
emit-final/KIP-825 are separate future slices).

The crate models each window type as a **triplet**: a `*WindowedKGroupedStream`
DSL handle, an aggregation processor, and a store + output-key serde. Session
windows sit beside time windows this way. This slice adds sliding windows as a
fourth parallel member, **reusing** the existing `WindowBytesStore` and
`TimeWindowedSerde` — in the JVM, `KGroupedStream.windowedBy(SlidingWindows)`
returns the same `TimeWindowedKStream` and materializes into the same window
store with the same `key‖windowStart:8B-BE` output-key layout as time windows.
All the novelty lives in one new processor implementing the KIP-450 algorithm.

## 2. Goal and non-goals

### Goal

A Rust app can write, with full JVM behavioral parity including out-of-order
records:

```rust
stream
    .group_by_key()
    .windowed_by_sliding(SlidingWindows::of_time_difference_and_grace(100, 50))
    .count("counts");   // KTable<Windowed<K>, i64>
```

and the topology serializes byte-for-byte identically to JVM Kafka Streams 4.1
(`optimization=all`), while the aggregation output matches the JVM for both
in-order and out-of-order input.

### Non-goals (deferred)

- **Emit-final / `EmitStrategy.onWindowClose`** (KIP-825) — emit-on-update only,
  consistent with the rest of the crate's windowed aggregations.
- **Sliding-window joins** — KIP-450 is aggregation-only; stream-stream joins
  already use `JoinWindows`.
- **Versioned stores, cogroup** — separate slices.
- No backwards-compat shims (greenfield, per `CLAUDE.md`).

## 3. Design

### 3.1 `SlidingWindows` (`dsl/windows.rs`)

```rust
pub struct SlidingWindows {
    pub time_difference_ms: i64,   // window size W; windows are inclusive [start, start + W]
    pub grace_ms: i64,
}
```

Constructors mirroring the JVM:
- `of_time_difference_with_no_grace(time_difference_ms)` — grace 0.
- `of_time_difference_and_grace(time_difference_ms, grace_ms)`.

`time_difference_ms >= 0`, `grace_ms >= 0` (assert). Sliding windows are
data-defined, not epoch-aligned, so there is **no `windows_for`** — affected
windows are discovered by scanning the store (§3.3). Window bounds are
**inclusive** on both ends (`end = start + time_difference_ms`); the `Window`
struct's end interpretation is carried by the operator, as the existing doc
comment states.

### 3.2 DSL handle (`dsl/sliding_windowed_kgrouped.rs`)

A near-clone of `windowed_kgrouped.rs`:

- `KGroupedStream::windowed_by_sliding(SlidingWindows) ->
  SlidingWindowedKGroupedStream<K, V>` (distinct method name because Rust cannot
  overload `windowed_by` by argument type — same reason `windowed_by_session`
  exists).
- Terminal ops `count`/`reduce`/`aggregate` (+ `count_explicit`/
  `reduce_explicit`/`aggregate_explicit`), each returning
  `KTable<Windowed<K>, _, TimeWindowedSerde<KS>, VS>`.
- Lowering reuses `KGroupedStream::record_repartition`, `mint_store_name`,
  `Topology::add_window_store` (size = `time_difference_ms`, grace = `grace_ms`),
  `TimeWindowedSerde::new(key_serde, time_difference_ms)` for the output key, and
  a sliding analogue of `windowed_suppress_factory`.
- Result table carries `.with_window_grace(Some(grace_ms))` and the suppress
  factory, exactly like the time-windowed path.

The store-name-index burn behavior (JVM bumps the index by one on the unnamed
`count` path for time windows; `reduce`/`aggregate` do not) is **whatever the
captured golden shows** — verified, not assumed.

### 3.3 Processor (`dsl/processors/sliding_window_aggregate.rs`)

Two processors, mirroring the time-window pair:

- `KStreamSlidingWindowAggregateProcessor<K, V, VA, I, A>` — count/aggregate
  (`init` + `agg`).
- `KStreamSlidingWindowReduceProcessor<K, V, R>` — first value in a window seeds
  the accumulator, later values fold via `reducer(&acc, &v)`.

Both are `Processor<K, V, Windowed<K>, Change<VA>>`, emit-on-update, and require
a non-null key (enforced by the preceding repartition). The body is a faithful
port of JVM `KStreamSlidingWindowAggregate` (apache/kafka 4.1 is the source of
truth):

For a record with key `k`, value `v`, timestamp `t`, window size `W`:

1. **Late-record drop.** Maintain observed stream time per task; drop (and, per
   JVM, count as a late record) if `t < streamTime - (W + grace_ms)`.
2. **Scan** the key's existing windows via `WindowStore::fetch(k, max(0, t-2W),
   t)`, classifying each found window `[ws, ws+W]` by where its end sits relative
   to `t` to determine: whether the record's **left window** `[t-W, t]` already
   exists, whether records exist that require creating the record's **right
   window** `[t+1, t+1+W]`, and which already-materialized windows the record
   now falls inside (the out-of-order case) and must be updated.
3. **Create/update** the left window, the right window (when warranted), and any
   straddling windows, computing each window's new aggregate from the scanned
   contents plus `v`. Each created/updated window emits a `Change<VA>` keyed by
   `Windowed { key: k, window: { start, end: start + W } }`, with
   `new_ts = max(t, storedTs)` (matching the `ValueAndTimestamp` store value).

The exact branch structure (the JVM's `processInOrder` left/right-window logic
plus the out-of-order path) is ported and pinned by the behavioral golden
(§3.5); the design commits to *behavioral equivalence with the JVM*, not to a
paraphrase of the branches here.

### 3.4 Wiring

- Re-export `SlidingWindows` and `SlidingWindowedKGroupedStream` from `dsl/mod.rs`
  and the `lib.rs` prelude `pub use` block.
- Add a `lib.rs` module-doc section describing sliding windows, beside the
  existing time/session window prose.

### 3.5 Verification — two gates

**Structural (existing harness).** Add `sliding_window_count` (and, to exercise
the non-burn path, `sliding_window_aggregate`) topologies to
`tests/jvm-capture` (`Capture.java` + `run.sh` fixture list), producing
`testdata/golden/dsl/sliding_window_*.topology.json`. Assert byte-equality in
`dsl_golden_frame.rs`. This pins processor/store names, store-name-burn, the
window-store changelog config (`cleanup.policy=compact,delete`,
`retention.ms = W + grace + 86_400_000`), and copartition/subtopology shape.

**Behavioral (new infra).** Extend the JVM-capture harness with a
`TopologyTestDriver`-based runner that feeds a fixed battery of input records —
**including out-of-order timestamps** — through the JVM sliding-window topology
and dumps each output `(windowedKey, oldAgg, newAgg, recordTs)` in emission
order to a committed JSON golden under `testdata/golden/dsl/behavioral/`. A new
test (extending `dsl_execution.rs` or a sibling) replays the identical inputs
through the Rust runtime's `TopologyTestDriver` and asserts the output sequence
matches exactly. This is the real fidelity gate for the out-of-order algorithm;
hand-derived expectations are insufficient for KIP-450.

The behavioral runner is written generically enough to be reused for the
existing time/session window types in a later cleanup (not in scope here beyond
what sliding needs).

## 4. Testing strategy

- TDD: write the behavioral golden replay test first (red), then port the
  processor until green.
- Unit tests on the processor mirror `window_aggregate.rs`'s in-process
  `Dispatch`/`ProcessorContext` harness, with explicit out-of-order sequences.
- `SlidingWindows` constructor + bound-assert unit tests.
- Full `cargo test -p crabka-client-streams` is the erasure-safety gate (DSL
  type mismatches are runtime downcast failures, not compile errors).
- `cargo fmt --check` and `cargo clippy --workspace --all-targets -D warnings`
  before push.

## 5. Files touched

New:
- `crates/client-streams/src/dsl/sliding_windowed_kgrouped.rs`
- `crates/client-streams/src/dsl/processors/sliding_window_aggregate.rs`
- `crates/client-streams/tests/testdata/golden/dsl/sliding_window_*.topology.json`
- `crates/client-streams/tests/testdata/golden/dsl/behavioral/sliding_window_*.json`

Modified:
- `src/dsl/windows.rs` (`SlidingWindows`)
- `src/dsl/kgrouped.rs` (`windowed_by_sliding`)
- `src/dsl/mod.rs`, `src/lib.rs` (re-exports + module doc)
- `src/dsl/processors/mod.rs` (module decl)
- `tests/jvm-capture/{src/main/java/crabka/capture/Capture.java, run.sh,
  build.gradle}` (structural fixtures + behavioral runner)
- `tests/dsl_golden_frame.rs`, `tests/dsl_execution.rs` (assertions)

## 6. Risks

- **Algorithm fidelity.** KIP-450's out-of-order branch structure is intricate;
  the behavioral golden is the mitigation — any divergence shows up as a
  sequence mismatch on captured JVM output.
- **Behavioral-capture infra.** Running `TopologyTestDriver` in the Docker
  harness and emitting a deterministic, ordered output dump is new; it must be
  deterministic (fixed input, no wall-clock punctuation) to be a stable golden.
- **`fetch` range semantics.** The scan window `[max(0,t-2W), t]` and the
  store's half-open vs inclusive range handling must align with the JVM's
  `fetch(from, to)` (inclusive) — covered by the behavioral golden's
  boundary-timestamp cases.
