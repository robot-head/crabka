# KIP-1071 streams client — punctuation (`ProcessorContext::schedule` + stream-time & wall-clock timers)

**Status:** design approved (brainstorm)
**Builds on:** #2 Processor API (`Processor`/`ProcessorContext`/erased graph driver + `init`/`close` lifecycle), #2b runtime (`StreamThread`/`StreamTask`), #3 state stores. Branches from `main` (independent of the open process/process_values PR #420 — different files; rebase if it lands first).
**Ground truth:** Apache Kafka Streams 4.1 — KIP semantics for `ProcessorContext.schedule` + the JVM `TopologyTestDriver` punctuation behavior (captured empirically to pin timing).

## 1. Goal

Let a custom Processor-API node register **punctuators** — periodic callbacks that fire on stream-time or wall-clock-time boundaries and may `forward(...)` records downstream and read/write state stores. This is the KIP-820 / Processor-API `schedule` capability, repeatedly deferred by the suppress and process/process_values slices, and the foundation for timer-driven emission.

## 2. Scope

### In scope
1. **`PunctuationType { StreamTime, WallClockTime }`** — both JVM punctuation types.
2. **`Punctuator<K, V>`** trait — `async fn punctuate(&mut self, ctx: &mut ProcessorContext<'_,'_,K,V>, timestamp: i64)`. A trait object (like `Processor`), implemented on a user struct; shares state with its owning processor via `Arc<Mutex<_>>` (the established `stream_join` `TimeTracker` pattern).
3. **`ProcessorContext::schedule<P>(&mut self, interval: Duration, ty: PunctuationType, p: P) -> Cancellable`** — register a punctuator. Callable from `init` and from `process`.
4. **`Cancellable`** — `.cancel()` stops a schedule; the driver skips and drops cancelled entries.
5. **Firing** in the `Graph` driver: `stream_time` tracking + `punctuate_stream_time` + `punctuate_wall_clock`, each with first-fire + catch-up, positioned at the scheduling node so a punctuator's `forward` flows downstream.
6. **`TopologyTestDriver`**: auto-fire stream-time punctuations after each `pipe_input`; add **`advance_wall_clock_time(Duration)`** to fire wall-clock punctuations against a mock clock.
7. **`StreamThread`**: drive wall-clock punctuation between polls via an injected `Clock` (defaults to system time); drive stream-time punctuation after each processed batch.
8. A capture-first **behavior pin** against the JVM `TopologyTestDriver` for the exact firing semantics (first-fire offset, catch-up, per-type timestamp passed).

### Non-goals (deferred)
- **Closure-style punctuator sugar** (`schedule(.., |ctx, ts| async {...})`) — trait-only this slice (async-closure erasure is its own problem).
- **DSL `schedule`** — punctuation is Processor-API only; no `KStream`/`KTable` operator.
- **Commit-interval / "enforced" punctuation** and **standby-task punctuation**.
- **Wire/topology changes** — none; punctuation is pure runtime (a punctuating processor is an ordinary processor node in the KIP-1071 wire).

## 3. Architecture

Punctuation is a **runtime** feature layered onto the existing erased graph driver. No wire/topology bytes change.

```
ProcessorContext::schedule(interval, ty, p)
        │  (records into the Dispatch, tagged with the current node_idx)
        ▼
Graph.schedules: Vec<ScheduleEntry>     ← one per live punctuator
        │
        ▼  driven by:
TopologyTestDriver  →  Graph::punctuate_stream_time(stream_time)  (after each pipe_input)
                    →  Graph::punctuate_wall_clock(mock_now)       (advance_wall_clock_time)
StreamThread        →  task.punctuate_stream_time(...)             (after a processed batch)
                    →  task.punctuate_wall_clock(clock.now())      (between polls)
```

### 3.1 Punctuator + erasure (mirrors `ErasedNode`)

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PunctuationType { StreamTime, WallClockTime }

#[async_trait]
pub trait Punctuator<K: Send, V: Send>: Send + 'static {
    async fn punctuate(&mut self, ctx: &mut ProcessorContext<'_, '_, K, V>, timestamp: i64);
}

/// Internal: a punctuator erased to the driver's untyped surface, mirroring
/// `ErasedNode`. Rebuilds the typed `ProcessorContext` from the `Dispatch`.
#[async_trait]
pub(crate) trait ErasedPunctuator: Send {
    async fn fire(&mut self, dispatch: &mut Dispatch<'_>, timestamp: i64);
}
pub(crate) struct TypedPunctuator<K, V, P> { inner: P, _pd: PhantomData<(K, V)> }
#[async_trait]
impl<K, V, P> ErasedPunctuator for TypedPunctuator<K, V, P>
where K: Any + Send + Clone, V: Any + Send + Clone, P: Punctuator<K, V> {
    async fn fire(&mut self, dispatch: &mut Dispatch<'_>, timestamp: i64) {
        let mut ctx = ProcessorContext::<'_, '_, K, V>::new(dispatch);
        self.inner.punctuate(&mut ctx, timestamp).await;
    }
}
```

The user's `Punctuator` shares mutable state with its owning `Processor` via `Arc<Mutex<_>>` (e.g. the processor buffers in `process`, the punctuator drains the buffer + `forward`s in `punctuate`). Precedent: `dsl/processors/stream_join.rs` already shares an `Arc<Mutex<TimeTracker>>` between processors.

### 3.2 Schedule storage + `schedule()`

`Dispatch` gains four fields:
```rust
pub(crate) struct Dispatch<'a> {
    // … existing: buffer, children, output, record_ctx, stores, globals …
    pub node_idx: usize,                         // the node this dispatch is positioned at
    pub schedules: &'a mut Vec<ScheduleEntry>,   // sink for ProcessorContext::schedule
    pub sched_stream_time: i64,                  // current stream-time (base for a StreamTime schedule)
    pub sched_wall_clock: i64,                   // current wall clock (base for a WallClockTime schedule)
}

pub(crate) struct ScheduleEntry {
    pub node_idx: usize,
    pub interval_ms: i64,
    pub ty: PunctuationType,
    pub next_time: i64,                          // stamped at schedule = base + interval (§3.4)
    pub punctuator: Box<dyn ErasedPunctuator>,
    pub cancel: Arc<AtomicBool>,
}
```

`ProcessorContext::schedule` builds a `TypedPunctuator`, a fresh `cancel` flag, computes the first-fire time `next_time = base + interval_ms` where `base = if ty == StreamTime { dispatch.sched_stream_time } else { dispatch.sched_wall_clock }`, pushes a `ScheduleEntry { node_idx: dispatch.node_idx, next_time, .. }`, and returns `Cancellable(cancel)`. Interval is `Duration` → `interval_ms` (must be ≥ 1ms; panic on 0, matching the JVM's `IllegalArgumentException`). The `Graph` sets `sched_stream_time`/`sched_wall_clock` on every `Dispatch` it builds (= its current `stream_time`/`wall_clock`); at `init` (before any record) `stream_time = i64::MIN` and `wall_clock` = the task's start clock (`0` in the `TopologyTestDriver`), so a stream-time schedule first-fires on the first record and a wall-clock schedule first-fires one interval after the start clock — **matching the captured JVM behavior**.

The `Graph` owns `schedules: Vec<ScheduleEntry>`. Every driver entry point that builds a `Dispatch` (`pipe`, `init_processors`, and the new punctuate paths) now passes `node_idx` and `&mut self.schedules`. After a node's `init`/`process`, newly-pushed entries are already in the `Graph` vec (the `Dispatch` borrowed it mutably).

### 3.3 `Cancellable`

```rust
pub struct Cancellable(Arc<AtomicBool>);
impl Cancellable { pub fn cancel(&self) { self.0.store(true, Ordering::SeqCst); } }
```
The driver, before firing an entry, checks `cancel.load(..)`; cancelled entries are removed from `schedules` (retain).

### 3.4 Firing (reuses the `pipe` drain loop)

Factor the `pipe` body into a private `Graph::drain(buffer)` that runs the existing non-recursive forward-buffer loop. Two new methods:

```rust
pub async fn punctuate_stream_time(&mut self, stream_time: i64) -> Result<(), ProcessorError>;
pub async fn punctuate_wall_clock(&mut self, now_ms: i64) -> Result<(), ProcessorError>;
```
Each iterates `self.schedules` for the matching `PunctuationType`, dropping cancelled entries, and fires each **due** entry **at most once** (the captured JVM `TopologyTestDriver` resyncs `next` ahead rather than firing every missed boundary):
```text
if now >= entry.next_time {
    fire(entry.node_idx, now);   // value passed = the CURRENT time (§3.5), for BOTH types
    entry.next_time = if now - entry.next_time >= interval_ms {
        now + interval_ms        // ≥ one interval behind → resync to now + interval (skip missed boundaries)
    } else {
        entry.next_time + interval_ms   // < one interval behind → advance one interval
    };
}
```
Both branches yield `next_time > now`, so a single `punctuate(now)` call fires an entry at most once. `fire(node_idx, ts)`: build a `Dispatch` positioned at `node_idx` with that node's real `children`, a punctuation `RecordContext { topic: "", partition: -1, offset: -1, timestamp: ts }`, a fresh forward buffer, `&mut self.schedules` (so a punctuator may schedule more), then call the entry's `ErasedPunctuator::fire`, then `drain(buffer)` so forwarded records flow to children. (The `ScheduleEntry` owns the `Box<dyn ErasedPunctuator>`; `mem::replace` it out with a no-op placeholder so the `Dispatch` can borrow `&mut self.schedules`, then restore it — see the plan.)

`Graph` tracks `stream_time: i64` (init `i64::MIN`), bumped to `max(stream_time, record.ts)` inside `pipe`, and `wall_clock: i64` (init `0`), set to `now_ms` at the top of `punctuate_wall_clock`. The TTD/thread call `punctuate_stream_time(self.stream_time)` / `punctuate_wall_clock(now_ms)`.

### 3.5 Per-type timestamp + first-fire — PINNED by the §5 capture

Confirmed against the JVM `TopologyTestDriver` (`testdata/punctuation/behavior.json`):
- **Both types pass the CURRENT time** to `punctuate` — stream-time passes the current `stream_time`, wall-clock passes the current `now_ms`. (NOT the scheduled boundary.)
- **Fire at most once per driving action**, resyncing `next` ahead (no per-missed-boundary catch-up): e.g. interval 10, a stream-time jump 11→100 fires **once** at 100 (not at 20,30,…,100).
- **First-fire** = `base + interval` where `base` = `stream_time` (`i64::MIN` at init → stream-time first-fires on the first record) or `wall_clock` (`0` at init in the TTD → wall-clock first-fires at `interval`).
- Captured sequence (interval 10): stream pipes at ts {0,5,9,10,11,100} fire at **{0, 10, 100}**; wall advances {+3,+3,+4,+100} (clock → 3,6,10,110) fire at **{10, 110}**.

### 3.6 Test driver + thread wiring

- `TopologyTestDriver`: keep a per-subtopology `stream_time` (already implicit via the `Graph`); after each `pipe_input`, call `graph.punctuate_stream_time(graph.stream_time)`. Add `pub fn advance_wall_clock_time(&mut self, by: Duration)`: `self.mock_wall_ms += by.as_millis(); graph.punctuate_wall_clock(self.mock_wall_ms)`. The mock clock starts at 0.
- `StreamThread`: hold a `clock: Arc<dyn Clock>` (trait `Clock { fn now_ms(&self) -> i64; }`, default `SystemClock` using `SystemTime::now()`); after processing a poll batch call `task.punctuate_stream_time(task.stream_time)`, and once per loop iteration `task.punctuate_wall_clock(self.clock.now_ms())`. `StreamTask` forwards to its `Graph`. DI keeps it unit-testable with a `ManualClock`.

## 4. Components & boundaries

| Unit | Responsibility | Depends on |
|---|---|---|
| `processor/punctuation.rs` (new) | `PunctuationType`, `Punctuator`, `Cancellable`, `ErasedPunctuator`/`TypedPunctuator`, `ScheduleEntry` | `api`, `erased` |
| `processor/erased.rs` | `Dispatch` + `node_idx` + `schedules` fields | — |
| `processor/api.rs` | `ProcessorContext::schedule` | `punctuation` |
| `processor/graph.rs` | `schedules`/`stream_time` state, `drain`, `punctuate_stream_time`/`_wall_clock`, positioned `fire` | `punctuation`, `erased` |
| `test_driver.rs` | stream-time auto-fire, `advance_wall_clock_time`, mock clock | `graph` |
| `runtime/task.rs` | `punctuate_stream_time`/`_wall_clock` pass-through; `stream_time` | `graph` |
| `runtime/thread.rs` | `Clock` DI; per-loop wall-clock + per-batch stream-time tick | `task` |
| `tests/jvm-capture/.../PunctuationBehavior.java` (new) | JVM TTD harness emitting fired-timestamp traces | — |

## 5. Capture-first behavior pin

A standalone Java `TopologyTestDriver` harness (NOT a wire golden — punctuation isn't in the wire): a topology with a processor scheduling one `STREAM_TIME` and one `WALL_CLOCK_TIME` punctuator (each `interval=10`), forwarding `(ts)` on each fire. The harness pipes records at chosen timestamps / calls `advanceWallClockTime`, and writes the **sequence of fired timestamps** to a fixture. The Rust execution tests assert the same sequence, pinning: first-fire offset, catch-up count, and which timestamp each type passes. Run via the existing `tests/jvm-capture` Docker harness (new `--punctuation` mode or an added capture method).

## 6. Testing

- **Unit:** `schedule` records an entry tagged with the node index; `Cancellable::cancel` flips the flag; `interval=0` panics; `TypedPunctuator::fire` rebuilds the typed context and forwards.
- **Execution (stream-time, TTD):** a processor buffers values in `process` and a stream-time punctuator emits a rollup every `interval` of stream-time; piping records at ascending timestamps yields the JVM-matched fire sequence; catch-up (a big timestamp jump fires multiple times); `cancel()` stops further fires; a stream-time punctuator can read/write a connected store.
- **Execution (wall-clock, TTD):** `advance_wall_clock_time` fires wall-clock punctuators with the mock-clock timestamp; catch-up across a large advance; mixed stream + wall-clock schedules fire independently.
- **Runtime (thread):** with a `ManualClock`, the `StreamThread` loop fires wall-clock punctuation; the broker-free `StreamTask` test drives stream-time punctuation after a batch.
- **Behavior pin:** the fired-timestamp sequence matches the captured JVM fixture.

## 7. Error handling

- `interval` of 0 → panic (`schedule interval must be positive`), matching the JVM.
- A punctuator that panics propagates as a `ProcessorError` out of the driver (same as a processor panic), not silently swallowed.
- Cancelled schedules are removed on the next punctuate pass; a `Cancellable` cancelled before first fire never fires.

## 8. Slice decomposition (phases, one PR)

- **P-i — stream-time.** `punctuation.rs` (types + erasure) + `Dispatch` fields + `ProcessorContext::schedule` + `Graph` `schedules`/`stream_time`/`drain`/`punctuate_stream_time`/positioned `fire` + TTD stream-time auto-fire + the Java capture harness + stream-time execution & unit tests.
- **P-ii — wall-clock.** `Graph::punctuate_wall_clock` + TTD `advance_wall_clock_time` + mock clock + `StreamThread` `Clock` DI + per-loop wall-clock tick + wall-clock execution & thread tests.

## 9. Open question resolved capture-first

The exact JVM `TopologyTestDriver` firing schedule (first-fire offset, catch-up count, and the timestamp each `PunctuationType` passes to `punctuate`) is pinned by the §5 capture before the Rust firing logic is finalized.
