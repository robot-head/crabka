# Punctuation (`ProcessorContext::schedule` + stream-time & wall-clock timers) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a custom Processor-API node register stream-time and wall-clock punctuators via `ProcessorContext::schedule`, firing periodic callbacks that can `forward(...)` downstream and use state stores.

**Architecture:** Pure runtime feature on the existing erased graph driver — no wire/topology change, no goldens. A punctuator is a trait object (erased like `Processor`); schedules live in the `Graph` tagged by node index; firing reuses the `pipe` forward-buffer drain loop positioned at the scheduling node. The `TopologyTestDriver` fires stream-time after each `pipe_input` and adds `advance_wall_clock_time`; the `StreamThread` drives wall-clock between polls via an injected `Clock`. Exact firing semantics are pinned capture-first against the JVM `TopologyTestDriver`.

**Tech Stack:** Rust, `async_trait`, `pollster` (test driver), the `crabka-client-streams` crate; JVM Kafka Streams 4.1 `TopologyTestDriver` (Docker) for the behavior capture.

**Branch:** `streams-punctuation` (off `origin/main`). All work in the worktree `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`. Commit with `-c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com"`; never `git config`; never edit the main repo.

**Spec:** `docs/superpowers/specs/2026-06-06-kip-1071-streams-client-punctuation-design.md`

---

## File structure

- **Create** `crates/client-streams/src/processor/punctuation.rs` — `PunctuationType`, `Punctuator`, `Cancellable`, `ErasedPunctuator`, `TypedPunctuator`, `ScheduleEntry`.
- **Modify** `crates/client-streams/src/processor/mod.rs` — `pub mod punctuation;`.
- **Modify** `crates/client-streams/src/processor/erased.rs` — add `node_idx` + `schedules` to `Dispatch`.
- **Modify** `crates/client-streams/src/processor/api.rs` — `ProcessorContext::schedule`.
- **Modify** `crates/client-streams/src/processor/graph.rs` — `schedules`/`stream_time` state, factor `drain`, `punctuate_stream_time`/`punctuate_wall_clock`, positioned `fire`; pass new `Dispatch` fields everywhere.
- **Modify** `crates/client-streams/src/test_driver.rs` — stream-time auto-fire, `advance_wall_clock_time`, mock clock.
- **Modify** `crates/client-streams/src/runtime/task.rs` — `punctuate_stream_time`/`punctuate_wall_clock` pass-through; stream-time tick in `process_once`.
- **Modify** `crates/client-streams/src/runtime/thread.rs` — `Clock` DI, per-loop wall-clock + per-batch stream-time tick.
- **Modify** `crates/client-streams/src/lib.rs` — re-export the public types + docs.
- **Create** `crates/client-streams/tests/jvm-capture/src/main/java/crabka/capture/PunctuationBehavior.java` + a `run.sh` `--punctuation` mode + fixture `crates/client-streams/tests/testdata/punctuation/behavior.json`.
- **Modify** `crates/client-streams/tests/dsl_execution.rs` (or a new `tests/punctuation.rs`) — execution tests.

---

## Task 1: JVM `TopologyTestDriver` behavior capture (CONTROLLER, capture-first)

**Goal:** Pin the exact JVM firing schedule (first-fire offset, catch-up count, the timestamp each `PunctuationType` passes) before writing the Rust firing logic. This task is run by the controller (Docker), mirroring the existing `BufferValueCapture` standalone-Java path.

**Files:**
- Create: `crates/client-streams/tests/jvm-capture/src/main/java/crabka/capture/PunctuationBehavior.java`
- Modify: `crates/client-streams/tests/jvm-capture/run.sh` (add `--punctuation` mode)
- Create (output): `crates/client-streams/tests/testdata/punctuation/behavior.json`

- [ ] **Step 1: Write the Java harness.** A `TopologyTestDriver` program: a topology `source("in") -> proc -> sink("out")` where `proc` schedules, in `init`, one `STREAM_TIME` punctuator and one `WALL_CLOCK_TIME` punctuator, each `interval = Duration.ofMillis(10)`. Each punctuator, on fire, appends `"<type>:<timestamp>"` to a shared list. The harness then runs a fixed script and writes the fired sequence to JSON.

```java
package crabka.capture;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.*;
import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;

public final class PunctuationBehavior {
    public static void main(String[] args) throws Exception {
        Path out = Paths.get(args.length > 0 ? args[0] : "out");
        Files.createDirectories(out);
        List<String> fired = new ArrayList<>();

        Topology topo = new Topology();
        topo.addSource("src", "in");
        topo.addProcessor("proc", () -> new Processor<String, String, String, String>() {
            private ProcessorContext<String, String> ctx;
            @Override public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
                context.schedule(Duration.ofMillis(10), PunctuationType.STREAM_TIME,
                    ts -> fired.add("stream:" + ts));
                context.schedule(Duration.ofMillis(10), PunctuationType.WALL_CLOCK_TIME,
                    ts -> fired.add("wall:" + ts));
            }
            @Override public void process(Record<String, String> r) { ctx.forward(r); }
        }, "src");
        topo.addSink("snk", "out", "proc");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "punct");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Mock wall clock starts at 0 (TopologyTestDriver default initial wall-clock time).
        try (TopologyTestDriver driver = new TopologyTestDriver(topo, props, java.time.Instant.ofEpochMilli(0))) {
            TestInputTopic<String, String> in =
                driver.createInputTopic("in", Serdes.String().serializer(), Serdes.String().serializer());
            // Stream-time script: pipe records at ascending event timestamps, with a jump for catch-up.
            in.pipeInput("k", "a", java.time.Instant.ofEpochMilli(5));   // stream-time 5
            in.pipeInput("k", "b", java.time.Instant.ofEpochMilli(12));  // crosses 10 boundary
            in.pipeInput("k", "c", java.time.Instant.ofEpochMilli(45));  // big jump → catch-up
            fired.add("---");
            // Wall-clock script: advance the mock clock past several boundaries.
            driver.advanceWallClockTime(Duration.ofMillis(10));  // 10
            driver.advanceWallClockTime(Duration.ofMillis(25));  // 35 → catch-up
        }

        StringBuilder sb = new StringBuilder("[\n");
        for (int i = 0; i < fired.size(); i++)
            sb.append("  \"").append(fired.get(i)).append("\"").append(i + 1 < fired.size() ? ",\n" : "\n");
        sb.append("]\n");
        Files.writeString(out.resolve("behavior.json"), sb.toString());
        System.out.println("punctuation behavior:\n" + sb);
    }
}
```

- [ ] **Step 2: Add the `--punctuation` run mode** to `run.sh`, mirroring the `BufferValueCapture` block (download the streams + test-utils jars, javac, run). The test-utils jar is `kafka-streams-test-utils-4.1.0.jar` plus its transitive deps already present (`kafka-streams`, `kafka-clients`, slf4j). Mount `tests/` so the fixture persists; write to `/tests/testdata/punctuation`.

```bash
  --punctuation)
    docker run --rm -v "$HERE":/work -v "$HERE/../testdata":/tests/testdata -w /work eclipse-temurin:21-jdk bash -c '
      set -e
      MVN=https://repo1.maven.org/maven2
      mkdir -p /tmp/jars
      for art in \
        "org/apache/kafka/kafka-streams/4.1.0/kafka-streams-4.1.0.jar" \
        "org/apache/kafka/kafka-streams-test-utils/4.1.0/kafka-streams-test-utils-4.1.0.jar" \
        "org/apache/kafka/kafka-clients/4.1.0/kafka-clients-4.1.0.jar" \
        "org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar" \
        "org/rocksdb/rocksdbjni/9.7.3/rocksdbjni-9.7.3.jar" ; do
        curl -sSL "$MVN/$art" -o "/tmp/jars/$(basename $art)"
      done
      CP=$(echo /tmp/jars/*.jar | tr " " ":")
      javac -cp "$CP" -d /tmp/build src/main/java/crabka/capture/PunctuationBehavior.java
      java -cp "/tmp/build:$CP" crabka.capture.PunctuationBehavior /tests/testdata/punctuation
    '
    ;;
```

- [ ] **Step 3: Run the capture (controller).** `cd crates/client-streams/tests/jvm-capture && ./run.sh --punctuation`. Inspect `testdata/punctuation/behavior.json`. Record the EXACT sequence — it is ground truth for Tasks 5 & 6. Expected shape (CONFIRM against the actual output; do not assume): stream fires at the scheduled boundaries it crosses given event-time, then `"---"`, then wall fires at the mock-clock boundaries crossed. If Docker is unavailable at execution time, leave the harness committed and proceed using the documented model (first-fire = `schedule_time + interval`; catch-up `while now >= next { fire(next_for_stream | now_for_wall); next += interval }`); note the divergence risk in the task report.

- [ ] **Step 4: Commit.**
```bash
git add crates/client-streams/tests/jvm-capture/src/main/java/crabka/capture/PunctuationBehavior.java \
        crates/client-streams/tests/jvm-capture/run.sh \
        crates/client-streams/tests/testdata/punctuation/behavior.json
git commit -m "test(streams): capture JVM TopologyTestDriver punctuation behavior (capture-first)"
```

---

## Task 2: `processor/punctuation.rs` — types + erasure

**Files:**
- Create: `crates/client-streams/src/processor/punctuation.rs`
- Modify: `crates/client-streams/src/processor/mod.rs`, `crates/client-streams/src/lib.rs`

- [ ] **Step 1: Write the module.** Read `crates/client-streams/src/processor/node.rs` for the `ErasedNode`/`Typed... → ProcessorContext::new` erasure pattern to mirror. `Dispatch`/`ScheduleEntry` cross-reference each other, so `ScheduleEntry` lives here and `erased.rs` imports it (Task 3).

```rust
//! Punctuation (`ProcessorContext::schedule`, KIP Processor API): periodic
//! callbacks fired on stream-time or wall-clock boundaries. A `Punctuator` is a
//! trait object erased to the driver exactly like a `Processor` (`TypedPunctuator`
//! rebuilds the typed `ProcessorContext` from the `Dispatch`). Schedules live in
//! the `Graph`, tagged by node index; the driver fires them positioned at that
//! node so a punctuator's `forward` flows downstream. Punctuation is invisible in
//! the wire topology — pure runtime.
use std::any::Any;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

use crate::processor::api::ProcessorContext;
use crate::processor::erased::Dispatch;

/// Which clock drives a punctuation schedule (JVM `PunctuationType`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PunctuationType {
    /// Driven by the task's observed max record timestamp.
    StreamTime,
    /// Driven by the system (or mock) wall clock.
    WallClockTime,
}

/// A periodic callback. Implemented on a user struct (like [`Processor`]); shares
/// mutable state with its owning processor via `Arc<Mutex<_>>`.
///
/// [`Processor`]: crate::processor::api::Processor
#[async_trait]
pub trait Punctuator<K: Send, V: Send>: Send + 'static {
    /// Fire at `timestamp` (stream-time: the scheduled time; wall-clock: the
    /// clock's current time). May `forward` via `ctx` and use state stores.
    async fn punctuate(&mut self, ctx: &mut ProcessorContext<'_, '_, K, V>, timestamp: i64);
}

/// Handle returned by [`ProcessorContext::schedule`](crate::processor::api::ProcessorContext::schedule).
/// `cancel()` stops the schedule; the driver drops it on the next punctuate pass.
#[derive(Clone)]
pub struct Cancellable(Arc<AtomicBool>);
impl Cancellable {
    pub(crate) fn new(flag: Arc<AtomicBool>) -> Self {
        Self(flag)
    }
    /// Stop this schedule from firing again.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Internal: a punctuator erased to the driver's untyped surface (mirrors
/// `ErasedNode`).
#[async_trait]
pub(crate) trait ErasedPunctuator: Send {
    async fn fire(&mut self, dispatch: &mut Dispatch<'_>, timestamp: i64);
}

/// Wraps a typed [`Punctuator`] into an [`ErasedPunctuator`] by rebuilding the
/// typed [`ProcessorContext`] from the `Dispatch`.
pub(crate) struct TypedPunctuator<K, V, P> {
    inner: P,
    _pd: PhantomData<fn(K, V)>,
}
impl<K, V, P> TypedPunctuator<K, V, P> {
    pub(crate) fn new(inner: P) -> Self {
        Self {
            inner,
            _pd: PhantomData,
        }
    }
}
#[async_trait]
impl<K, V, P> ErasedPunctuator for TypedPunctuator<K, V, P>
where
    K: Any + Send + Clone,
    V: Any + Send + Clone,
    P: Punctuator<K, V>,
{
    async fn fire(&mut self, dispatch: &mut Dispatch<'_>, timestamp: i64) {
        let mut ctx = ProcessorContext::<'_, '_, K, V>::new(dispatch);
        self.inner.punctuate(&mut ctx, timestamp).await;
    }
}

/// One live punctuation schedule, owned by the `Graph`.
pub(crate) struct ScheduleEntry {
    pub node_idx: usize,
    pub interval_ms: i64,
    pub ty: PunctuationType,
    /// The next time to fire; `None` until first evaluated (first fire = the
    /// evaluating clock value + `interval_ms`).
    pub next_time: Option<i64>,
    pub punctuator: Box<dyn ErasedPunctuator>,
    pub cancel: Arc<AtomicBool>,
}
impl ScheduleEntry {
    pub fn is_cancelled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst)
    }
}
```

- [ ] **Step 2: Register + re-export.** In `processor/mod.rs` add `pub mod punctuation;` (match the existing `pub mod api;` style). In `lib.rs` re-export `PunctuationType`, `Punctuator`, `Cancellable` next to the existing `Processor`/`ProcessorContext` re-exports. Do NOT re-export `ErasedPunctuator`/`TypedPunctuator`/`ScheduleEntry` (crate-internal).

- [ ] **Step 3: Unit tests** (in `punctuation.rs`):
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::check;

    #[test]
    fn cancellable_flips_the_flag() {
        let flag = Arc::new(AtomicBool::new(false));
        let c = Cancellable::new(flag.clone());
        check!(!flag.load(Ordering::SeqCst));
        c.cancel();
        check!(flag.load(Ordering::SeqCst));
    }

    #[test]
    fn schedule_entry_reports_cancelled() {
        let flag = Arc::new(AtomicBool::new(false));
        struct NoOp;
        #[async_trait]
        impl ErasedPunctuator for NoOp {
            async fn fire(&mut self, _d: &mut Dispatch<'_>, _ts: i64) {}
        }
        let e = ScheduleEntry {
            node_idx: 0,
            interval_ms: 10,
            ty: PunctuationType::StreamTime,
            next_time: None,
            punctuator: Box::new(NoOp),
            cancel: flag.clone(),
        };
        check!(!e.is_cancelled());
        flag.store(true, Ordering::SeqCst);
        check!(e.is_cancelled());
    }
}
```
NOTE: `TypedPunctuator`/`ScheduleEntry`/`ErasedPunctuator` are unused by production until Tasks 3-4 — add `#[allow(dead_code)]` to each with a `// consumed by schedule/fire in T3/T4` comment; remove the allows in the task that first uses each.

- [ ] **Step 4: Verify + commit.** `cargo test -p crabka-client-streams --lib punctuation` (2 pass); `cargo clippy -p crabka-client-streams --all-targets -- -D warnings`; `cargo fmt -p crabka-client-streams`. Commit `feat(streams): punctuation types + erasure (PunctuationType/Punctuator/Cancellable/ScheduleEntry)`.

---

## Task 3: `Dispatch` gains `node_idx` + `schedules`; thread through all sites

**Files:**
- Modify: `crates/client-streams/src/processor/erased.rs`, `crates/client-streams/src/processor/graph.rs`, and every other `Dispatch { .. }` construction site.

- [ ] **Step 1: Add the fields** to `Dispatch` in `erased.rs`:
```rust
pub(crate) struct Dispatch<'a> {
    pub buffer: &'a mut VecDeque<(usize, ErasedRecord)>,
    pub children: &'a [usize],
    pub output: &'a mut Vec<OutputRecord>,
    pub record_ctx: &'a RecordContext,
    pub stores: &'a mut crate::store::registry::StoreRegistry,
    pub globals: &'a crate::runtime::global::GlobalStateManager,
    /// The graph node this dispatch is positioned at (so `schedule` tags the
    /// owning node, and a punctuator forwards to this node's children).
    pub node_idx: usize,
    /// Sink for `ProcessorContext::schedule`: newly-registered punctuation
    /// schedules (the `Graph` owns the backing `Vec`).
    pub schedules: &'a mut Vec<crate::processor::punctuation::ScheduleEntry>,
}
```

- [ ] **Step 2: Find every construction site.** Run:
```
grep -rn "Dispatch {" crates/client-streams/src
```
Expected sites (the lib build will pin the exact set): `processor/graph.rs` (`pipe`, `init_processors`), and `#[cfg(test)]` Dispatch builders in `processor/api.rs`, `processor/node.rs`, `processor/erased.rs` if present. Each must add `node_idx` + `schedules`. For test sites that don't exercise scheduling, pass `node_idx: 0` and a `&mut Vec::new()` bound to a local (e.g. `let mut scheds = Vec::new();` before the `Dispatch`).

- [ ] **Step 3: Update `graph.rs` construction.** In `Graph::pipe`'s drain loop, set `node_idx: idx` and `schedules: &mut self.schedules` (added in Task 4 — for THIS task, add the field to `Graph` first; see Task 4 Step 1, or temporarily thread a local `&mut Vec::new()` and switch to `self.schedules` in Task 4). To keep Task 3 self-contained and compiling, add the `Graph.schedules` field now:
```rust
// in struct Graph:
pub schedules: Vec<crate::processor::punctuation::ScheduleEntry>,
pub stream_time: i64,
// in every Graph constructor / instantiate: schedules: Vec::new(), stream_time: i64::MIN,
```
Find Graph construction with `grep -rn "Graph {" crates/client-streams/src` and add the two fields (init `Vec::new()` / `i64::MIN`). In `pipe`, after seeding/draining, bump stream-time: at the top of `pipe`, `self.stream_time = self.stream_time.max(timestamp);`. In the drain-loop `Dispatch`, the borrow of `self.schedules` conflicts with `self.nodes[idx]`/`self.output`/`self.stores` — they are DISTINCT fields, so bind each as a separate local (the existing code already binds `node`/`out`/`stores` as disjoint locals; add `let scheds = &mut self.schedules;` alongside and pass `schedules: scheds, node_idx: idx`).

- [ ] **Step 4: Update `init_processors`** similarly: pass `node_idx: idx`, `schedules: &mut self.schedules`. (Its `children: &[]` stays — init can't forward, but it CAN schedule.) Same disjoint-field binding.

- [ ] **Step 5: Verify + commit.** `cargo build -p crabka-client-streams --tests` (compiles — all Dispatch sites updated); `cargo test -p crabka-client-streams` (existing tests still green); clippy + fmt. Commit `feat(streams): Dispatch carries node_idx + schedules sink; Graph owns schedules/stream_time`.

---

## Task 4: `ProcessorContext::schedule` + Graph firing (`drain`, `punctuate_stream_time`, positioned `fire`)

**Files:**
- Modify: `crates/client-streams/src/processor/api.rs`, `crates/client-streams/src/processor/graph.rs`

- [ ] **Step 1: `ProcessorContext::schedule`** in `api.rs` (on the same `impl<'ctx,'d,KOut,VOut> ProcessorContext` block as `forward`, which already bounds `KOut: Any+Send+Clone, VOut: Any+Send+Clone`):
```rust
/// Schedule a periodic [`Punctuator`]. Callable from `init` or `process`.
/// `interval` must be positive. Returns a [`Cancellable`] to stop it.
///
/// [`Punctuator`]: crate::processor::punctuation::Punctuator
pub fn schedule<P>(
    &mut self,
    interval: std::time::Duration,
    ty: crate::processor::punctuation::PunctuationType,
    punctuator: P,
) -> crate::processor::punctuation::Cancellable
where
    P: crate::processor::punctuation::Punctuator<KOut, VOut>,
{
    let interval_ms = i64::try_from(interval.as_millis()).unwrap_or(i64::MAX);
    assert!(interval_ms >= 1, "schedule interval must be positive (>= 1ms)");
    let cancel = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let erased: Box<dyn crate::processor::punctuation::ErasedPunctuator> =
        Box::new(crate::processor::punctuation::TypedPunctuator::<KOut, VOut, P>::new(punctuator));
    self.dispatch
        .schedules
        .push(crate::processor::punctuation::ScheduleEntry {
            node_idx: self.dispatch.node_idx,
            interval_ms,
            ty,
            next_time: None,
            punctuator: erased,
            cancel: cancel.clone(),
        });
    crate::processor::punctuation::Cancellable::new(cancel)
}
```
Make `ErasedPunctuator`, `TypedPunctuator::new`, and `ScheduleEntry` fields visible to `api.rs` (they are `pub(crate)` already). Remove the `#[allow(dead_code)]` from `TypedPunctuator`/`ScheduleEntry` now that they're used.

- [ ] **Step 2: Factor `drain`** in `graph.rs`. Extract the `pipe` drain loop (the `while let Some((idx, rec)) = buffer.pop_front()` block) into:
```rust
/// Drain a forward buffer through the graph (the non-recursive driver loop).
async fn drain(
    &mut self,
    mut buffer: std::collections::VecDeque<(usize, ErasedRecord)>,
    rc: &RecordContext,
) -> Result<(), ProcessorError> {
    while let Some((idx, rec)) = buffer.pop_front() {
        let children = std::mem::take(&mut self.children[idx]);
        let res = {
            let node = &mut self.nodes[idx];
            let out = &mut self.output;
            let stores = &mut self.stores;
            let scheds = &mut self.schedules;
            let mut d = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: out,
                record_ctx: rc,
                stores,
                globals: &self.globals,
                node_idx: idx,
                schedules: scheds,
            };
            node.process(&mut d, rec).await
        };
        self.children[idx] = children;
        res?;
    }
    Ok(())
}
```
Rewrite `pipe` to seed the buffer + `rc` exactly as today (bump `self.stream_time = self.stream_time.max(timestamp)` first), then `self.drain(buffer, &rc).await`.

- [ ] **Step 3: Positioned `fire` + `punctuate_stream_time`** in `graph.rs`:
```rust
/// Fire one schedule's punctuator positioned at its node, then drain any
/// records it forwarded. `ts` is the timestamp passed to `punctuate`.
async fn fire_schedule(&mut self, sched_idx: usize, ts: i64) -> Result<(), ProcessorError> {
    let node_idx = self.schedules[sched_idx].node_idx;
    let rc = RecordContext { topic: String::new(), partition: -1, offset: -1, timestamp: ts };
    let mut buffer: std::collections::VecDeque<(usize, ErasedRecord)> = std::collections::VecDeque::new();
    // Take the punctuator out so the Dispatch can borrow `self.schedules` (for
    // re-scheduling) without aliasing the entry we're firing.
    let mut punct = std::mem::replace(
        &mut self.schedules[sched_idx].punctuator,
        Box::new(NoopPunctuator),
    );
    let children = std::mem::take(&mut self.children[node_idx]);
    {
        let out = &mut self.output;
        let stores = &mut self.stores;
        let scheds = &mut self.schedules;
        let mut d = Dispatch {
            buffer: &mut buffer,
            children: &children,
            output: out,
            record_ctx: &rc,
            stores,
            globals: &self.globals,
            node_idx,
            schedules: scheds,
        };
        punct.fire(&mut d, ts).await;
    }
    self.children[node_idx] = children;
    self.schedules[sched_idx].punctuator = punct;
    self.drain(buffer, &rc).await
}

/// Fire all due STREAM_TIME schedules up to `stream_time`, with catch-up.
pub async fn punctuate_stream_time(&mut self, stream_time: i64) -> Result<(), ProcessorError> {
    self.punctuate(crate::processor::punctuation::PunctuationType::StreamTime, stream_time).await
}

async fn punctuate(
    &mut self,
    ty: crate::processor::punctuation::PunctuationType,
    now: i64,
) -> Result<(), ProcessorError> {
    use crate::processor::punctuation::PunctuationType;
    // Drop cancelled schedules first.
    self.schedules.retain(|e| !e.is_cancelled());
    // Snapshot which schedules fire at which timestamps (avoid borrow conflicts
    // while firing, which itself may push new schedules).
    let n = self.schedules.len();
    for i in 0..n.min(self.schedules.len()) {
        if self.schedules[i].ty != ty || self.schedules[i].is_cancelled() {
            continue;
        }
        let interval = self.schedules[i].interval_ms;
        let mut next = self.schedules[i].next_time.unwrap_or(now + interval);
        // Stream-time passes the scheduled `next`; wall-clock passes `now`.
        while now >= next {
            let ts = match ty {
                PunctuationType::StreamTime => next,
                PunctuationType::WallClockTime => now,
            };
            self.fire_schedule(i, ts).await?;
            next += interval;
        }
        self.schedules[i].next_time = Some(next);
    }
    Ok(())
}
```
Add a private `NoopPunctuator` (impls `ErasedPunctuator` with an empty `fire`) in `graph.rs` or export one from `punctuation.rs` for the `mem::replace` placeholder. NOTE: re-confirm the per-type `ts` (StreamTime → `next`, WallClock → `now`) + first-fire (`now + interval`) against the Task 1 `behavior.json`; adjust if the capture differs.

- [ ] **Step 4: Unit test** (`graph.rs` `#[cfg(test)]`): build a tiny graph with a processor that, in `init`, schedules a stream-time punctuator forwarding `Record::new(None, ts, ts)`; pipe a record at ts=5, then `punctuate_stream_time(25)`; assert the output contains the expected fired timestamps (per the model / fixture: 10, 20). Use the existing graph-construction test helpers in `graph.rs`/`node.rs`.

- [ ] **Step 5: Verify + commit.** `cargo test -p crabka-client-streams --lib`; clippy + fmt. Commit `feat(streams): ProcessorContext::schedule + Graph stream-time punctuation firing`.

---

## Task 5: `TopologyTestDriver` stream-time firing + execution tests

**Files:**
- Modify: `crates/client-streams/src/test_driver.rs`
- Create/Modify: `crates/client-streams/tests/punctuation.rs`

- [ ] **Step 1: Auto-fire stream-time after each pipe.** In `test_driver.rs` `pipe_bytes`, after the `while let Some(...) = queue.pop_front()` loop completes (all loopback drained), fire stream-time punctuations and route their outputs the same way. Refactor the output-routing in `pipe_bytes` (the `for out in self.graph.take_output()` block + changelog drain) into a helper `route_outputs(&mut self, queue: &mut VecDeque<PendingRecord>)`, then:
```rust
fn pipe_bytes(&mut self, topic: &str, key: Option<&[u8]>, value: &[u8], timestamp: i64) {
    let mut queue: VecDeque<PendingRecord> = VecDeque::from([(topic.to_string(),
        key.map(<[u8]>::to_vec), value.to_vec(), timestamp)]);
    while let Some((t, k, v, ts)) = queue.pop_front() {
        let _ = pollster::block_on(self.graph.pipe(&t, k.as_deref(), &v, ts));
        self.route_outputs(&mut queue);
        // Stream-time advanced by this record → fire stream-time punctuators; their
        // forwarded records route like any output (and may loop back).
        let _ = pollster::block_on(self.graph.punctuate_stream_time(self.graph.stream_time));
        self.route_outputs(&mut queue);
    }
}
```
(`self.graph.stream_time` is `pub` from Task 3.) Ensure `route_outputs` does the existing source-topic-loopback vs `self.output` routing + `drain_changelogs`.

- [ ] **Step 2: Execution tests** in `crates/client-streams/tests/punctuation.rs`. Use the public API (`Topology`, `Processor`, `Punctuator`, `PunctuationType`, `schedule`, `TopologyTestDriver`). A processor buffers values in an `Arc<Mutex<Vec<..>>>`; its stream-time `Punctuator` (sharing the same `Arc<Mutex<..>>`) drains the buffer and forwards a rollup keyed `None` valued `count` at each fire. Assert the emitted sequence matches the Task 1 `behavior.json` stream section (e.g., piping at ts 5/12/45 fires stream-time punctuations at 10, 20, then catch-up 20→... per the fixture). Add tests:
  - **fires on boundary**: pipe ascending timestamps; assert one fire per crossed boundary.
  - **catch-up on a jump**: a big timestamp gap fires multiple times in one pipe.
  - **cancel stops it**: capture the `Cancellable` (via shared state), `cancel()`, assert no further fires.
  - **store access**: a stream-time punctuator reads/writes a connected `add_state_store` store.

  Mirror the exact fired-timestamp values from `behavior.json` (do NOT invent them — read the committed fixture). Construct the punctuator-state-sharing with `Arc<Mutex<_>>` as `dsl/processors/stream_join.rs` does.

- [ ] **Step 3: Verify + commit.** `cargo test -p crabka-client-streams --test punctuation`; full suite; clippy + fmt. Commit `feat(streams): TopologyTestDriver stream-time punctuation + execution tests`.

---

## Task 6: Wall-clock — `punctuate_wall_clock` + TTD `advance_wall_clock_time`

**Files:**
- Modify: `crates/client-streams/src/processor/graph.rs`, `crates/client-streams/src/test_driver.rs`, `crates/client-streams/tests/punctuation.rs`

- [ ] **Step 1: `Graph::punctuate_wall_clock`** (graph.rs) — one line over the shared `punctuate`:
```rust
/// Fire all due WALL_CLOCK_TIME schedules up to `now_ms`, with catch-up.
pub async fn punctuate_wall_clock(&mut self, now_ms: i64) -> Result<(), ProcessorError> {
    self.punctuate(crate::processor::punctuation::PunctuationType::WallClockTime, now_ms).await
}
```

- [ ] **Step 2: TTD mock clock + `advance_wall_clock_time`.** In `test_driver.rs`, add `mock_wall_ms: i64` to the struct (init `0` in `new`). Add:
```rust
/// Advance the mock wall clock by `by`, firing wall-clock punctuators (with
/// catch-up). Mirrors the JVM `TopologyTestDriver.advanceWallClockTime`.
#[allow(clippy::needless_pass_by_value)]
pub fn advance_wall_clock_time(&mut self, by: std::time::Duration) {
    self.mock_wall_ms += i64::try_from(by.as_millis()).unwrap_or(i64::MAX);
    let mut queue: VecDeque<PendingRecord> = VecDeque::new();
    let _ = pollster::block_on(self.graph.punctuate_wall_clock(self.mock_wall_ms));
    self.route_outputs(&mut queue);
    // Drain any loopback the punctuators produced.
    while let Some((t, k, v, ts)) = queue.pop_front() {
        let _ = pollster::block_on(self.graph.pipe(&t, k.as_deref(), &v, ts));
        self.route_outputs(&mut queue);
        let _ = pollster::block_on(self.graph.punctuate_stream_time(self.graph.stream_time));
        self.route_outputs(&mut queue);
    }
}
```

- [ ] **Step 3: Wall-clock execution tests** (`tests/punctuation.rs`): `advance_wall_clock_time(Duration::from_millis(10))` fires once; a larger advance catches up multiple boundaries (assert against the `behavior.json` wall section); a wall-clock punctuator forwards downstream; a mixed topology with BOTH a stream-time and a wall-clock schedule fires each independently (piping records fires stream-time only; advancing the clock fires wall-clock only). Read the committed `behavior.json` wall values — do not invent them.

- [ ] **Step 4: Verify + commit.** `cargo test -p crabka-client-streams --test punctuation`; full suite; clippy + fmt. Commit `feat(streams): wall-clock punctuation + TopologyTestDriver::advance_wall_clock_time`.

---

## Task 7: `StreamThread` wall-clock + stream-time wiring (`Clock` DI)

**Files:**
- Modify: `crates/client-streams/src/runtime/task.rs`, `crates/client-streams/src/runtime/thread.rs`

- [ ] **Step 1: Task pass-throughs.** In `task.rs`, add to `StreamTask`:
```rust
pub async fn punctuate_stream_time(&mut self) -> Result<(), StreamsClientError> {
    self.graph.punctuate_stream_time(self.graph.stream_time).await.map_err(Into::into)
}
pub async fn punctuate_wall_clock(&mut self, now_ms: i64) -> Result<(), StreamsClientError> {
    self.graph.punctuate_wall_clock(now_ms).await.map_err(Into::into)
}
```
Map `ProcessorError` to `StreamsClientError` the same way `process_once` already does. In `process_once`, after the per-record `pipe` loop (before/after `take_output` — match the existing structure), call `self.graph.punctuate_stream_time(self.graph.stream_time).await?` so stream-time punctuators fire after a processed batch, and route their outputs through the SAME `take_output` → producer path the records use (read `process_once` to place it so forwarded punctuation records are produced/looped identically).

- [ ] **Step 2: `Clock` trait + impls** (in `thread.rs` or a small `runtime/clock.rs`):
```rust
pub(crate) trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
}
pub(crate) struct SystemClock;
impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0))
            .unwrap_or(i64::MAX)
    }
}
#[cfg(test)]
pub(crate) struct ManualClock(pub std::sync::Arc<std::sync::atomic::AtomicI64>);
#[cfg(test)]
impl Clock for ManualClock {
    fn now_ms(&self) -> i64 { self.0.load(std::sync::atomic::Ordering::SeqCst) }
}
```

- [ ] **Step 3: Thread DI + tick.** Add `clock: std::sync::Arc<dyn Clock>` to `StreamThread`; default `Arc::new(SystemClock)` in `new` (add a `#[cfg(test)]` constructor or a `with_clock` to inject `ManualClock`). In `poll_all`, after `task.process_once(...)`, call `task.punctuate_wall_clock(self.clock.now_ms()).await?` for each task (wall-clock tick once per poll pass). (Stream-time already fires inside `process_once` from Step 1.)

- [ ] **Step 4: Tests** (`thread.rs`/`task.rs` `#[cfg(test)]`): with a `ManualClock`, after `apply_assignment` + `poll_all`, a wall-clock punctuator fired (advance the atomic, poll again, assert its effect via the mock producer/sink). A broker-free `StreamTask` test: pipe a batch, assert a stream-time punctuator fired. Reuse the existing `MockFetcher`/producer harness in `thread.rs` tests.

- [ ] **Step 5: Verify + commit.** `cargo test -p crabka-client-streams`; clippy + fmt. Commit `feat(streams): StreamThread drives wall-clock + stream-time punctuation (Clock DI)`.

---

## Task 8: Docs + final verification

**Files:**
- Modify: `crates/client-streams/src/lib.rs`

- [ ] **Step 1: Docs.** Add a `## Punctuation (timers)` section to the `lib.rs` crate docs (mirror the existing DSL prose style): `ProcessorContext::schedule(interval, PunctuationType, Punctuator)`; STREAM_TIME (observed max record ts, scheduled-time stamp) vs WALL_CLOCK_TIME (system/mock clock, now stamp); first-fire + catch-up; `Cancellable`; punctuators forward downstream + use stores; `TopologyTestDriver::advance_wall_clock_time` for tests; state-sharing with the processor via `Arc<Mutex<_>>`. Include one short runnable doctest: a processor scheduling a stream-time punctuator that forwards a count, driven by `TopologyTestDriver` (use the fixture's fire timestamps).

- [ ] **Step 2: Final verify.** `cargo test -p crabka-client-streams` + `--doc`; `cargo clippy -p crabka-client-streams --all-targets -- -D warnings`; `cargo fmt -p crabka-client-streams --check`. All green. Commit `test(streams): punctuation docs + final verification`.

---

## Done criteria
- `ProcessorContext::schedule` registers stream-time and wall-clock punctuators that `forward` downstream and use stores; `Cancellable::cancel` stops them.
- `Graph` fires both types with first-fire + catch-up positioned at the scheduling node; `TopologyTestDriver` auto-fires stream-time + has `advance_wall_clock_time`; `StreamThread` drives both via an injected `Clock`.
- The fired-timestamp sequences match the captured JVM `TopologyTestDriver` `behavior.json`.
- No wire/topology change (no goldens); full suite + doctests + clippy `--all-targets -D warnings` + fmt green.

## Notes for the implementer
- **Capture-first (Task 1):** read the committed `behavior.json` before asserting fire timestamps in Tasks 5/6. The per-type timestamp + first-fire model in Task 4 is the expected default — reconcile it with the fixture and adjust the `punctuate` loop if they differ.
- **Punctuator state-sharing:** a `Punctuator` is a separate object from its `Processor`; share mutable state via `Arc<Mutex<_>>` (precedent: `dsl/processors/stream_join.rs`).
- **Borrow split in `fire_schedule`:** `mem::replace` the punctuator out (Noop placeholder) so the `Dispatch` can borrow `&mut self.schedules` for re-scheduling; restore it after; restore `self.children[node_idx]` before `drain`.
- **Scheduling in `init` vs `process`:** both work (the `Dispatch` carries `schedules` in both paths). `init`'s `children: &[]` means a punctuator scheduled in init still forwards correctly — `fire_schedule` uses the node's REAL `self.children[node_idx]`, not init's empty slice.
- **No DSL surface, no wire bytes** — purely Processor-API + runtime. Don't add a `KStream`/`KTable` operator.
