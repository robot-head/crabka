# Suppress Slice A — core buffer + `untilWindowCloses` + `unbounded` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `KTable.suppress(Suppressed.untilWindowCloses(unbounded()))` — final-results emission for windowed/session tables — byte-exact against JVM Kafka-Streams 4.1.

**Architecture:** An in-memory time-ordered buffer (`TimeOrderedKeyValueBuffer`) holds the per-window `Change` updates; the `KTableSuppressProcessor` advances a stream-time clock from record timestamps and forwards each window's final `Change` once `stream_time ≥ window.end + grace`. `suppress` is a specialized method on `KTable<Windowed<KInner>, V>`; the upstream window's grace is threaded into the KTable handle.

**Tech Stack:** Rust, `async-trait`; reuses 4c `Change<V>`, 4d `Windowed<K>`/`Window`. No serdes (the buffer is in-memory; the changelog is Slice D).

**Branch / worktree:** `streams-suppress-a` (stacked on `streams-4d-iv-session-windows` / PR #402) in `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`. Spec: `docs/superpowers/specs/2026-06-06-kip-1071-streams-client-suppress-a-design.md`.

**Git discipline:** all git via `git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl …`; assert branch `== streams-suppress-a` before each commit; commit `-c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com"`; never `git config`; no push.

---

## File Structure

**New files:**
- `crates/client-streams/src/dsl/processors/suppress_buffer.rs` — `TimeOrderedKeyValueBuffer<K,V>`.
- `crates/client-streams/src/dsl/processors/suppress.rs` — `KTableSuppressProcessor<KInner,V>`.
- `crates/client-streams/src/dsl/suppress.rs` — `Suppressed` + `BufferConfig`.
- `crates/client-streams/tests/testdata/golden/dsl/suppress_until_window_closes.topology.json` — JVM capture (Phase C).

**Modified files:**
- `src/dsl/processors/mod.rs` — register `suppress_buffer` + `suppress`.
- `src/dsl/mod.rs` + `src/lib.rs` — `pub mod suppress;` + re-export `Suppressed`, `BufferConfig`; lib prose.
- `src/dsl/ktable.rs` — `window_grace_ms` field + `with_window_grace`/getter; `map_values`/`map_values_materialized`/`filter` propagate grace; the `suppress` method.
- `src/dsl/windowed_kgrouped.rs` + `src/dsl/session_windowed_kgrouped.rs` — set grace on the returned windowed KTable (4 sites).
- `src/dsl/names.rs` — `KTABLE_SUPPRESS`.
- `tests/dsl_execution.rs` — suppress execution tests.
- `tests/dsl_golden_frame.rs` — `suppress_until_window_closes_matches_jvm`.
- `tests/jvm-capture/.../Capture.java` + `run.sh` — fixture #13.

## Execution batches (non-overlapping file sets per batch)

- **Batch 1 (parallel):** Task 1 (`suppress_buffer.rs`) ∥ Task 2 (`suppress.rs` config).
- **Batch 2:** Task 3 (`suppress.rs` processor) — needs Task 1.
- **Batch 3:** Task 4 (grace plumbing in `ktable.rs`/`windowed_kgrouped.rs`/`session_windowed_kgrouped.rs` + `suppress` method + `names.rs`) — needs Tasks 2, 3.
- **Batch 4:** Task 5 (execution tests) — needs Task 4.
- **Batch 5 (Phase C):** Task 6 (capture + golden, controller runs Docker) then Task 7 (docs + final verify).

---

## Task 1: `TimeOrderedKeyValueBuffer`

**Files:**
- Create: `crates/client-streams/src/dsl/processors/suppress_buffer.rs`
- Modify: `crates/client-streams/src/dsl/processors/mod.rs`

- [ ] **Step 1: Register the module.** In `src/dsl/processors/mod.rs` add:

```rust
pub(crate) mod suppress_buffer;
```

- [ ] **Step 2: Create `suppress_buffer.rs`** with the buffer + tests:

```rust
//! In-memory time-ordered buffer for `suppress` (KIP final-results). Holds at most
//! one entry per key (replace-by-key), ordered by `(buffer_time, seq)` so eviction
//! drains the earliest-closing windows first. Unbounded in Slice A (no size cap);
//! Slice B adds record/byte accounting + overflow, Slice D adds serialization for
//! the changelog.
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

struct Entry<K, V> {
    key: K,
    value: V,
    record_ts: i64,
}

/// Time-ordered, replace-by-key buffer. `K` must be `Eq + Hash + Clone` (the
/// suppress key is `Windowed<KInner>`).
pub(crate) struct TimeOrderedKeyValueBuffer<K, V> {
    /// Ordered by `(buffer_time, seq)`; `seq` disambiguates equal buffer times.
    entries: BTreeMap<(i64, u64), Entry<K, V>>,
    /// Locate-and-replace the slot currently held by a key.
    index: HashMap<K, (i64, u64)>,
    seq: u64,
}

impl<K: Eq + Hash + Clone, V> TimeOrderedKeyValueBuffer<K, V> {
    pub(crate) fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            index: HashMap::new(),
            seq: 0,
        }
    }

    /// Insert or replace the entry for `key`. A re-put removes the key's prior slot
    /// (so there is always exactly one entry per key) and inserts a fresh slot at
    /// `(buffer_time, seq)`.
    pub(crate) fn put(&mut self, key: K, buffer_time: i64, value: V, record_ts: i64) {
        if let Some(old_slot) = self.index.remove(&key) {
            self.entries.remove(&old_slot);
        }
        let slot = (buffer_time, self.seq);
        self.seq += 1;
        self.index.insert(key.clone(), slot);
        self.entries.insert(slot, Entry { key, value, record_ts });
    }

    /// Pop and return every entry whose `buffer_time <= threshold`, in
    /// `(buffer_time, seq)` order, as `(key, value, record_ts)`.
    pub(crate) fn evict_while(&mut self, threshold: i64) -> Vec<(K, V, i64)> {
        let mut out = Vec::new();
        while let Some((&slot, _)) = self.entries.iter().next() {
            if slot.0 > threshold {
                break;
            }
            let entry = self.entries.remove(&slot).expect("slot present");
            self.index.remove(&entry.key);
            out.push((entry.key, entry.value, entry.record_ts));
        }
        out
    }

    #[allow(dead_code)] // used by tests (and Slice B accounting)
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replace_by_key_keeps_one_entry() {
        let mut b = TimeOrderedKeyValueBuffer::<String, i64>::new();
        b.put("k".into(), 10, 1, 5);
        b.put("k".into(), 10, 2, 7); // same key + buffer_time → replace
        assert_eq!(b.len(), 1);
        let out = b.evict_while(10);
        assert_eq!(out, vec![("k".into(), 2, 7)]);
        assert_eq!(b.len(), 0);
    }

    #[test]
    fn evicts_in_buffer_time_order_up_to_threshold() {
        let mut b = TimeOrderedKeyValueBuffer::<String, i64>::new();
        b.put("a".into(), 30, 1, 30);
        b.put("b".into(), 10, 2, 10);
        b.put("c".into(), 20, 3, 20);
        // threshold 20 → evict b(10), c(20); a(30) stays.
        let out = b.evict_while(20);
        assert_eq!(
            out,
            vec![("b".into(), 2, 10), ("c".into(), 3, 20)]
        );
        assert_eq!(b.len(), 1);
        // raising the threshold drains the rest.
        assert_eq!(b.evict_while(100), vec![("a".into(), 1, 30)]);
    }

    #[test]
    fn evict_below_threshold_returns_empty() {
        let mut b = TimeOrderedKeyValueBuffer::<String, i64>::new();
        b.put("a".into(), 50, 1, 50);
        assert_eq!(b.evict_while(49), vec![]);
        assert_eq!(b.len(), 1);
    }
}
```

- [ ] **Step 3: Run tests.** Run: `cd /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl && cargo test -p crabka-client-streams suppress_buffer` → 3 tests PASS.
- [ ] **Step 4: clippy + fmt.** `cargo clippy -p crabka-client-streams --lib -- -D warnings` (clean) + `cargo fmt -p crabka-client-streams`.
- [ ] **Step 5: Commit.**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl add crates/client-streams/src/dsl/processors/suppress_buffer.rs crates/client-streams/src/dsl/processors/mod.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-dsl): TimeOrderedKeyValueBuffer (replace-by-key, ordered eviction)"
```

---

## Task 2: `Suppressed` + `BufferConfig`

**Files:**
- Create: `crates/client-streams/src/dsl/suppress.rs`
- Modify: `crates/client-streams/src/dsl/mod.rs`, `src/lib.rs`

- [ ] **Step 1: Create `src/dsl/suppress.rs`:**

```rust
//! `Suppressed` + `BufferConfig` — the suppress configuration surface.
//!
//! Slice A implements `until_window_closes(unbounded())` (final results for
//! windowed tables). `BufferConfig` is a marker for the unbounded buffer here;
//! Slice B grows it (`max_records`/`max_bytes` + overflow), Slice C adds
//! `until_time_limit`, Slice D adds the logging toggle.

/// How the suppress buffer is bounded. Slice A: unbounded only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferConfig {
    _private: (),
}

impl BufferConfig {
    /// An unbounded in-memory buffer (no record/byte cap). The only Slice-A config.
    #[must_use]
    pub fn unbounded() -> Self {
        Self { _private: () }
    }
}

/// A suppression configuration. Slice A: `until_window_closes`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Suppressed {
    #[allow(dead_code)] // read by the lowering once Slice B/C branch on the buffer
    pub(crate) buffer: BufferConfig,
}

impl Suppressed {
    /// Emit each window's final result once the window closes
    /// (`stream_time >= window.end + grace`). Requires a windowed `KTable`.
    #[must_use]
    pub fn until_window_closes(buffer: BufferConfig) -> Self {
        Self { buffer }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors() {
        let s = Suppressed::until_window_closes(BufferConfig::unbounded());
        assert_eq!(s.buffer, BufferConfig::unbounded());
    }
}
```

- [ ] **Step 2: Register + re-export.** In `src/dsl/mod.rs` add `pub mod suppress;` (next to the other `pub mod` lines) and add a re-export line:

```rust
pub use suppress::{BufferConfig, Suppressed};
```

In `src/lib.rs`, add `BufferConfig, Suppressed` to the public `pub use dsl::{…}` re-export list (alphabetical position — read the current line first and insert).

- [ ] **Step 3: Run + verify.** `cargo test -p crabka-client-streams --lib suppress::tests` (PASS) + `cargo build -p crabka-client-streams` + `cargo clippy -p crabka-client-streams --lib -- -D warnings` + `cargo fmt -p crabka-client-streams`.
- [ ] **Step 4: Commit.**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl add crates/client-streams/src/dsl/suppress.rs crates/client-streams/src/dsl/mod.rs crates/client-streams/src/lib.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-dsl): Suppressed + BufferConfig (until_window_closes/unbounded)"
```

---

## Task 3: `KTableSuppressProcessor`

**Files:**
- Create: `crates/client-streams/src/dsl/processors/suppress.rs`
- Modify: `crates/client-streams/src/dsl/processors/mod.rs`

Mirrors the KTable Change-processor pattern in `src/dsl/processors/table.rs` (read `KTableSourceProcessor` for the `impl Processor<.., Change<..>, .., Change<..>>` shape), but holds an owned buffer instead of a store — so no `ctx.get_state_store`, no borrow scoping.

- [ ] **Step 1: Register the module.** In `src/dsl/processors/mod.rs` add (next to `suppress_buffer`):

```rust
pub(crate) mod suppress;
```

- [ ] **Step 2: Create `src/dsl/processors/suppress.rs`:**

```rust
//! `KTableSuppressProcessor` — KIP final-results suppression (`untilWindowCloses`).
//! Buffers the per-window `Change` updates and forwards each window's final value
//! once stream-time passes `window.end + grace`. Emit-on-close (vs the windowed
//! aggregations' emit-on-update).
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::dsl::processors::change::Change;
use crate::dsl::processors::suppress_buffer::TimeOrderedKeyValueBuffer;
use crate::dsl::windows::Windowed;
use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;

type Marker<T> = PhantomData<fn() -> T>;

/// Suppress processor for a windowed `KTable<Windowed<KInner>, V>`. `grace_ms` is
/// the upstream window's grace; a window closes when `observed_stream_time >=
/// window.end + grace_ms`.
#[allow(dead_code)]
pub(crate) struct KTableSuppressProcessor<KInner, V> {
    pub buffer: TimeOrderedKeyValueBuffer<Windowed<KInner>, Change<V>>,
    pub observed_stream_time: i64,
    pub grace_ms: i64,
    pub _pd: Marker<(KInner, V)>,
}

impl<KInner, V> KTableSuppressProcessor<KInner, V>
where
    KInner: Eq + std::hash::Hash + Clone,
{
    pub(crate) fn new(grace_ms: i64) -> Self {
        Self {
            buffer: TimeOrderedKeyValueBuffer::new(),
            observed_stream_time: i64::MIN,
            grace_ms,
            _pd: PhantomData,
        }
    }
}

#[async_trait]
impl<KInner, V> Processor<Windowed<KInner>, Change<V>, Windowed<KInner>, Change<V>>
    for KTableSuppressProcessor<KInner, V>
where
    KInner: std::any::Any + Send + Sync + Clone + Eq + std::hash::Hash,
    V: std::any::Any + Send + Clone,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<KInner>, Change<V>>,
        r: Record<Windowed<KInner>, Change<V>>,
    ) {
        let key = r.key.expect("suppress requires a non-null key");
        self.observed_stream_time = self.observed_stream_time.max(r.timestamp);
        let buffer_time = key.window.end;
        self.buffer.put(key, buffer_time, r.value, r.timestamp);

        let threshold = self.observed_stream_time - self.grace_ms;
        for (k, change, rts) in self.buffer.evict_while(threshold) {
            ctx.forward(Record::new(Some(k), change, rts));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::marker::PhantomData;

    use super::*;
    use crate::dsl::windows::Window;
    use crate::processor::api::ProcessorContext;
    use crate::processor::erased::{Dispatch, ErasedRecord};
    use crate::processor::record::{Record, RecordContext};
    use crate::store::registry::StoreRegistry;

    fn windowed(key: &str, start: i64, end: i64) -> Windowed<String> {
        Windowed { key: key.into(), window: Window { start, end } }
    }

    #[tokio::test]
    async fn buffers_until_window_closes_then_emits_once() {
        let mut stores = StoreRegistry::default();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext { topic: "in".into(), partition: 0, offset: 0, timestamp: 0 };

        let mut proc = KTableSuppressProcessor::<String, i64>::new(0);

        // Two updates for window [0,10): count 1 then 2. ts in [0,10) < window end.
        for (cnt, ts) in [(1i64, 1i64), (2, 3)] {
            let mut d = Dispatch { buffer: &mut buffer, children: &children, output: &mut output, record_ctx: &rc, stores: &mut stores };
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            let change = if cnt == 1 { Change::update(None, 1) } else { Change::update(Some(1), 2) };
            proc.process(&mut ctx, Record::new(Some(windowed("a", 0, 10)), change, ts)).await;
        }
        // Nothing emitted yet (stream_time = 3 < window end 10).
        assert!(buffer.is_empty());

        // A record for window [20,30) advances stream_time to 25 ≥ 10 → [0,10) closes.
        {
            let mut d = Dispatch { buffer: &mut buffer, children: &children, output: &mut output, record_ctx: &rc, stores: &mut stores };
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some(windowed("a", 20, 30)), Change::update(None, 1), 25)).await;
        }
        // Exactly the [0,10) final value (2) emits; [20,30) stays buffered.
        assert_eq!(buffer.len(), 1);
        let (_, rec) = buffer.pop_front().unwrap();
        let k = rec.key.unwrap().downcast::<Windowed<String>>().unwrap();
        assert_eq!(k.window, Window { start: 0, end: 10 });
        assert_eq!(rec.value.downcast::<Change<i64>>().unwrap().new, Some(2));
    }

    #[tokio::test]
    async fn grace_delays_close() {
        let mut stores = StoreRegistry::default();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext { topic: "in".into(), partition: 0, offset: 0, timestamp: 0 };

        let mut proc = KTableSuppressProcessor::<String, i64>::new(5); // grace 5

        {
            let mut d = Dispatch { buffer: &mut buffer, children: &children, output: &mut output, record_ctx: &rc, stores: &mut stores };
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some(windowed("a", 0, 10)), Change::update(None, 1), 5)).await;
        }
        // stream_time 12 → threshold 12-5=7 < window end 10 → NOT closed.
        {
            let mut d = Dispatch { buffer: &mut buffer, children: &children, output: &mut output, record_ctx: &rc, stores: &mut stores };
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some(windowed("b", 10, 20)), Change::update(None, 1), 12)).await;
        }
        assert!(buffer.is_empty());
        // stream_time 16 → threshold 11 >= 10 → [0,10) closes.
        {
            let mut d = Dispatch { buffer: &mut buffer, children: &children, output: &mut output, record_ctx: &rc, stores: &mut stores };
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some(windowed("c", 20, 30)), Change::update(None, 1), 16)).await;
        }
        assert_eq!(buffer.len(), 1);
        let (_, rec) = buffer.pop_front().unwrap();
        assert_eq!(rec.key.unwrap().downcast::<Windowed<String>>().unwrap().window, Window { start: 0, end: 10 });
    }
}
```

- [ ] **Step 3: Run tests.** `cargo test -p crabka-client-streams suppress::tests` → 2 tests PASS. (If `ErasedRecord` key/value aren't `downcast`-able as written, mirror the exact downcast pattern from `window_aggregate.rs`'s tests.)
- [ ] **Step 4: clippy + fmt.** `cargo clippy -p crabka-client-streams --all-targets -- -D warnings` + `cargo fmt -p crabka-client-streams`.
- [ ] **Step 5: Commit.**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl add crates/client-streams/src/dsl/processors/suppress.rs crates/client-streams/src/dsl/processors/mod.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-dsl): KTableSuppressProcessor (emit-on-window-close)"
```

---

## Task 4: grace plumbing + `KTable::suppress` + `names.rs`

**Files:**
- Modify: `src/dsl/ktable.rs`, `src/dsl/windowed_kgrouped.rs`, `src/dsl/session_windowed_kgrouped.rs`, `src/dsl/names.rs`

- [ ] **Step 1: Add the grace field + accessors** to `src/dsl/ktable.rs`. Add the field to the struct:

```rust
pub struct KTable<K, V> {
    builder: Rc<RefCell<InternalStreamsBuilder>>,
    node: NodeId,
    #[allow(dead_code)]
    store_name: Option<String>,
    #[allow(dead_code)]
    source_topic: Option<String>,
    /// For windowed tables: the upstream window's grace (suppress closes a window
    /// at `window.end + window_grace_ms`). `None` for non-windowed tables.
    window_grace_ms: Option<i64>,
    _pd: PhantomData<fn() -> (K, V)>,
}
```

In `KTable::new`, initialize `window_grace_ms: None` (keep the 4-arg signature):

```rust
        Self {
            builder,
            node,
            store_name,
            source_topic,
            window_grace_ms: None,
            _pd: PhantomData,
        }
```

Add a builder method + getter in the `impl<K, V> KTable<K, V>` block (next to `store_name`):

```rust
    /// Tag this table with its upstream window's grace (set by windowed/session
    /// aggregations; propagated through `Change`-preserving ops). Read by `suppress`.
    #[must_use]
    pub(crate) fn with_window_grace(mut self, grace_ms: Option<i64>) -> Self {
        self.window_grace_ms = grace_ms;
        self
    }

    pub(crate) fn window_grace_ms(&self) -> Option<i64> {
        self.window_grace_ms
    }
```

- [ ] **Step 2: Propagate grace through `map_values` / `map_values_materialized` / `filter`.** In each of those three methods in `ktable.rs`, capture `let grace = self.window_grace_ms;` near the top (before `borrow_mut`), and change the trailing `KTable::new(...)` to `KTable::new(...).with_window_grace(grace)`. (These ops keep the key, so a derived windowed table keeps its grace.)

- [ ] **Step 3: Set grace on the windowed aggregations.** In `src/dsl/windowed_kgrouped.rs`, both return sites (the `lower_aggregate_windowed` and `lower_reduce_windowed` tails, currently `KTable::new(Rc::clone(&self.builder), agg_id, Some(store_name), None)` / `red_id`), change to append `.with_window_grace(Some(windows.grace_ms))`. (`windows` is in scope in both methods.) Do the same in `src/dsl/session_windowed_kgrouped.rs` (the `lower_aggregate` and `lower_reduce` tails — `windows.grace_ms` is the `SessionWindows` grace).

- [ ] **Step 4: Add the suppress node name.** In `src/dsl/names.rs`, after `KTABLE_MERGE` add:

```rust
/// The JVM `KTableImpl.suppress` processor node prefix.
pub(crate) const KTABLE_SUPPRESS: &str = "KTABLE-SUPPRESS-";
```

- [ ] **Step 5: Add the `suppress` method** to `ktable.rs`. Add a specialized impl block (after the existing `impl<K,V> KTable<K,V> where …` block) — import `Windowed`, `Hash`, `Suppressed`, the processor, as needed:

```rust
impl<KInner, V> KTable<crate::dsl::windows::Windowed<KInner>, V>
where
    KInner: Any + Send + Sync + Clone + Eq + std::hash::Hash,
    V: Any + Send + Clone,
{
    /// `suppress(untilWindowCloses(..))`: buffer per-window updates and emit each
    /// window's final value once it closes (`stream_time >= window.end + grace`).
    /// The grace comes from the upstream windowed aggregation.
    #[must_use]
    pub fn suppress(
        &self,
        _suppressed: crate::dsl::suppress::Suppressed,
    ) -> KTable<crate::dsl::windows::Windowed<KInner>, V> {
        let grace_ms = self.window_grace_ms.unwrap_or(0);
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::KTABLE_SUPPRESS);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::TableProcessor { store_name: None },
            vec![parent_id],
        );
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            // Parent forwards Change<V>; suppress buffers and forwards Change<V>.
            let parent = NodeHandle::<
                crate::dsl::windows::Windowed<KInner>,
                Change<V>,
            >::from_name(state.handle_name[&parent_id].clone());
            let h = state
                .topology
                .add_processor::<
                    crate::dsl::windows::Windowed<KInner>,
                    Change<V>,
                    crate::dsl::windows::Windowed<KInner>,
                    Change<V>,
                    _,
                    _,
                    _,
                >(
                    name.clone(),
                    move || {
                        crate::dsl::processors::suppress::KTableSuppressProcessor::<KInner, V>::new(
                            grace_ms,
                        )
                    },
                    [parent],
                );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        KTable::new(Rc::clone(&self.builder), id, None, None).with_window_grace(Some(grace_ms))
    }
}
```

(Match the existing `ktable.rs` imports — `Any`, `Change`, `GraphNodeKind`, `LowerState`, `NodeHandle`, `names`, `Rc` are already imported by the other methods; add `std::hash::Hash` if needed.)

- [ ] **Step 6: Build + run.** `cargo build -p crabka-client-streams` (compiles) + `cargo test -p crabka-client-streams --lib` (existing tests green) + `cargo clippy -p crabka-client-streams --all-targets -- -D warnings` + `cargo fmt -p crabka-client-streams`. The DSL is exercised by Tasks 5/6.
- [ ] **Step 7: Commit.**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl add crates/client-streams/src/dsl/ktable.rs crates/client-streams/src/dsl/windowed_kgrouped.rs crates/client-streams/src/dsl/session_windowed_kgrouped.rs crates/client-streams/src/dsl/names.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-dsl): KTable::suppress(untilWindowCloses) + window-grace plumbing"
```

---

## Task 5: Suppress execution tests

**Files:**
- Modify: `crates/client-streams/tests/dsl_execution.rs`

Use the existing windowed/session execution tests in this file as the harness template (`StreamsBuilder` → `build` → `TopologyTestDriver::new(&built).unwrap()` → `d.pipe_input("in", Consumed::with(..), Some(key), value, ts)` → `d.read_output("out", Produced::with(TimeWindowedSerde::new(StringSerde, size), I64Serde))` returning `Option<(Option<Windowed<String>>, i64)>`).

- [ ] **Step 1: Add the tests.** Append to `tests/dsl_execution.rs`:

```rust
/// Suppress(untilWindowCloses): a window's count is buffered and emitted exactly
/// once, when stream-time passes the window's end. Records in window [0,60000)
/// produce no output until a later-window record advances stream-time past 60000.
#[test]
fn dsl_suppress_until_window_closes_emits_final_only() {
    use crabka_client_streams::{
        BufferConfig, Grouped, I64Serde, Materialized, Suppressed, TimeWindowedSerde, TimeWindows,
        Window, Windowed,
    };
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .group_by_key(Grouped::with(StringSerde, StringSerde))
        .windowed_by(TimeWindows::of_size(60_000))
        .count(Materialized::with(StringSerde, I64Serde))
        .suppress(Suppressed::until_window_closes(BufferConfig::unbounded()))
        .to_stream()
        .to(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, 60_000), I64Serde),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // Two records in window [0,60000): count → 2. No output yet.
    for ts in [1_000i64, 3_000] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some("a".to_string()),
            "x".to_string(),
            ts,
        );
    }
    let out = Produced::with(TimeWindowedSerde::new(StringSerde, 60_000), I64Serde);
    assert_eq!(d.read_output("out", out), None); // buffered, not yet closed
    // A record in window [60000,120000) advances stream-time to 65000 ≥ 60000 →
    // window [0,60000) closes, emitting its final count (2) exactly once.
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("a".to_string()),
        "x".to_string(),
        65_000,
    );
    assert_eq!(
        d.read_output("out", out),
        Some((
            Some(Windowed { key: "a".into(), window: Window { start: 0, end: 60_000 } }),
            2
        ))
    );
    // The [60000,120000) window is still buffered → no further output.
    assert_eq!(d.read_output("out", out), None);
}
```

> **Implementer note:** adapt to this file's real `pipe_input`/`read_output` signatures (read an existing windowed test). `Produced`/`TimeWindowedSerde` are `Copy` — pass `out` by value (no `.clone()`, which would trip `clippy::clone_on_copy`). The contract is: no output until the later-window record arrives, then exactly `([0,60000), 2)`.

- [ ] **Step 2: Run.** `cargo test -p crabka-client-streams --test dsl_execution suppress` → PASS.
- [ ] **Step 3: clippy + fmt.** `cargo clippy -p crabka-client-streams --test dsl_execution -- -D warnings` + `cargo fmt -p crabka-client-streams --check`.
- [ ] **Step 4: Commit.**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl add crates/client-streams/tests/dsl_execution.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(streams-dsl): suppress(untilWindowCloses) execution test (final-only emit)"
```

---

## Task 6: `suppress_until_window_closes` golden (Phase C — controller runs Docker)

**Files:**
- Modify: `tests/jvm-capture/src/main/java/crabka/capture/Capture.java`, `tests/jvm-capture/run.sh`
- Create: `tests/testdata/golden/dsl/suppress_until_window_closes.topology.json`
- Modify: `tests/dsl_golden_frame.rs`

> **The controller (not a subagent) runs the Docker capture step.**

- [ ] **Step 1: Add the Java fixture #13.** In `Capture.java` register it in `main` (after the `session_count` line):

```java
        write(outDir, "suppress_until_window_closes", suppressUntilWindowCloses());
```

and bump the completion message to `13 fixtures`. Add the method (next to `windowedCount()`):

```java
    /**
     * 13. suppress_until_window_closes: windowed count + suppress(untilWindowCloses,
     * logging disabled). With logging disabled the suppress buffer adds no changelog,
     * so the wire is expected byte-identical to windowed_count.
     */
    static Topology suppressUntilWindowCloses() {
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in")
            .groupByKey()
            .windowedBy(org.apache.kafka.streams.kstream.TimeWindows.ofSizeWithNoGrace(
                java.time.Duration.ofSeconds(60)))
            .count()
            .suppress(org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses(
                    org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded())
                .withLoggingDisabled())
            .toStream()
            .to("out");
        return b.build(optimizedProps());
    }
```

Update `run.sh`'s fixture-count comments (12 → 13; add `suppress_until_window_closes`).

- [ ] **Step 2: Run the Docker capture (CONTROLLER).** `cd crates/client-streams/tests/jvm-capture && ./run.sh --gradle` → writes `../testdata/golden/dsl/suppress_until_window_closes.topology.json` and rewrites the other 12 (which must stay byte-identical — `git status --short` shows only the new file untracked).

- [ ] **Step 3: Inspect + reconcile.** Read the fixture. Expected: byte-identical to `windowed_count.topology.json` (one subtopology, source `["in"]`, the one `KSTREAM-AGGREGATE-STATE-STORE-0000000001-changelog`, no suppress changelog). If the JVM surfaces an extra topic or a different store index/name, that's the oracle — tune the DSL (the `KTABLE_SUPPRESS` prefix in `names.rs`, or the lowering) until the golden matches. If `withLoggingDisabled` requires a `.withName(...)` in 4.1 to compile/behave, add it (it doesn't change the wire).

- [ ] **Step 4: Add the golden frame test.** In `tests/dsl_golden_frame.rs` (mirror `windowed_count_matches_jvm`):

```rust
#[test]
fn suppress_until_window_closes_matches_jvm() {
    use crabka_client_streams::{
        BufferConfig, Grouped, I64Serde, Materialized, Suppressed, TimeWindowedSerde, TimeWindows,
    };
    // Mirrors Capture.java `suppressUntilWindowCloses()`. With logging disabled the
    // suppress buffer adds no changelog → wire is byte-identical to windowed_count.
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .group_by_key(Grouped::with(StringSerde, StringSerde))
        .windowed_by(TimeWindows::of_size(60_000))
        .count(Materialized::with(StringSerde, I64Serde))
        .suppress(Suppressed::until_window_closes(BufferConfig::unbounded()))
        .to_stream()
        .to(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, 60_000), I64Serde),
        );
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "suppress_until_window_closes");
}
```

- [ ] **Step 5: Run goldens.** `cargo test -p crabka-client-streams --test dsl_golden_frame` → `suppress_until_window_closes_matches_jvm` PASS **and all 12 prior PASS** (`13 passed`).
- [ ] **Step 6: Commit.**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl add crates/client-streams/tests/jvm-capture/src/main/java/crabka/capture/Capture.java crates/client-streams/tests/jvm-capture/run.sh crates/client-streams/tests/testdata/golden/dsl/suppress_until_window_closes.topology.json crates/client-streams/tests/dsl_golden_frame.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(streams-dsl): suppress_until_window_closes golden (#13) captured from JVM 4.1"
```

---

## Task 7: Docs + final verification

**Files:**
- Modify: `crates/client-streams/src/lib.rs`

- [ ] **Step 1: Add a suppress paragraph** to the `lib.rs` crate docs, after the session-window paragraph:

```rust
//! [`KTable::suppress`]`(`[`Suppressed`]`::until_window_closes(`[`BufferConfig`]`::unbounded()))`
//! turns a windowed table's emit-on-update stream into **final results**: it buffers
//! each window's updates and forwards the window's final value exactly once, when
//! stream-time passes `window.end + grace` (the grace comes from the upstream
//! windowed aggregation). The buffer is in-memory and time-ordered; bounded buffers,
//! `untilTimeLimitElapsed`, and the buffer changelog are later slices.
```

(Adjust intra-doc links to ones that resolve — `KTable`, `Suppressed`, `BufferConfig` are re-exported.)

- [ ] **Step 2: Final verification.** Run, in order:

```
cargo test -p crabka-client-streams
cargo test -p crabka-client-streams --doc
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
cargo fmt -p crabka-client-streams --check
```

Expected: all green; `dsl_golden_frame` shows `13 passed` (12 prior byte-identical + suppress); `dsl_execution` includes the suppress test; `suppress_buffer` (3) + `suppress::tests` (2) green; no clippy warnings; fmt clean.

- [ ] **Step 3: Commit.**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl add crates/client-streams/src/lib.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "docs(streams-dsl): document suppress(untilWindowCloses) + final slice-A verification"
```

---

## Done criteria

- `KTable<Windowed<K>,V>::suppress(Suppressed::until_window_closes(BufferConfig::unbounded()))` buffers per-window updates and emits each window's final value once `stream_time >= window.end + grace`.
- Grace threaded from windowed + session aggregations into the KTable handle (propagated through `map_values`/`filter`).
- `suppress_until_window_closes` golden byte-matches JVM 4.1 (== `windowed_count` shape, logging disabled); **12 prior goldens byte-identical**.
- Buffer + processor unit tests + the execution test (buffer-then-final-emit, graced close) pass.
- Full suite + doctests + clippy `--all-targets` + fmt all green.
