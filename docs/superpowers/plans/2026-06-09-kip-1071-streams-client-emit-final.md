# Emit-Final (KIP-825) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add native emit-on-window-close (`EmitStrategy.ON_WINDOW_CLOSE`, KIP-825) to the time, sliding, and session windowed aggregations in `crabka-client-streams`.

**Architecture:** Emit-final is a *behavioral mode on the existing aggregate processors*, not a new topology node. A new `EmitStrategy` DSL type is threaded from the windowed handles into the processors via an `.emit_strategy()` builder. In `ON_WINDOW_CLOSE` mode the processor still updates its store but suppresses every per-update `ctx.forward`, then scans the store for now-closed windows and forwards one final `Change` each, advancing a `last_emitted_close` watermark. Lowering and store registration are unchanged, so the wire topology is byte-identical to emit-on-update.

**Tech Stack:** Rust 2024, `async-trait`, tokio, the existing `client-streams` DSL/graph/lowering machinery, `TopologyTestDriver`, JVM golden capture (apache/kafka:4.1.0).

**Spec:** `docs/superpowers/specs/2026-06-09-kip-1071-streams-client-emit-final-design.md`

**Conventions for every task:**
- Worktree: all `git` commands MUST use `git -C /Users/mattstone/git/crabka/.claude/worktrees/hardcore-rosalind-d53a17` and the branch is `claude/hardcore-rosalind-d53a17`. Assert the branch before committing.
- Commit identity: `git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com"` (never `git config`).
- All commands run from the crate dir: `cd /Users/mattstone/git/crabka/.claude/worktrees/hardcore-rosalind-d53a17/crates/client-streams`.
- Before pushing/finishing: `cargo fmt --all` then `cargo clippy --workspace --all-targets -- -D warnings` (CI gate is `--all-targets`, not `--lib`).
- Commit-message footer line: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

---

## Batch 0 — Foundation (sequential; later batches depend on this)

Files in this batch (`dsl/emit.rs`, `dsl/mod.rs`, `store/window.rs`, `store/session.rs`) are touched only here, so Batch 1 tasks are conflict-free afterward.

### Task 1: `EmitStrategy` DSL type

**Files:**
- Create: `crates/client-streams/src/dsl/emit.rs`
- Modify: `crates/client-streams/src/dsl/mod.rs` (add `pub mod emit;` near line 8 and `pub use emit::EmitStrategy;` near line 22)

- [ ] **Step 1: Write the failing test** — append to the new file `src/dsl/emit.rs`:

```rust
//! `EmitStrategy` — when a windowed aggregation forwards results. Mirrors the JVM
//! `org.apache.kafka.streams.kstream.EmitStrategy`: `on_window_update()` (the
//! default, emit on every update) vs `on_window_close()` (emit each window's final
//! result once stream-time passes its close).
//!
//! Carried as a `Copy` field on the windowed handles and threaded into the
//! aggregate processors at lowering. It changes ONLY runtime forwarding behavior —
//! the lowered topology (node kind, store registration, names) is identical for
//! both strategies, matching the JVM (one `KStreamWindowAggregate` class
//! parameterized by `EmitStrategy`).

/// When a windowed aggregation forwards its results downstream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmitStrategy {
    kind: EmitKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EmitKind {
    OnWindowUpdate,
    OnWindowClose,
}

impl EmitStrategy {
    /// Emit on every update (the default).
    #[must_use]
    pub fn on_window_update() -> Self {
        Self {
            kind: EmitKind::OnWindowUpdate,
        }
    }

    /// Emit each window's final result once stream-time passes its close.
    #[must_use]
    pub fn on_window_close() -> Self {
        Self {
            kind: EmitKind::OnWindowClose,
        }
    }

    /// True for the emit-on-update (default) strategy. Aggregate processors guard
    /// their per-update `ctx.forward` with this.
    pub(crate) fn is_on_update(self) -> bool {
        matches!(self.kind, EmitKind::OnWindowUpdate)
    }

    /// True for the emit-on-close strategy.
    pub(crate) fn is_on_close(self) -> bool {
        matches!(self.kind, EmitKind::OnWindowClose)
    }
}

impl Default for EmitStrategy {
    fn default() -> Self {
        Self::on_window_update()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_on_update() {
        assert!(EmitStrategy::default().is_on_update());
        assert!(!EmitStrategy::default().is_on_close());
    }

    #[test]
    fn on_window_close_is_close() {
        let e = EmitStrategy::on_window_close();
        assert!(e.is_on_close());
        assert!(!e.is_on_update());
    }
}
```

- [ ] **Step 2: Wire the module** — in `src/dsl/mod.rs`, add `pub mod emit;` alongside the other `pub mod` lines (≈line 8) and `pub use emit::EmitStrategy;` alongside the other `pub use` lines (≈line 22).

- [ ] **Step 3: Run the test to verify it passes**

Run: `cargo test -p crabka-client-streams dsl::emit:: -- --nocapture`
Expected: PASS (2 tests).

- [ ] **Step 4: Commit**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/hardcore-rosalind-d53a17 add crates/client-streams/src/dsl/emit.rs crates/client-streams/src/dsl/mod.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/hardcore-rosalind-d53a17 -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(client-streams): EmitStrategy DSL type (KIP-825)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Window store `fetch_all_in_range` (cross-key closed-window scan)

**Files:**
- Modify: `crates/client-streams/src/store/window.rs` (trait `WindowStore` ≈line 19-27; impl ≈line 146-212; tests ≈line 214)

Background: `WindowBytesStore` keys are `key‖windowStart:8B‖seq:4B` (`store/window_schema.rs` helpers `store_key`, `window_start_of`, `key_bytes_of`, `unwrap_value`). Because the inner key prefixes the window-start, there is no `range()` shortcut across keys — emit-final needs a full `scan_all()` filtered by decoded window-start.

- [ ] **Step 1: Write the failing test** — append to the `#[cfg(test)] mod tests` in `src/store/window.rs`:

```rust
#[tokio::test]
async fn fetch_all_in_range_scans_across_keys() {
    let mut s = WindowBytesStore::<String, i64>::in_memory(
        "w".into(),
        Box::new(StringSerde),
        Box::new(I64Serde),
        "app-w-changelog".into(),
    );
    // Two keys, three windows. windowStart ∈ {0, 0, 10}.
    s.put("a".into(), 0, 1, 5).await;
    s.put("b".into(), 0, 7, 6).await;
    s.put("a".into(), 10, 9, 12).await;

    // Range [0,0] returns both windowStart==0 entries (either key order).
    let mut got = s.fetch_all_in_range(0, 0).await;
    got.sort();
    assert_eq!(
        got,
        vec![
            ("a".to_string(), 0, 5, 1),
            ("b".to_string(), 0, 6, 7),
        ]
    );

    // Range [0,10] returns all three.
    assert_eq!(s.fetch_all_in_range(0, 10).await.len(), 3);
    // Range above everything returns nothing.
    assert!(s.fetch_all_in_range(11, 100).await.is_empty());
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p crabka-client-streams store::window::tests::fetch_all_in_range_scans_across_keys`
Expected: FAIL — `no method named fetch_all_in_range`.

- [ ] **Step 3: Add the trait method** — in `src/store/window.rs`, extend the `WindowStore` trait (after `fetch_with_ts`, before `put`):

```rust
    /// Every window across ALL keys whose `windowStart` is in `[start_from,
    /// start_to]`, as `(key, windowStart, recordTs, value)`. Backs emit-final's
    /// closed-window scan (the byte layout is key-prefixed, so this is a filtered
    /// full scan, mirroring the JVM `fetchAll`).
    async fn fetch_all_in_range(
        &self,
        start_from: i64,
        start_to: i64,
    ) -> Vec<(K, i64, i64, V)>;
```

- [ ] **Step 4: Implement it** — in the `impl ... WindowStore for WindowBytesStore` block (after `fetch_with_ts`, before `put`). Note `key_bytes_of` returns the serialized inner key bytes, deserialized via `key_serde`:

```rust
    async fn fetch_all_in_range(
        &self,
        start_from: i64,
        start_to: i64,
    ) -> Vec<(K, i64, i64, V)> {
        let mut out = Vec::new();
        for (k, wrapped) in self.backend.scan_all().await {
            let ws = window_start_of(&k);
            if ws < start_from || ws > start_to {
                continue;
            }
            let key = self
                .key_serde
                .deserialize(&self.changelog_topic, key_bytes_of(&k))
                .expect("window key deserialize");
            let (ts, raw) = unwrap_value(&wrapped);
            let value = self
                .value_serde
                .deserialize(&self.changelog_topic, raw)
                .expect("window value deserialize");
            out.push((key, ws, ts, value));
        }
        out
    }
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p crabka-client-streams store::window::tests::fetch_all_in_range_scans_across_keys`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/hardcore-rosalind-d53a17 add crates/client-streams/src/store/window.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/hardcore-rosalind-d53a17 -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(client-streams): WindowStore::fetch_all_in_range for emit-final scan

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Session store `find_closed_sessions`

**Files:**
- Modify: `crates/client-streams/src/store/session.rs` (trait `SessionStore` ≈line 19-31; impl block; tests)

Background: `SessionBytesStore` keys are `key‖end:8B‖start:8B` (`store/session_schema.rs` helpers `session_end_of`, `session_start_of`, `session_key_bytes_of`). Emit-final scans by `session.end`.

- [ ] **Step 1: Write the failing test** — append to the `#[cfg(test)] mod tests` in `src/store/session.rs` (match the existing test imports there; `StringSerde`/`I64Serde` from `crate::processor::serde`):

```rust
#[tokio::test]
async fn find_closed_sessions_scans_by_end() {
    let mut s = SessionBytesStore::<String, i64>::in_memory(
        "s".into(),
        Box::new(StringSerde),
        Box::new(I64Serde),
        "app-s-changelog".into(),
    );
    // (key, start, end, value)
    s.put("a".into(), 0, 5, 1).await;
    s.put("b".into(), 2, 8, 2).await;
    s.put("a".into(), 20, 30, 3).await;

    // end <= 8 → the first two (either key order).
    let mut got = s.find_closed_sessions(8).await;
    got.sort();
    assert_eq!(
        got,
        vec![("a".to_string(), 0, 5, 1), ("b".to_string(), 2, 8, 2)]
    );
    // end <= 4 → none.
    assert!(s.find_closed_sessions(4).await.is_empty());
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p crabka-client-streams store::session::tests::find_closed_sessions_scans_by_end`
Expected: FAIL — `no method named find_closed_sessions`.

- [ ] **Step 3: Add the trait method** — extend the `SessionStore` trait (after `remove`):

```rust
    /// Every session across ALL keys whose `end <= close_time`, as
    /// `(key, start, end, value)`. Backs emit-final's closed-session scan.
    async fn find_closed_sessions(&self, close_time: i64) -> Vec<(K, i64, i64, V)>;
```

- [ ] **Step 4: Implement it** — in the `impl ... SessionStore for SessionBytesStore` block. Bring the `scan_all` decode helpers into scope (the `use crate::store::session_schema::{...}` at the top already imports `session_end_of`, `session_start_of`, `session_key_bytes_of`):

```rust
    async fn find_closed_sessions(&self, close_time: i64) -> Vec<(K, i64, i64, V)> {
        let mut out = Vec::new();
        for (k, raw) in self.backend.scan_all().await {
            let end = session_end_of(&k);
            if end > close_time {
                continue;
            }
            let key = self
                .key_serde
                .deserialize(&self.changelog_topic, session_key_bytes_of(&k))
                .expect("session key deserialize");
            let value = self
                .value_serde
                .deserialize(&self.changelog_topic, &raw)
                .expect("session value deserialize");
            out.push((key, session_start_of(&k), end, value));
        }
        out
    }
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p crabka-client-streams store::session::tests::find_closed_sessions_scans_by_end`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/hardcore-rosalind-d53a17 add crates/client-streams/src/store/session.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/hardcore-rosalind-d53a17 -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(client-streams): SessionStore::find_closed_sessions for emit-final scan

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Batch 1 — Per-window-type emit-final (parallel; disjoint files)

After Batch 0, these three tasks touch non-overlapping files and may be dispatched concurrently:
- Task 4: `dsl/windowed_kgrouped.rs` + `dsl/processors/window_aggregate.rs`
- Task 5: `dsl/session_windowed_kgrouped.rs` + `dsl/processors/session_aggregate.rs`
- Task 6: `dsl/sliding_windowed_kgrouped.rs` + `dsl/processors/sliding_window_aggregate.rs`

All three follow the same shape: (a) add `emit: EmitStrategy` to the handle + an `.emit_strategy()` builder; (b) thread it into the processor constructor in the lowering thunk; (c) add `emit` + `last_emitted_close` fields to the processor; (d) guard every per-update `ctx.forward` with `if self.emit.is_on_update()`; (e) after the update logic, call a new `emit_closed_windows`/`emit_closed_sessions` that scans the store and forwards finals.

**Shared semantic note for the close-scan** (applies to all three; the *exact emitted `Change` shape is confirmed by the goldens in Batch 2* — implement `Change::update(None, final)` and adjust if a golden disagrees):
- `window_close_time = stream_time - grace_ms`.
- Emit windows whose `end <= window_close_time` AND `end > last_emitted_close`.
- After emitting, set `last_emitted_close = window_close_time`.
- The watermark lives on the processor (not persisted) — JVM-consistent (see spec Edge Cases).

### Task 4: Time-window emit-final (tumbling/hopping)

**Files:**
- Modify: `crates/client-streams/src/dsl/windowed_kgrouped.rs` (struct ≈line 41-56; `new` ≈line 63-80; lowering thunks ≈line 298-325 and ≈line 390-416)
- Modify: `crates/client-streams/src/dsl/processors/window_aggregate.rs` (both processor structs + impls; tests ≈line 151)

- [ ] **Step 1: Write the failing processor test** — append to `#[cfg(test)] mod tests` in `window_aggregate.rs`. It drives the aggregate processor in `on_window_close` mode and asserts nothing emits until a later record closes window `[0,10)`:

```rust
#[tokio::test]
async fn windowed_count_emit_final_emits_only_on_close() {
    use crate::dsl::emit::EmitStrategy;

    let mut stores = StoreRegistry::default();
    stores.insert(Box::new(WindowBytesStore::<String, i64>::in_memory(
        "w".into(),
        Box::new(StringSerde),
        Box::new(I64Serde),
        "app-w-changelog".into(),
    )));
    let children = [0usize];
    let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
    let mut output = Vec::new();
    let rc = RecordContext { topic: "in".into(), partition: 0, offset: 0, timestamp: 0 };

    let mut proc = KStreamWindowAggregateProcessor {
        store_name: "w".into(),
        windows: TimeWindows::of_size(10),
        init: || 0i64,
        agg: |_k: &String, _v: &String, a: i64| a + 1,
        emit: EmitStrategy::on_window_close(),
        stream_time: i64::MIN,
        last_emitted_close: i64::MIN,
        _pd: PhantomData::<fn() -> (String, String, i64)>,
    };

    // Helper to run one record.
    async fn run(
        proc: &mut KStreamWindowAggregateProcessor<
            String, String, i64,
            impl Fn() -> i64 + Send + 'static,
            impl Fn(&String, &String, i64) -> i64 + Send + 'static,
        >,
        buffer: &mut VecDeque<(usize, ErasedRecord)>,
        output: &mut Vec<ErasedRecord>,
        children: &[usize],
        stores: &mut StoreRegistry,
        rc: &RecordContext,
        k: &str,
        ts: i64,
    ) {
        let globals = crate::runtime::global::GlobalStateManager::default();
        let mut scheds = Vec::new();
        let mut d = Dispatch {
            buffer, children, output, record_ctx: rc, stores, globals: &globals,
            node_idx: 0, schedules: &mut scheds, sched_stream_time: i64::MIN, sched_wall_clock: 0,
        };
        let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
        proc.process(&mut ctx, Record::new(Some(k.into()), "x".into(), ts)).await;
    }

    // Two records in [0,10): updates the store, emits NOTHING (emit-final).
    run(&mut proc, &mut buffer, &mut output, &children, &mut stores, &rc, "a", 3).await;
    run(&mut proc, &mut buffer, &mut output, &children, &mut stores, &rc, "a", 7).await;
    assert!(buffer.is_empty(), "emit-final must not emit while window is open");

    // A record at ts=15 (window [10,20)) advances stream_time → [0,10) closes (grace 0).
    run(&mut proc, &mut buffer, &mut output, &children, &mut stores, &rc, "a", 15).await;

    // Exactly one final for [0,10) with the final count 2; [10,20) stays open.
    assert_eq!(buffer.len(), 1);
    let (_, rec) = buffer.pop_front().unwrap();
    let key = rec.key.unwrap().downcast::<Windowed<String>>().unwrap();
    assert_eq!(key.window, Window { start: 0, end: 10 });
    assert_eq!(rec.value.downcast::<Change<i64>>().unwrap().new, Some(2));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p crabka-client-streams dsl::processors::window_aggregate::tests::windowed_count_emit_final_emits_only_on_close`
Expected: FAIL — missing fields `emit`/`stream_time`/`last_emitted_close`.

- [ ] **Step 3: Add fields to both processor structs** — in `window_aggregate.rs`, add to `KStreamWindowAggregateProcessor` (after `agg`) and `KStreamWindowReduceProcessor` (after `reducer`):

```rust
    /// Emit on every update (default) or only on window close (KIP-825).
    pub emit: crate::dsl::emit::EmitStrategy,
    /// Observed max record timestamp (per task instance) — drives window-close.
    pub stream_time: i64,
    /// Highest `window_close_time` already emitted; prevents re-emit.
    pub last_emitted_close: i64,
```

- [ ] **Step 4: Guard the update-forward and add the close scan** — replace the body of `KStreamWindowAggregateProcessor::process` so the per-window `ctx.forward` is gated and a close-scan runs after. The full method becomes:

```rust
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<VA>>,
        r: Record<K, V>,
    ) {
        let key = r.key.expect("windowed aggregate requires a non-null key");
        let size = self.windows.size_ms;
        self.stream_time = self.stream_time.max(r.timestamp);
        let window_close_time = self.stream_time - self.windows.grace_ms;

        for ws in self.windows.windows_for(r.timestamp) {
            // Emit-final drops updates for windows that already closed.
            if self.emit.is_on_close() && ws + size <= self.last_emitted_close {
                continue;
            }
            let (old, new, new_ts) = {
                let store = ctx
                    .get_window_store::<K, VA>(&self.store_name)
                    .expect("window store not found");
                let prior = store.fetch_single(&key, ws).await;
                let stored_ts = prior.as_ref().map_or(i64::MIN, |&(ts, _)| ts);
                let old = prior.map(|(_ts, v)| v);
                let seed = old.clone().unwrap_or_else(|| (self.init)());
                let new = (self.agg)(&key, &r.value, seed);
                let new_ts = std::cmp::max(r.timestamp, stored_ts);
                store.put(key.clone(), ws, new.clone(), new_ts).await;
                (old, new, new_ts)
            };
            if self.emit.is_on_update() {
                ctx.forward(Record::new(
                    Some(Windowed { key: key.clone(), window: Window { start: ws, end: ws + size } }),
                    Change::update(old, new),
                    new_ts,
                ));
            }
        }

        if self.emit.is_on_close() {
            self.emit_closed_windows(ctx, window_close_time).await;
        }
    }
```

- [ ] **Step 5: Add the `emit_closed_windows` helper** — add an inherent `impl` block below the `KStreamWindowAggregateProcessor` `Processor` impl (a separate `impl<K, V, VA, I, A> KStreamWindowAggregateProcessor<...>` with the same bounds as the trait impl plus `VA: Clone`):

```rust
impl<K, V, VA, I, A> KStreamWindowAggregateProcessor<K, V, VA, I, A>
where
    K: std::any::Any + Send + Sync + Clone,
    V: Send + 'static,
    VA: std::any::Any + Send + Clone,
    I: Fn() -> VA + Send + 'static,
    A: Fn(&K, &V, VA) -> VA + Send + 'static,
{
    /// Forward each window whose `end <= window_close_time` and `end >
    /// last_emitted_close` as a final `Change`, ascending by `(window_start)`,
    /// then advance the watermark.
    async fn emit_closed_windows(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<VA>>,
        window_close_time: i64,
    ) {
        let size = self.windows.size_ms;
        // windowStart upper bound: end = start + size <= close ⟺ start <= close - size.
        let start_to = window_close_time - size;
        let start_from = self.last_emitted_close - size; // re-emit guard handled below
        let mut due = {
            let store = ctx
                .get_window_store::<K, VA>(&self.store_name)
                .expect("window store not found");
            store.fetch_all_in_range(start_from, start_to).await
        };
        due.retain(|(_, ws, _, _)| ws + size > self.last_emitted_close);
        due.sort_by_key(|(_, ws, _, _)| *ws);
        for (k, ws, ts, v) in due {
            ctx.forward(Record::new(
                Some(Windowed { key: k, window: Window { start: ws, end: ws + size } }),
                Change::update(None, v),
                ts,
            ));
        }
        self.last_emitted_close = window_close_time;
    }
}
```

- [ ] **Step 6: Mirror the same changes into `KStreamWindowReduceProcessor`** — add the three fields; gate its `ctx.forward` with `if self.emit.is_on_update()`; add the same `stream_time`/`window_close_time` computation + closed-window drop at the top of its `for ws` loop; add a matching `emit_closed_windows` inherent impl (bounds: `K: Any+Send+Sync+Clone, V: Any+Send+Clone, R: Fn(&V,&V)->V+Send+'static`, value type `V`).

- [ ] **Step 7: Thread `emit` through the handle** — in `windowed_kgrouped.rs`:
  - Add field `emit: EmitStrategy` to `TimeWindowedKGroupedStream` (import `use crate::dsl::emit::EmitStrategy;`).
  - In `new`, accept nothing extra; initialize `emit: EmitStrategy::on_window_update()`.
  - Add the builder:
    ```rust
    /// Emit on every update (default) or only on window close (KIP-825).
    #[must_use]
    pub fn emit_strategy(mut self, emit: EmitStrategy) -> Self {
        self.emit = emit;
        self
    }
    ```
  - In `lower_aggregate_windowed`, capture `let emit = self.emit;` before `borrow_mut`, and set `emit, stream_time: i64::MIN, last_emitted_close: i64::MIN` in the `KStreamWindowAggregateProcessor { .. }` literal inside the lowering thunk.
  - Do the same in `lower_reduce_windowed` for `KStreamWindowReduceProcessor`.

> NOTE: `KGroupedStream::windowed_by` constructs `TimeWindowedKGroupedStream::new(...)`; since `new`'s signature is unchanged (emit defaults internally), no caller changes are needed.

- [ ] **Step 8: Add a DSL-level emit-final test** — append a test to `windowed_kgrouped.rs`'s `#[cfg(test)]` (or the crate's windowed integration test module if that's where DSL+driver tests live — match the existing tumbling-count test's location) driving a full `TopologyTestDriver`:

```rust
#[tokio::test]
async fn time_window_emit_final_via_driver() {
    // Build: stream -> groupByKey -> windowedBy(size=10, grace=0)
    //   -> emit_strategy(on_window_close()) -> count -> toStream -> to("out")
    // Pipe records ts: a@1, a@4 (window [0,10)), then a@12 (window [10,20), closes [0,10)).
    // Assert "out" sees exactly ONE record for [0,10) with value 2 (no per-update emits).
    // (Use the same TopologyTestDriver setup pattern as the existing windowed-count
    //  DSL test in this crate; only `.emit_strategy(EmitStrategy::on_window_close())`
    //  is inserted before `.count(...)`.)
}
```
Fill the body by copying the nearest existing windowed-count driver test and inserting `.emit_strategy(crate::dsl::EmitStrategy::on_window_close())` before the terminal `count`. Assert one output record for `[0,10)` = 2.

- [ ] **Step 9: Run the time-window tests**

Run: `cargo test -p crabka-client-streams window_aggregate:: && cargo test -p crabka-client-streams time_window_emit_final`
Expected: PASS (new emit-final tests + unchanged emit-on-update regression test `windowed_count_tumbling_emits_per_window`).

- [ ] **Step 10: Commit**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/hardcore-rosalind-d53a17 add crates/client-streams/src/dsl/windowed_kgrouped.rs crates/client-streams/src/dsl/processors/window_aggregate.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/hardcore-rosalind-d53a17 -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(client-streams): emit-final for time windows (KIP-825)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: Session-window emit-final

**Files:**
- Modify: `crates/client-streams/src/dsl/session_windowed_kgrouped.rs` (struct ≈line 30-37; `new` ≈line 46; aggregate thunk ≈line 269-281; reduce thunk ≈line 355-365)
- Modify: `crates/client-streams/src/dsl/processors/session_aggregate.rs` (both processors; tests)

Session semantics: in `on_window_close` mode the processor still does its full merge/remove/put store maintenance, but suppresses BOTH the per-merge tombstone forward and the merged-session update forward; closed sessions (`end <= window_close_time`) are emitted by `emit_closed_sessions`. Stream-time = max observed `ts`; `window_close_time = stream_time - grace_ms` (session grace lives on `SessionWindows`; confirm the field name when editing — use the same grace source the existing suppress-factory path uses).

- [ ] **Step 1: Write the failing processor test** — append to `session_aggregate.rs` tests, driving `KStreamSessionAggregateProcessor` in close mode. Two records for key "a" at ts 0 and 4 (gap large enough to merge into one session `[0,4]`), assert nothing emits; then a record at ts 1000 (closes `[0,4]`), assert exactly one final for `[0,4]` with the merged count (2). Model the `Dispatch`/`ProcessorContext` setup on the existing session tests in this file, adding the new fields to the struct literal:

```rust
let mut proc = KStreamSessionAggregateProcessor {
    store_name: "s".into(),
    gap_ms: 10,
    init: || 0i64,
    agg: |_k: &String, _v: &String, a: i64| a + 1,
    merger: |_k: &String, a: i64, b: i64| a + b,
    emit: crate::dsl::emit::EmitStrategy::on_window_close(),
    stream_time: i64::MIN,
    last_emitted_close: i64::MIN,
    _pd: PhantomData::<fn() -> (String, String, i64)>,
};
```
Assertions: after the two in-gap records `buffer.is_empty()`; after the ts=1000 record, exactly one record with window `{start:0,end:4}` and `Change.new == Some(2)`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-client-streams session_aggregate::tests`
Expected: FAIL — missing fields.

- [ ] **Step 3: Add fields to both session processor structs** — add to `KStreamSessionAggregateProcessor` (after `merger`) and `KStreamSessionReduceProcessor` (after `reducer`):

```rust
    pub emit: crate::dsl::emit::EmitStrategy,
    pub stream_time: i64,
    pub last_emitted_close: i64,
```

- [ ] **Step 4: Gate the forwards + add close scan** — in `KStreamSessionAggregateProcessor::process`:
  - At the top, after computing `ts`: `self.stream_time = self.stream_time.max(ts); let window_close_time = self.stream_time - GRACE;` (use the session grace field).
  - Wrap the tombstone `ctx.forward(...)` (≈line 79-87) and the merged-session `ctx.forward(...)` (≈line 98-108) each in `if self.emit.is_on_update() { ... }`.
  - At the end of `process`, add:
    ```rust
    if self.emit.is_on_close() {
        self.emit_closed_sessions(ctx, window_close_time).await;
    }
    ```

- [ ] **Step 5: Add the `emit_closed_sessions` helper** — an inherent impl on `KStreamSessionAggregateProcessor` (bounds mirroring the trait impl plus `VA: Clone`):

```rust
    async fn emit_closed_sessions(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<VA>>,
        window_close_time: i64,
    ) {
        let mut due = {
            let store = ctx
                .get_session_store::<K, VA>(&self.store_name)
                .expect("session store not found");
            store.find_closed_sessions(window_close_time).await
        };
        due.retain(|(_, _, end, _)| *end > self.last_emitted_close);
        due.sort_by_key(|(_, start, end, _)| (*end, *start));
        for (k, start, end, v) in due {
            ctx.forward(Record::new(
                Some(Windowed { key: k, window: Window { start, end } }),
                Change::update(None, v),
                end,
            ));
        }
        self.last_emitted_close = window_close_time;
    }
```

- [ ] **Step 6: Mirror into `KStreamSessionReduceProcessor`** — add fields, gate both forwards, add the same close-scan tail + a matching `emit_closed_sessions` inherent impl with value type `V`.

- [ ] **Step 7: Thread `emit` through the session handle** — same shape as Task 4 Step 7: add `emit: EmitStrategy` field + `emit_strategy()` builder to `SessionWindowedKGroupedStream`, default `on_window_update()` in `new`, capture `let emit = self.emit;` in both lowering bodies, and set `emit, stream_time: i64::MIN, last_emitted_close: i64::MIN` in both `KStreamSession*Processor { .. }` literals.

- [ ] **Step 8: Run the session tests**

Run: `cargo test -p crabka-client-streams session_aggregate::`
Expected: PASS (new emit-final test + unchanged emit-on-update session tests).

- [ ] **Step 9: Commit**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/hardcore-rosalind-d53a17 add crates/client-streams/src/dsl/session_windowed_kgrouped.rs crates/client-streams/src/dsl/processors/session_aggregate.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/hardcore-rosalind-d53a17 -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(client-streams): emit-final for session windows (KIP-825)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 6: Sliding-window emit-final

**Files:**
- Modify: `crates/client-streams/src/dsl/sliding_windowed_kgrouped.rs` (struct ≈line 31-38; `new` ≈line 47; aggregate thunk ≈line 267; reduce thunk ≈line 360)
- Modify: `crates/client-streams/src/dsl/processors/sliding_window_aggregate.rs` (both processors; `process`/`process_early`/`process_normal`; tests)

Sliding already tracks `stream_time` + `close_time` and drops late records (≈line 75-85). The emit-final change: (a) add `emit` + `last_emitted_close` fields (keep the existing `stream_time`); (b) guard EVERY `ctx.forward` in `process_early` and `process_normal` with `if self.emit.is_on_update()`; (c) at the end of `process` (after the `process_early`/`process_normal` dispatch), call `emit_closed_windows`.

- [ ] **Step 1: Write the failing processor test** — append to `sliding_window_aggregate.rs` tests, driving the aggregate processor in close mode. Use `time_difference_ms` small (e.g. 10), grace 0. Feed records that build a window, assert no emits while open, then a far-future record to close, assert the final emits once. Add the new fields to the struct literal:

```rust
let mut proc = KStreamSlidingWindowAggregateProcessor {
    store_name: "w".into(),
    windows: SlidingWindows::of_time_difference(10), // match the actual constructor name
    init: || 0i64,
    agg: |_k: &String, _v: &String, a: i64| a + 1,
    stream_time: i64::MIN,
    emit: crate::dsl::emit::EmitStrategy::on_window_close(),
    last_emitted_close: i64::MIN,
    _pd: PhantomData::<fn() -> (String, String, i64)>,
};
```
(Confirm the `SlidingWindows` constructor + grace setter names from `dsl/windows.rs` when writing the test.) Assert: records within an open window emit nothing; a later record advancing stream-time past `window.end` closes it and exactly the expected finals emit (one per closed left/right window), each as `Change.new == Some(count)`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-client-streams sliding_window_aggregate::tests`
Expected: FAIL — missing fields `emit`/`last_emitted_close`.

- [ ] **Step 3: Add fields to both sliding processor structs** — add to `KStreamSlidingWindowAggregateProcessor` (after `stream_time`) and `KStreamSlidingWindowReduceProcessor`:

```rust
    pub emit: crate::dsl::emit::EmitStrategy,
    pub last_emitted_close: i64,
```

- [ ] **Step 4: Guard every update-forward** — in `sliding_window_aggregate.rs`, wrap each `ctx.forward(Record::new(...))` call inside `process_early` and `process_normal` (aggregate-processor sites at ≈line 177, 207, 237, 267; reduce-processor sites at ≈line 593, 623, 652, 685, 764, 794, 827 — verify exact sites by searching `ctx.forward(` in the file) with:

```rust
if self.emit.is_on_update() {
    ctx.forward(Record::new(/* ...unchanged args... */));
}
```
Leave the store `put`/update logic untouched — only the forwards are gated.

- [ ] **Step 5: Call the close scan from `process`** — in the `Processor::process` method (≈line 67-94), after the `if t < w { process_early } else { process_normal }` dispatch, add:

```rust
        if self.emit.is_on_close() {
            self.emit_closed_windows(ctx, close_time).await;
        }
```
(`close_time` is already computed at ≈line 77.)

- [ ] **Step 6: Add the `emit_closed_windows` helper** — an inherent method on `KStreamSlidingWindowAggregateProcessor` (reuse the existing inherent `impl` block at ≈line 97). Sliding window `end = start + time_difference_ms`:

```rust
    async fn emit_closed_windows(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<VA>>,
        window_close_time: i64,
    ) {
        let w = self.windows.time_difference_ms;
        let start_to = window_close_time - w;
        let start_from = self.last_emitted_close - w;
        let mut due = {
            let store = ctx
                .get_window_store::<K, VA>(&self.store_name)
                .expect("window store not found");
            store.fetch_all_in_range(start_from, start_to).await
        };
        due.retain(|(_, ws, _, _)| ws + w > self.last_emitted_close);
        due.sort_by_key(|(_, ws, _, _)| *ws);
        for (k, ws, ts, v) in due {
            ctx.forward(Record::new(
                Some(Windowed { key: k, window: Window { start: ws, end: ws + w } }),
                Change::update(None, v),
                ts,
            ));
        }
        self.last_emitted_close = window_close_time;
    }
```

- [ ] **Step 7: Mirror into `KStreamSlidingWindowReduceProcessor`** — add fields, guard its forwards, add the `emit_closed_windows` call in its `process`, add a matching inherent `emit_closed_windows` with value type `V`.

- [ ] **Step 8: Thread `emit` through the sliding handle** — same shape as Task 4 Step 7 on `SlidingWindowedKGroupedStream`: `emit` field + `emit_strategy()` builder, default in `new`, capture in both lowering bodies, set `emit, last_emitted_close: i64::MIN` (and the existing `stream_time: i64::MIN`) in both processor literals.

- [ ] **Step 9: Run the sliding tests**

Run: `cargo test -p crabka-client-streams sliding_window_aggregate::`
Expected: PASS (new emit-final test + unchanged emit-on-update sliding tests).

- [ ] **Step 10: Commit**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/hardcore-rosalind-d53a17 add crates/client-streams/src/dsl/sliding_windowed_kgrouped.rs crates/client-streams/src/dsl/processors/sliding_window_aggregate.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/hardcore-rosalind-d53a17 -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(client-streams): emit-final for sliding windows (KIP-825)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Batch 1 gate — workspace lint + full suite

- [ ] **Step 1: Format + clippy (the CI gate)**

Run:
```bash
cd /Users/mattstone/git/crabka/.claude/worktrees/hardcore-rosalind-d53a17/crates/client-streams
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
```
Expected: clean. Watch `clippy::pedantic` (workspace-wide warn → error): backtick any identifiers in new `///`/`//!` doc spans, and confirm no `large_futures`. If clippy auto-fixes anything, re-stage + amend.

- [ ] **Step 2: Full crate test run**

Run: `cargo test -p crabka-client-streams`
Expected: PASS. Erasure mismatches surface as runtime downcast panics, not compile errors — a green suite here is the real signal.

- [ ] **Step 3: Commit any fmt/clippy fixups** (only if Steps 1-2 changed files).

---

## Batch 2 — JVM behavioral goldens (parallel; separate fixtures)

These pin byte-exact output AND confirm the emit-final topology equals the emit-on-update topology (no extra node/store). Capture from `apache/kafka:4.1.0` Streams under `crates/client-streams/tests/jvm-capture/`, following the EXACT pattern of the existing windowed fixtures (the sliding-window goldens from #475 are the closest template — read one end-to-end before starting).

> **Capture host note (from project memory):** the Docker 4.1.0 capture harness is effectively Linux-bound; multi-broker JVM data paths don't work from Mac host procs. If capture can't run locally, generate the fixture JSON on Linux CI (or the existing capture workflow) and commit the artifact — do NOT hand-edit golden bytes.

> **`Change` shape confirmation:** the first golden that runs DECIDES whether emit-final forwards `Change(final, old=null)` or `Change(final, old=prior)`. If the capture shows a non-null `old`, change `Change::update(None, v)` → the captured shape in all three `emit_closed_*` helpers (Tasks 4-6) and re-run their unit tests before finalizing goldens.

### Task 7: Time-window emit-final golden

**Files:**
- Create: `crates/client-streams/tests/jvm-capture/emit_final_time_window.json` (or the dir/format the existing windowed fixtures use)
- Modify: the capture harness manifest + the Rust golden test list that replays fixtures (find it via the sliding fixture from #475)
- Modify: `.github/workflows/ci.yml` — add any NEW `tests/<x>.rs` file to the crate's `cargo llvm-cov --test <name>` list (else codecov/patch reports 0%)

- [ ] **Step 1:** Read an existing windowed golden fixture + its replay test end-to-end; note the topology JSON and the output-records JSON shape.
- [ ] **Step 2:** Author/capture the JVM program: `stream → groupByKey → windowedBy(TimeWindows.ofSizeWithNoGrace(...)) → emitStrategy(EmitStrategy.onWindowClose()) → count() → toStream → to(out)`. Capture topology + output for the same input the unit test uses.
- [ ] **Step 3:** Add the Rust replay test asserting crabka's `TopologyTestDriver` output matches the fixture byte-for-byte, and asserting the topology equals the emit-on-update topology except for runtime behavior.
- [ ] **Step 4:** Run: `cargo test -p crabka-client-streams emit_final_time_window` → PASS.
- [ ] **Step 5:** Commit (message: `test(client-streams): emit-final time-window JVM golden (KIP-825)`).

### Task 8: Session-window emit-final golden

**Files:** as Task 7 with `emit_final_session_window` (program uses `SessionWindows` + `emitStrategy(onWindowClose())` + `count()`; exercise a merge-then-close input).

- [ ] **Step 1-5:** Same five steps as Task 7, session variant. Run: `cargo test -p crabka-client-streams emit_final_session_window` → PASS. Commit (`test(client-streams): emit-final session-window JVM golden (KIP-825)`).

### Task 9: Sliding-window emit-final golden

**Files:** as Task 7 with `emit_final_sliding_window` (program uses `SlidingWindows.ofTimeDifferenceWithNoGrace(...)` + `emitStrategy(onWindowClose())` + `aggregate(...)`; exercise left/right window close).

- [ ] **Step 1-5:** Same five steps as Task 7, sliding variant. Run: `cargo test -p crabka-client-streams emit_final_sliding_window` → PASS. Commit (`test(client-streams): emit-final sliding-window JVM golden (KIP-825)`).

---

## Batch 2 gate — final verification

- [ ] **Step 1:** `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings` → clean.
- [ ] **Step 2:** `cargo test -p crabka-client-streams` → PASS (all unit + golden tests).
- [ ] **Step 3:** Confirm any new `tests/*.rs` files are in the `ci.yml` llvm-cov `--test` list for the crate (codecov/patch is PR-only + non-blocking, but the entry must exist or it reports 0%).
- [ ] **Step 4:** Use `superpowers:finishing-a-development-branch` to open the PR (conventional title: `feat(client-streams): native emit-final / EmitStrategy.onWindowClose (KIP-825)`).

---

## Self-review notes (coverage map)

- Spec §"Components" 1 (EmitStrategy) → Task 1.
- Spec §"Components" 4 (store scan) → Tasks 2 (window) + 3 (session).
- Spec §"Components" 2-3 (handle builder + processor branch) → Tasks 4/5/6 Steps 3-8.
- Spec §"Emit semantics" (Change shape, late-drop, no end-of-stream flush) → Tasks 4-6 close-scan + the Batch-2 `Change`-shape confirmation note.
- Spec §"Window-type specifics" → Task 4 (time), Task 5 (session merge-then-close), Task 6 (sliding left/right).
- Spec §"Edge cases" (watermark not persisted) → encoded as the processor-local `last_emitted_close` (no changelog persistence), documented in code comments.
- Spec §"Testing" → unit tests in Tasks 4-6; JVM goldens in Tasks 7-9; CI/coverage in the Batch gates.
