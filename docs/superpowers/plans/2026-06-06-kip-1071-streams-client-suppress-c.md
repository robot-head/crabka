# Suppress Slice C — `untilTimeLimitElapsed` + `emitEarlyWhenFull` (fn-pointer unification) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `Suppressed.untilTimeLimitElapsed(wait, BufferConfig)` (rate-limiter for any KTable) + `emitEarlyWhenFull` overflow, by unifying slice A/B's window-close `suppress` with the rate-limiter behind a `fn(&K,i64)->i64` buffer-time pointer.

**Architecture:** One generic `KTable<K,V>::suppress(Suppressed<K>)` + one generic `KTableSuppressProcessor<K,V>`. `Suppressed<K>` carries a Copy `fn(&K,i64)->i64` (window-close reads `window.end`; time-limit reads record ts) + a `WaitKind` (upstream grace vs fixed duration). `BufferConfig` gains an overflow mode (strict shutdown vs eager emit-early).

**Tech Stack:** Rust; reuses slice A/B's `TimeOrderedKeyValueBuffer` + `Change`.

**Branch / worktree:** `streams-suppress-c` (stacked on `streams-suppress-b` / PR #408) in `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`. Spec: `docs/superpowers/specs/2026-06-06-kip-1071-streams-client-suppress-c-design.md`.

**Git discipline:** all git via `git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl …`; assert branch `== streams-suppress-c` before each commit; commit `-c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com"`; never `git config`; no push.

---

## File Structure (modified only — no new files)

- `src/dsl/suppress.rs` — `BufferConfig` overflow mode + eager `max_records(n)` + toggles + `record_cap()`/`is_emit_early()`; `Suppressed<K>` (fn-pointer + `WaitKind` + `until_time_limit` + refactored `until_window_closes`).
- `src/dsl/processors/suppress_buffer.rs` — `evict_oldest`.
- `src/dsl/processors/suppress.rs` — generalize the processor `<KInner,V>` → `<K,V>` (buffer_time/wait_ms/emit_early); migrate its tests.
- `src/dsl/ktable.rs` — move `suppress` to the general `impl<K,V> KTable<K,V>`.
- `tests/dsl_execution.rs` — time-limit + emit-early + window-close-eager-panic tests.
- `src/lib.rs` — doc note.

## Execution batches (sequential — the unification is one compile unit)

- **T1:** `BufferConfig` overflow mode + buffer `evict_oldest` + unit tests.
- **T2 (the unification):** `Suppressed<K>` + processor generalization + `KTable::suppress` on the general impl + migrate the processor tests. **Needs T1.**
- **T3:** execution tests + lib note + final verification. **Needs T2.**

---

## Task 1: `BufferConfig` overflow mode + buffer `evict_oldest`

**Files:**
- Modify: `crates/client-streams/src/dsl/suppress.rs`
- Modify: `crates/client-streams/src/dsl/processors/suppress_buffer.rs`

- [ ] **Step 1: Replace `BufferConfig`** in `src/dsl/suppress.rs` (keep `Suppressed`/`until_window_closes` for now — T2 refactors them):

```rust
/// How the suppress buffer is bounded + what happens when it's full.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferConfig {
    max_records: Option<usize>,
    /// `false` = shutDownWhenFull (strict, panic); `true` = emitEarlyWhenFull (eager).
    emit_early: bool,
}

impl BufferConfig {
    /// Unbounded, strict (shutDownWhenFull).
    #[must_use]
    pub fn unbounded() -> Self {
        Self { max_records: None, emit_early: false }
    }

    /// Cap at `n` records, EAGER (emit-early-when-full) — the JVM static
    /// `BufferConfig.maxRecords(n)` (the rate-limiter default overflow).
    #[must_use]
    pub fn max_records(n: usize) -> Self {
        assert!(n >= 1, "max_records must be >= 1");
        Self { max_records: Some(n), emit_early: true }
    }

    /// Cap at `n` records, keeping the current overflow mode (strict on the
    /// `unbounded()` path) — the JVM `unbounded().withMaxRecords(n)`.
    #[must_use]
    pub fn with_max_records(self, n: usize) -> Self {
        assert!(n >= 1, "max_records must be >= 1");
        Self { max_records: Some(n), ..self }
    }

    /// Evict + emit the oldest buffered record when full (eager).
    #[must_use]
    pub fn emit_early_when_full(self) -> Self {
        Self { emit_early: true, ..self }
    }

    /// Shut the task down when full (strict).
    #[must_use]
    pub fn shut_down_when_full(self) -> Self {
        Self { emit_early: false, ..self }
    }

    pub(crate) fn record_cap(&self) -> Option<usize> {
        self.max_records
    }
    pub(crate) fn is_emit_early(&self) -> bool {
        self.emit_early
    }
}
```

Update the module doc line to mention the overflow toggles. Update the config unit test (the slice-B `buffer_config_record_cap`) to:

```rust
    #[test]
    fn buffer_config_caps_and_overflow() {
        assert_eq!(BufferConfig::unbounded().record_cap(), None);
        assert!(!BufferConfig::unbounded().is_emit_early()); // strict
        let strict = BufferConfig::unbounded().with_max_records(3);
        assert_eq!(strict.record_cap(), Some(3));
        assert!(!strict.is_emit_early());
        let eager = BufferConfig::max_records(5); // eager
        assert_eq!(eager.record_cap(), Some(5));
        assert!(eager.is_emit_early());
        assert!(eager.shut_down_when_full().is_emit_early() == false);
        assert!(BufferConfig::unbounded().emit_early_when_full().is_emit_early());
    }
```

(`Suppressed`'s constructor still references `buffer` — leave the slice-B `Suppressed { buffer }` + `until_window_closes` as-is in this task; T2 replaces them. The `s.buffer.max_records()` call in the slice-B `constructors`/lowering will break — temporarily change the slice-B getter call to `record_cap()` wherever it appears so T1 compiles: `ktable.rs` `suppressed.buffer.max_records()` → `suppressed.buffer.record_cap()`, and the suppress.rs test. T2 fully rewires it.)

> **Note:** to keep T1 a clean compile, also update the two current `record_cap` consumers from the slice-B name `max_records()`: in `src/dsl/ktable.rs` (the `suppress` lowering) and any test. Search `\.buffer\.max_records()` and `\.max_records()` on a `BufferConfig` and rename to `record_cap()`.

- [ ] **Step 2: Add `evict_oldest`** to `src/dsl/processors/suppress_buffer.rs` (after `evict_while`):

```rust
    /// Pop and return the single lowest-`(buffer_time, seq)` entry (used by
    /// emit-early overflow). `None` if empty.
    pub(crate) fn evict_oldest(&mut self) -> Option<(K, V, i64)> {
        let (&slot, _) = self.entries.iter().next()?;
        let entry = self.entries.remove(&slot).expect("slot present");
        self.index.remove(&entry.key);
        Some((entry.key, entry.value, entry.record_ts))
    }
```

Add a buffer unit test:

```rust
    #[test]
    fn evict_oldest_pops_lowest_buffer_time() {
        let mut b = TimeOrderedKeyValueBuffer::<String, i64>::new();
        b.put("a".into(), 30, 1, 30);
        b.put("b".into(), 10, 2, 10);
        assert_eq!(b.evict_oldest(), Some(("b".into(), 2, 10))); // lowest buffer_time
        assert_eq!(b.evict_oldest(), Some(("a".into(), 1, 30)));
        assert_eq!(b.evict_oldest(), None);
    }
```

- [ ] **Step 3: Build + test.** `cd <worktree> && cargo build -p crabka-client-streams` (compiles after the `record_cap` rename) + `cargo test -p crabka-client-streams --lib "suppress"` (config + buffer tests pass) + `cargo clippy -p crabka-client-streams --all-targets -- -D warnings` + `cargo fmt -p crabka-client-streams`.

- [ ] **Step 4: Commit.**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl add crates/client-streams/src/dsl/suppress.rs crates/client-streams/src/dsl/processors/suppress_buffer.rs crates/client-streams/src/dsl/ktable.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-dsl): suppress BufferConfig overflow mode + eager max_records + buffer evict_oldest"
```

---

## Task 2: `Suppressed<K>` + processor generalization + `KTable::suppress` (the unification)

**Files:**
- Modify: `crates/client-streams/src/dsl/suppress.rs`, `src/dsl/processors/suppress.rs`, `src/dsl/ktable.rs`

This is one compile unit — the three files change together.

- [ ] **Step 1: `Suppressed<K>`** in `src/dsl/suppress.rs` — replace the slice-A/B `Suppressed { buffer }` + `until_window_closes`:

```rust
/// A suppression configuration, parameterized by the table key `K`. Carries a
/// `fn(&K, i64) -> i64` (record key + timestamp → buffer time): window-close reads
/// `window.end`, time-limit reads the record timestamp. Fn pointers are `Copy`, so
/// `Suppressed<K>` is `Copy`.
#[derive(Clone, Copy, Debug)]
pub struct Suppressed<K> {
    pub(crate) buffer: BufferConfig,
    pub(crate) buffer_time: fn(&K, i64) -> i64,
    pub(crate) wait: WaitKind,
}

/// How long to wait before emitting a buffered record.
#[derive(Clone, Copy, Debug)]
pub(crate) enum WaitKind {
    /// Window-close: wait = the upstream window's grace (from the KTable handle).
    UpstreamGrace,
    /// Time-limit: wait = the configured duration (ms).
    Fixed(i64),
}

impl<KInner> Suppressed<crate::dsl::windows::Windowed<KInner>> {
    /// Emit each window's final result once it closes (`stream_time >= window.end +
    /// grace`). Requires a windowed `KTable` + a STRICT buffer (shutDownWhenFull).
    #[must_use]
    pub fn until_window_closes(buffer: BufferConfig) -> Self {
        assert!(
            !buffer.is_emit_early(),
            "untilWindowCloses requires a strict (shutDownWhenFull) buffer config"
        );
        Self { buffer, buffer_time: |k, _ts| k.window.end, wait: WaitKind::UpstreamGrace }
    }
}

impl<K> Suppressed<K> {
    /// Rate-limiter: emit at most one update per key per `wait_ms` (stream-time); a
    /// newer record for a key replaces the buffered one and resets the timer.
    #[must_use]
    pub fn until_time_limit(wait_ms: i64, buffer: BufferConfig) -> Self {
        assert!(wait_ms >= 0, "time limit must be >= 0");
        Self { buffer, buffer_time: |_k, ts| ts, wait: WaitKind::Fixed(wait_ms) }
    }
}
```

Update the `constructors` test in `suppress.rs` to exercise both (`until_window_closes` + `until_time_limit`) — assert the `wait`/`buffer_time` produce expected values, e.g.:

```rust
    #[test]
    fn suppressed_constructors() {
        use crate::dsl::windows::{Window, Windowed};
        let wc = Suppressed::until_window_closes(BufferConfig::unbounded());
        let wk = Windowed { key: "k".to_string(), window: Window { start: 0, end: 99 } };
        assert_eq!((wc.buffer_time)(&wk, 5), 99); // window.end
        let tl = Suppressed::<String>::until_time_limit(50, BufferConfig::max_records(2));
        assert_eq!((tl.buffer_time)(&"k".to_string(), 5), 5); // record ts
    }
```

- [ ] **Step 2: Generalize the processor** in `src/dsl/processors/suppress.rs`. Replace the struct + `new` + `process`:

```rust
//! `KTableSuppressProcessor` — KIP suppression. Buffers `Change` updates and emits
//! each buffered record once stream-time passes `buffer_time(record) + wait_ms`.
//! Unifies `untilWindowCloses` (buffer_time = window.end, wait = grace) and
//! `untilTimeLimitElapsed` (buffer_time = record ts, wait = the duration) behind a
//! `fn(&K, i64) -> i64` buffer-time pointer.
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::dsl::processors::change::Change;
use crate::dsl::processors::suppress_buffer::TimeOrderedKeyValueBuffer;
use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;

type Marker<T> = PhantomData<fn() -> T>;

pub(crate) struct KTableSuppressProcessor<K, V> {
    pub buffer: TimeOrderedKeyValueBuffer<K, Change<V>>,
    pub observed_stream_time: i64,
    pub wait_ms: i64,
    pub buffer_time: fn(&K, i64) -> i64,
    pub max_records: Option<usize>,
    pub emit_early: bool,
    pub _pd: Marker<(K, V)>,
}

impl<K, V> KTableSuppressProcessor<K, V>
where
    K: Eq + std::hash::Hash + Clone,
{
    pub(crate) fn new(
        wait_ms: i64,
        buffer_time: fn(&K, i64) -> i64,
        max_records: Option<usize>,
        emit_early: bool,
    ) -> Self {
        Self {
            buffer: TimeOrderedKeyValueBuffer::new(),
            observed_stream_time: i64::MIN,
            wait_ms,
            buffer_time,
            max_records,
            emit_early,
            _pd: PhantomData,
        }
    }
}

#[async_trait]
impl<K, V> Processor<K, Change<V>, K, Change<V>> for KTableSuppressProcessor<K, V>
where
    K: std::any::Any + Send + Sync + Clone + Eq + std::hash::Hash,
    V: std::any::Any + Send + Clone,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, K, Change<V>>,
        r: Record<K, Change<V>>,
    ) {
        let key = r.key.expect("suppress requires a non-null key");
        self.observed_stream_time = self.observed_stream_time.max(r.timestamp);
        let bt = (self.buffer_time)(&key, r.timestamp);
        self.buffer.put(key, bt, r.value, r.timestamp);

        let threshold = self.observed_stream_time - self.wait_ms;
        for (k, change, rts) in self.buffer.evict_while(threshold) {
            ctx.forward(Record::new(Some(k), change, rts));
        }

        if let Some(cap) = self.max_records {
            if self.emit_early {
                // emitEarlyWhenFull: evict + emit the oldest until back within cap.
                while self.buffer.len() > cap {
                    let (k, change, rts) =
                        self.buffer.evict_oldest().expect("len > cap >= 1");
                    ctx.forward(Record::new(Some(k), change, rts));
                }
            } else {
                // shutDownWhenFull (slice B): fatal.
                assert!(
                    self.buffer.len() <= cap,
                    "suppress buffer exceeded its max capacity of {cap} records (shutDownWhenFull)"
                );
            }
        }
    }
}
```

- [ ] **Step 3: Migrate the processor tests** (still in `suppress.rs`). Add a window-close helper at the top of `mod tests` and re-point the existing tests:

```rust
    /// Construct a window-close processor (buffer_time = window.end, strict).
    fn window_close_proc(
        grace_ms: i64,
        max_records: Option<usize>,
    ) -> KTableSuppressProcessor<Windowed<String>, i64> {
        KTableSuppressProcessor::new(grace_ms, |k: &Windowed<String>, _ts| k.window.end, max_records, false)
    }
```

(Add `use crate::dsl::windows::Windowed;` to the test module if not already imported.) Replace the existing constructions:
- `KTableSuppressProcessor::<String, i64>::new(0, None)` → `window_close_proc(0, None)`
- `KTableSuppressProcessor::<String, i64>::new(5, None)` → `window_close_proc(5, None)`
- `KTableSuppressProcessor::<String, i64>::new(0, Some(2))` → `window_close_proc(0, Some(2))`

The bodies (`windowed(...)`, the assertions) are unchanged — the window-close behavior is preserved.

- [ ] **Step 4: Move `suppress` to the general impl** in `src/dsl/ktable.rs`. Change the impl block from `impl<KInner, V> KTable<Windowed<KInner>, V>` to the general `impl<K, V> KTable<K, V> where K: Any + Send + Sync + Clone + Eq + std::hash::Hash, V: Any + Send + Clone` (this impl block likely already exists for `map_values`/`filter`/etc. — if so, ADD `suppress` to it and DELETE the old specialized `impl<KInner,V> KTable<Windowed<KInner>,V>` block; ensure `K: Eq + Hash` is on whichever impl block holds `suppress`, adding a dedicated impl block if the existing general one lacks the `Eq + Hash` bound). The method:

```rust
    /// `suppress(Suppressed)`: buffer updates and emit on a delay. `until_window_closes`
    /// (windowed tables) emits each window's final value once it closes;
    /// `until_time_limit` rate-limits any table to one update per key per wait.
    #[must_use]
    pub fn suppress(&self, suppressed: crate::dsl::suppress::Suppressed<K>) -> KTable<K, V> {
        let wait_ms = match suppressed.wait {
            crate::dsl::suppress::WaitKind::UpstreamGrace => self.window_grace_ms.unwrap_or(0),
            crate::dsl::suppress::WaitKind::Fixed(ms) => ms,
        };
        let buffer_time = suppressed.buffer_time;
        let max_records = suppressed.buffer.record_cap();
        let emit_early = suppressed.buffer.is_emit_early();
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::KTABLE_SUPPRESS);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::TableProcessor { store_name: None },
            vec![parent_id],
        );
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, Change<V>>::from_name(state.handle_name[&parent_id].clone());
            let h = state
                .topology
                .add_processor::<K, Change<V>, K, Change<V>, _, _, _>(
                    name.clone(),
                    move || {
                        crate::dsl::processors::suppress::KTableSuppressProcessor::<K, V>::new(
                            wait_ms,
                            buffer_time,
                            max_records,
                            emit_early,
                        )
                    },
                    [parent],
                );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        KTable::new(Rc::clone(&self.builder), id, None, None).with_window_grace(self.window_grace_ms)
    }
```

(Match the existing imports in `ktable.rs` — `Any`, `Change`, `GraphNodeKind`, `LowerState`, `NodeHandle`, `names`, `Rc`. Remove the now-unused `Windowed` import + the old specialized impl block if it becomes empty. The returned table propagates the grace via `with_window_grace` so a suppress→suppress chain still closes windows.)

- [ ] **Step 5: Build + test.** `cargo build -p crabka-client-streams` + `cargo test -p crabka-client-streams --lib suppress` (config + processor tests, incl. the migrated window-close ones, PASS) + `cargo test -p crabka-client-streams --test dsl_golden_frame` (**13 passed**, byte-identical) + `cargo test -p crabka-client-streams --test dsl_execution suppress` (the slice-A/B suppress exec tests still PASS unchanged) + `cargo clippy -p crabka-client-streams --all-targets -- -D warnings` + `cargo fmt -p crabka-client-streams`.

- [ ] **Step 6: Commit.**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl add crates/client-streams/src/dsl/suppress.rs crates/client-streams/src/dsl/processors/suppress.rs crates/client-streams/src/dsl/ktable.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-dsl): unify suppress (fn-pointer buffer-time) + until_time_limit"
```

---

## Task 3: time-limit + emit-early execution tests + docs + verification

**Files:**
- Modify: `crates/client-streams/tests/dsl_execution.rs`, `src/lib.rs`

- [ ] **Step 1: Add the execution tests** to `tests/dsl_execution.rs`. Use a non-windowed `KTable` (e.g. `stream(...).to_table(Materialized)` or `…group_by_key().count()` un-windowed) as the suppress parent for the time-limit tests. Read an existing `to_table`/`count` execution test for the exact builder shape; the suppress contract is:

```rust
/// untilTimeLimitElapsed: a key is buffered and emitted once stream-time advances
/// past record_ts + wait; a newer record for the key replaces + resets the timer.
#[test]
fn dsl_suppress_until_time_limit_rate_limits() {
    use crabka_client_streams::{BufferConfig, I64Serde, Materialized, Suppressed};
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .group_by_key(Grouped::with(StringSerde, StringSerde))
        .count(Materialized::with(StringSerde, I64Serde))
        .suppress(Suppressed::until_time_limit(50, BufferConfig::unbounded()))
        .to_stream()
        .to("out", Produced::with(StringSerde, I64Serde));
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // "a"@10 → count 1, buffered (buffer_time 10, emits at 10+50=60).
    d.pipe_input("in", Consumed::with(StringSerde, StringSerde), Some("a".to_string()), "x".to_string(), 10);
    assert_eq!(d.read_output("out", Produced::with(StringSerde, I64Serde)), None);
    // "b"@100 advances stream-time to 100 ≥ 60 → "a" emits its final count (1).
    d.pipe_input("in", Consumed::with(StringSerde, StringSerde), Some("b".to_string()), "x".to_string(), 100);
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("a".into()), 1))
    );
}

/// emitEarlyWhenFull: an over-full eager buffer evicts + emits the oldest early
/// (no panic). cap 1, two keys → the first is emitted when the second arrives.
#[test]
fn dsl_suppress_emit_early_when_full_evicts_oldest() {
    use crabka_client_streams::{BufferConfig, I64Serde, Materialized, Suppressed};
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .group_by_key(Grouped::with(StringSerde, StringSerde))
        .count(Materialized::with(StringSerde, I64Serde))
        .suppress(Suppressed::until_time_limit(100_000, BufferConfig::max_records(1))) // eager cap 1
        .to_stream()
        .to("out", Produced::with(StringSerde, I64Serde));
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_input("in", Consumed::with(StringSerde, StringSerde), Some("a".to_string()), "x".to_string(), 1);
    // "b" overflows cap 1 → "a" evicted + emitted early (no panic), even though its
    // 100s time-limit hasn't elapsed.
    d.pipe_input("in", Consumed::with(StringSerde, StringSerde), Some("b".to_string()), "x".to_string(), 2);
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("a".into()), 1))
    );
}

/// untilWindowCloses requires a strict buffer — an eager config panics at construction.
#[test]
#[should_panic(expected = "strict")]
fn dsl_until_window_closes_rejects_eager_buffer() {
    use crabka_client_streams::{BufferConfig, Suppressed, Window, Windowed};
    let _ = Suppressed::<Windowed<String>>::until_window_closes(BufferConfig::max_records(2));
    let _ = Window { start: 0, end: 0 }; // keep the import used
}
```

> **Implementer note:** adapt builder/`pipe_input`/`read_output` to the real signatures (read existing tests). The contracts are: (1) time-limit buffers then emits on the wait elapsing; (2) eager over-cap evicts+emits the oldest with no panic; (3) `until_window_closes(eager)` panics. If `Window`/`Windowed` aren't both needed for the panic test, drop the unused one.

- [ ] **Step 2: Run.** `cargo test -p crabka-client-streams --test dsl_execution suppress` → the 3 new + existing suppress tests PASS.

- [ ] **Step 3: lib doc note.** Extend the `lib.rs` suppress paragraph:

```rust
//! [`Suppressed::until_time_limit`] is the rate-limiter variant for any table: it
//! emits at most one update per key per wait (stream-time), a newer record
//! resetting the timer. `BufferConfig::max_records(n)` (eager) +
//! [`BufferConfig::emit_early_when_full`] evict the oldest buffered record when
//! full instead of shutting down.
```

- [ ] **Step 4: Final verification.** Run:
```
cargo test -p crabka-client-streams
cargo test -p crabka-client-streams --doc
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
cargo fmt -p crabka-client-streams --check
```
Expected: all green; `dsl_golden_frame` `13 passed` byte-identical; `dsl_execution` includes the 3 new suppress tests; clippy + fmt clean.

- [ ] **Step 5: Commit.**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl add crates/client-streams/tests/dsl_execution.rs crates/client-streams/src/lib.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(streams-dsl): suppress until_time_limit + emit_early execution tests + docs"
```

---

## Done criteria

- `Suppressed::until_time_limit(wait, BufferConfig)` rate-limits any `KTable<K,V>` (K: Eq+Hash); a newer record per key resets the timer.
- `BufferConfig::max_records(n)` (eager) / `emit_early_when_full()` evict+emit the oldest when full; `unbounded().with_max_records(n)` / `shut_down_when_full()` panic. `until_window_closes` requires strict (panics on eager).
- Unified single `suppress(Suppressed<K>)` + one processor (fn-pointer buffer-time); existing window-close call-sites + behavior unchanged.
- 13 goldens byte-identical; full suite + doctests + clippy `--all-targets` + fmt green.
