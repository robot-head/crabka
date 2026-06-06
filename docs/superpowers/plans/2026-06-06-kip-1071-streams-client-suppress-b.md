# Suppress Slice B — bounded `BufferConfig` (`maxRecords` + shutDownWhenFull) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound the suppress buffer with `Suppressed.untilWindowCloses(unbounded().with_max_records(n))`; exceeding the cap shuts the task down (`shutDownWhenFull` via panic).

**Architecture:** A minimal extension of slice A — `BufferConfig` carries an optional record cap, the `KTableSuppressProcessor` enforces it after close-eviction (panic on overflow), and `KTable::suppress` threads the cap. No new files, no serdes, no golden change.

**Tech Stack:** Rust; reuses slice A's `TimeOrderedKeyValueBuffer` (its `len()` is the record count).

**Branch / worktree:** `streams-suppress-b` (stacked on `streams-suppress-a` / PR #405) in `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`. Spec: `docs/superpowers/specs/2026-06-06-kip-1071-streams-client-suppress-b-design.md`.

**Git discipline:** all git via `git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl …`; assert branch `== streams-suppress-b` before each commit; commit `-c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com"`; never `git config`; no push.

---

## File Structure

**Modified files only (no new files):**
- `src/dsl/suppress.rs` — `BufferConfig.max_records` field + `with_max_records(n)` + `max_records()` getter; config unit test.
- `src/dsl/processors/suppress.rs` — `max_records` field on the processor + `new(grace_ms, max_records)` + the post-eviction cap check; processor unit tests.
- `src/dsl/ktable.rs` — `suppress` threads the cap into the processor supplier.
- `tests/dsl_execution.rs` — the `#[should_panic]` execution test.
- `src/lib.rs` — one-line doc note on the cap (optional, in T2).

## Execution batches

Sequential (T2 depends on T1's API):
- **T1:** `BufferConfig` cap + processor cap field/enforcement + unit tests.
- **T2:** `KTable::suppress` wiring + execution test + lib note + final verification.

---

## Task 1: `BufferConfig` record cap + processor enforcement

**Files:**
- Modify: `crates/client-streams/src/dsl/suppress.rs`
- Modify: `crates/client-streams/src/dsl/processors/suppress.rs`

- [ ] **Step 1: Grow `BufferConfig`** in `src/dsl/suppress.rs`. Replace the struct + impl:

```rust
/// How the suppress buffer is bounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferConfig {
    max_records: Option<usize>,
}

impl BufferConfig {
    /// An unbounded in-memory buffer (no record cap).
    #[must_use]
    pub fn unbounded() -> Self {
        Self { max_records: None }
    }

    /// Cap the buffer at `n` records. Exceeding the cap shuts the task down
    /// (`shutDownWhenFull`). JVM strict path: `unbounded().withMaxRecords(n)`.
    #[must_use]
    pub fn with_max_records(self, n: usize) -> Self {
        assert!(n >= 1, "max_records must be >= 1");
        Self { max_records: Some(n) }
    }

    /// The record cap, if set (read by the suppress lowering).
    pub(crate) fn max_records(&self) -> Option<usize> {
        self.max_records
    }
}
```

Update the module doc comment's "Slice A: unbounded only" / "Slice B grows it" line to note `with_max_records` now exists. Keep `Suppressed` unchanged.

- [ ] **Step 2: Update the config unit test** in `suppress.rs` `mod tests` — replace `constructors` with:

```rust
    #[test]
    fn buffer_config_record_cap() {
        assert_eq!(BufferConfig::unbounded().max_records(), None);
        assert_eq!(
            BufferConfig::unbounded().with_max_records(3).max_records(),
            Some(3)
        );
        let s = Suppressed::until_window_closes(BufferConfig::unbounded().with_max_records(5));
        assert_eq!(s.buffer.max_records(), Some(5));
    }
```

- [ ] **Step 3: Add the cap to the processor** in `src/dsl/processors/suppress.rs`. Add the field to the struct + update `new`:

```rust
pub(crate) struct KTableSuppressProcessor<KInner, V> {
    pub buffer: TimeOrderedKeyValueBuffer<Windowed<KInner>, Change<V>>,
    pub observed_stream_time: i64,
    pub grace_ms: i64,
    pub max_records: Option<usize>,
    pub _pd: Marker<(KInner, V)>,
}

impl<KInner, V> KTableSuppressProcessor<KInner, V>
where
    KInner: Eq + std::hash::Hash + Clone,
{
    pub(crate) fn new(grace_ms: i64, max_records: Option<usize>) -> Self {
        Self {
            buffer: TimeOrderedKeyValueBuffer::new(),
            observed_stream_time: i64::MIN,
            grace_ms,
            max_records,
            _pd: PhantomData,
        }
    }
}
```

- [ ] **Step 4: Enforce the cap** in `process()` — after the `evict_while` forward loop, before the end of `process`, add:

```rust
        // shutDownWhenFull: checked AFTER close-eviction, so windows that
        // buffer-then-immediately-close don't count — only genuinely-open buffered
        // windows do. Over capacity → fatal (the JVM throws StreamsException + the
        // thread dies; panic is the Rust analog).
        if let Some(cap) = self.max_records {
            assert!(
                self.buffer.len() <= cap,
                "suppress buffer exceeded its max capacity of {cap} records (shutDownWhenFull)"
            );
        }
```

- [ ] **Step 5: Update the processor unit tests.** The existing `new(0)` calls become `new(0, None)`. Add two new tests in the `mod tests` of `suppress.rs`:

```rust
    #[tokio::test]
    #[should_panic(expected = "max capacity")]
    async fn exceeding_max_records_shuts_down() {
        let mut stores = StoreRegistry::default();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext { topic: "in".into(), partition: 0, offset: 0, timestamp: 0 };
        let mut proc = KTableSuppressProcessor::<String, i64>::new(0, Some(2)); // cap 2
        // Three distinct keys in the SAME open window [0,10) (ts < 10 → none close).
        for (k, ts) in [("a", 1i64), ("b", 2), ("c", 3)] {
            let mut d = Dispatch { buffer: &mut buffer, children: &children, output: &mut output, record_ctx: &rc, stores: &mut stores };
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            // the third put brings len() to 3 > cap 2 → panic
            proc.process(&mut ctx, Record::new(Some(windowed(k, 0, 10)), Change::update(None, 1), ts)).await;
        }
    }

    #[tokio::test]
    async fn at_capacity_does_not_panic_and_closes_normally() {
        let mut stores = StoreRegistry::default();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext { topic: "in".into(), partition: 0, offset: 0, timestamp: 0 };
        let mut proc = KTableSuppressProcessor::<String, i64>::new(0, Some(2)); // cap 2
        // Two keys in [0,10): len == cap, not over → no panic.
        for (k, ts) in [("a", 1i64), ("b", 2)] {
            let mut d = Dispatch { buffer: &mut buffer, children: &children, output: &mut output, record_ctx: &rc, stores: &mut stores };
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some(windowed(k, 0, 10)), Change::update(None, 1), ts)).await;
        }
        assert!(buffer.is_empty()); // nothing closed yet
        // A record in window [10,20) at ts=15 closes [0,10): the close-eviction runs
        // BEFORE the cap check, so len drops to 1 (the new window) → no panic, and
        // both [0,10) entries emit.
        {
            let mut d = Dispatch { buffer: &mut buffer, children: &children, output: &mut output, record_ctx: &rc, stores: &mut stores };
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some(windowed("z", 10, 20)), Change::update(None, 1), 15)).await;
        }
        assert_eq!(buffer.len(), 2); // a@[0,10] and b@[0,10] emitted
    }
```

(The existing slice-A tests `buffers_until_window_closes_then_emits_once` and `grace_delays_close` need their `new(0)` / `new(5)` calls updated to `new(0, None)` / `new(5, None)`. The `windowed(...)` test helper already exists in this module.)

- [ ] **Step 6: Run + verify.** Run:
```
cargo test -p crabka-client-streams --lib suppress
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
cargo fmt -p crabka-client-streams
```
Expected: config + processor tests PASS (incl. the `#[should_panic]` one and the at-capacity one); clippy clean.

- [ ] **Step 7: Commit.**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl add crates/client-streams/src/dsl/suppress.rs crates/client-streams/src/dsl/processors/suppress.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-dsl): suppress BufferConfig.with_max_records + shutDownWhenFull (panic)"
```

---

## Task 2: `KTable::suppress` wiring + execution test + verification

**Files:**
- Modify: `crates/client-streams/src/dsl/ktable.rs`
- Modify: `crates/client-streams/tests/dsl_execution.rs`
- Modify: `crates/client-streams/src/lib.rs` (doc note)

- [ ] **Step 1: Thread the cap** in `ktable.rs` `suppress`. The method currently takes `_suppressed: Suppressed` (unused) and captures `let grace_ms = self.window_grace_ms.unwrap_or(0);`. Change:
  - rename the param `_suppressed` → `suppressed`,
  - add `let max_records = suppressed.buffer.max_records();` next to the `grace_ms` capture,
  - change the supplier closure `move || KTableSuppressProcessor::<KInner, V>::new(grace_ms)` to `move || KTableSuppressProcessor::<KInner, V>::new(grace_ms, max_records)`.

(`suppressed.buffer` is `pub(crate)`; `BufferConfig::max_records()` is `pub(crate)` — both in-crate accessible from `ktable.rs`.)

- [ ] **Step 2: Add the execution test** to `tests/dsl_execution.rs`:

```rust
/// Suppress with a record cap: exceeding `maxRecords` shuts the task down
/// (shutDownWhenFull). Three distinct keys land in one still-open window
/// [0,60000) with a cap of 2 → the third overflows → panic.
#[test]
#[should_panic(expected = "max capacity")]
fn dsl_suppress_max_records_shuts_down_when_full() {
    use crabka_client_streams::{BufferConfig, I64Serde, Suppressed, TimeWindows};
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .group_by_key(Grouped::with(StringSerde, StringSerde))
        .windowed_by(TimeWindows::of_size(60_000))
        .count(Materialized::with(StringSerde, I64Serde))
        .suppress(Suppressed::until_window_closes(
            BufferConfig::unbounded().with_max_records(2),
        ))
        .to_stream()
        .to(
            "out",
            Produced::with(
                crabka_client_streams::TimeWindowedSerde::new(StringSerde, 60_000),
                I64Serde,
            ),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // Three distinct keys in window [0,60000) (ts < 60000 → none close) → the third
    // brings the buffer to 3 > cap 2 → panic.
    for (k, ts) in [("a", 1_000i64), ("b", 2_000), ("c", 3_000)] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some(k.to_string()),
            "x".to_string(),
            ts,
        );
    }
}
```

> **Implementer note:** adapt to this file's real `pipe_input`/`read_output` signatures (read an existing suppress/windowed test). The `#[should_panic(expected = "max capacity")]` is the contract — the third `pipe_input` triggers the processor panic, which propagates through the driver.

- [ ] **Step 2b: Run.** `cargo test -p crabka-client-streams --test dsl_execution suppress` → the new `#[should_panic]` test + the existing slice-A suppress tests PASS.

- [ ] **Step 3: lib doc note.** In `src/lib.rs`, extend the existing suppress paragraph (after "…later slices.") with one sentence:

```rust
//! `BufferConfig::unbounded().with_max_records(n)` caps the buffer; exceeding the
//! cap shuts the task down (`shutDownWhenFull`). `maxBytes` is a later slice.
```

- [ ] **Step 4: Final verification.** Run, in order:
```
cargo test -p crabka-client-streams
cargo test -p crabka-client-streams --doc
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
cargo fmt -p crabka-client-streams --check
```
Expected: all green; **`dsl_golden_frame` stays `13 passed` byte-identical** (no topology change); `dsl_execution` includes the new `#[should_panic]` test; clippy + fmt clean.

- [ ] **Step 5: Commit.**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl add crates/client-streams/src/dsl/ktable.rs crates/client-streams/tests/dsl_execution.rs crates/client-streams/src/lib.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-dsl): KTable::suppress threads maxRecords cap + execution test"
```

---

## Done criteria

- `Suppressed::until_window_closes(BufferConfig::unbounded().with_max_records(n))` caps the suppress buffer; exceeding `n` (after close-eviction) panics (`shutDownWhenFull`).
- Cap checked after close-eviction (immediate-close windows don't count) — validated by the at-capacity non-panicking test.
- 13 goldens byte-identical (no topology change); full suite + doctests + clippy `--all-targets` + fmt green.
