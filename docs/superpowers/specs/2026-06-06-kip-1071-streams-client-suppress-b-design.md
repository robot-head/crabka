# KIP-1071 Streams Client — Suppress Slice B: bounded `BufferConfig` (`maxRecords` + shutDownWhenFull)

**Status:** design approved (2026-06-06)
**Branch:** `streams-suppress-b` — stacks on `streams-suppress-a` (PR #405). Rebase
onto `main` when #405 merges (the established stacked-slice cadence).
**Worktree:** `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`
**Ground truth:** Docker JVM Kafka-Streams 4.1.0 (no new golden in this slice).

The second slice of the suppress program (A → **B** → C → D). It bounds the
suppress buffer with a **record cap**: `Suppressed.untilWindowCloses(unbounded()
.withMaxRecords(n))`. When the buffer exceeds the cap (after emitting any
closed windows), the task **shuts down** (the JVM `StrictBufferConfig` /
`shutDownWhenFull` semantics).

Per `CLAUDE.md`: greenfield (no compat shims); match Kafka semantics.

## 1. Scope (decided)

- **In:** `BufferConfig::with_max_records(n)` (the JVM strict path
  `unbounded().withMaxRecords(n)`); the record-cap enforcement in the suppress
  processor; `shutDownWhenFull` via **panic** when the cap is exceeded.
- **Out (later slices):** `maxBytes` — deferred to slice **D**, where the buffer
  changelog already introduces the key/value serdes that byte-accounting needs.
  `emitEarlyWhenFull` — deferred to slice **C**, since the JVM only accepts it with
  `untilTimeLimitElapsed` (`untilWindowCloses` requires a `StrictBufferConfig`).
- **No golden change:** the suppress config is not wire-visible (slice A's golden
  is logging-disabled), so the 13 DSL golden frames stay byte-identical. Validation
  is execution (`#[should_panic]`).

## 2. Architecture

A minimal extension of slice A — no new files, no serdes, no new processor. The
record count is the buffer's existing `len()`. Three changes:
1. `BufferConfig` carries an optional record cap.
2. The `KTableSuppressProcessor` takes the cap and enforces it after close-eviction.
3. `KTable::suppress` threads the cap from the config into the processor.

## 3. `BufferConfig` (`dsl/suppress.rs`)

Replace the `_private: ()` marker with a private `max_records`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferConfig {
    max_records: Option<usize>,
}

impl BufferConfig {
    /// Unbounded in-memory buffer (no record cap).
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

`BufferConfig` stays `Copy` (the field is `Option<usize>`). External construction
stays blocked (the field is private; the only constructors are `unbounded()` +
`with_max_records`). `Suppressed::until_window_closes(BufferConfig)` is unchanged.

Note on the JVM API: `BufferConfig.maxRecords(n)` (the **static** form) returns an
*eager* config (emit-early default), which the JVM rejects for `untilWindowCloses`.
Our DSL exposes only the strict path (`unbounded().with_max_records(n)`), so we do
not provide a standalone `max_records(n)` constructor in this slice — the eager
config belongs to slice C's `until_time_limit`.

## 4. `KTableSuppressProcessor` (`dsl/processors/suppress.rs`)

Add a `max_records: Option<usize>` field; `new(grace_ms, max_records)`. `process()`
gains a post-eviction cap check:

```rust
async fn process(&mut self, ctx, r) {
    let key = r.key.expect("suppress requires a non-null key");
    self.observed_stream_time = self.observed_stream_time.max(r.timestamp);
    let buffer_time = key.window.end;
    self.buffer.put(key, buffer_time, r.value, r.timestamp);

    let threshold = self.observed_stream_time - self.grace_ms;
    for (k, change, rts) in self.buffer.evict_while(threshold) {
        ctx.forward(Record::new(Some(k), change, rts));
    }

    // shutDownWhenFull: the cap is checked AFTER close-eviction, so windows that
    // buffer-then-immediately-close don't count — only genuinely-open buffered
    // windows do. Over capacity → fatal (the JVM throws StreamsException + the
    // thread dies; panic is the Rust analog).
    if let Some(cap) = self.max_records {
        assert!(
            self.buffer.len() <= cap,
            "suppress buffer exceeded its max capacity of {cap} records (shutDownWhenFull)"
        );
    }
}
```

`buffer.len()` (added in slice A) is the record count — no serdes needed. The check
is an `assert!` so over-capacity panics with a clear message; the panic propagates
through the runtime (crashing the task) or the `TopologyTestDriver` (to the test).

## 5. DSL wiring (`dsl/ktable.rs`)

`KTable::suppress` reads the cap from the config and passes it to the processor.
In the existing lowering thunk, change the supplier closure from
`KTableSuppressProcessor::new(grace_ms)` to
`KTableSuppressProcessor::new(grace_ms, max_records)`, where
`let max_records = suppressed.buffer.max_records();` is captured before the thunk
(alongside `grace_ms`). No other lowering changes — the topology is unchanged.

## 6. Tests

- **Config unit test** (`suppress.rs` tests): `unbounded().with_max_records(3)`
  round-trips; `unbounded()` has `max_records() == None`.
- **Processor unit test** (`#[should_panic(expected = "max capacity")]`): a
  processor with `max_records = Some(2)`, grace 0; pipe three records for three
  distinct keys into the **same open** window `[0,10)` (timestamps 1, 2, 3, all
  `< 10` so none close) → the third `put` brings `len()` to 3 > 2 → panic. Also a
  **non-panicking** test: `max_records = Some(2)`, two keys buffered (`len == cap`,
  not over) → no panic, and once their window closes both emit normally.
- **Execution test** (`dsl_execution.rs`, `#[should_panic(expected = "max capacity")]`):
  the same scenario end-to-end via `TopologyTestDriver` —
  `windowed_by(TimeWindows 60s).count().suppress(until_window_closes(unbounded()
  .with_max_records(2)))`, pipe three distinct keys into one window → panic on the
  third. Plus the existing slice-A execution tests (unbounded) stay green.
- **No golden change** — `cargo test --test dsl_golden_frame` stays `13 passed`,
  byte-identical.

## 7. Phasing

Small — two tasks, no per-batch file overlap:
- **T1:** `BufferConfig` record cap (`dsl/suppress.rs`) + the processor cap field +
  enforcement + config/processor unit tests (`dsl/processors/suppress.rs`).
- **T2:** `KTable::suppress` wiring (`dsl/ktable.rs`) + the execution test
  (`dsl_execution.rs`) + lib-doc note + final verification (full suite, 13 goldens,
  clippy `--all-targets`, fmt).

## 8. Risks / open items

- **Panic as `shutDownWhenFull`** — `Processor::process` has no `Result` channel, so
  a fatal buffer-overflow panics (the Rust analog of the JVM's fatal throw). A typed
  error channel would be a cross-cutting trait change, out of scope. Documented.
- **Cap-check ordering** — checked *after* close-eviction (a record that
  immediately closes doesn't count against the cap), matching the JVM's
  `enforceConstraints`-after-evict order.
- **No `maxBytes` / `emitEarlyWhenFull`** — deferred to D / C respectively, per the
  JVM type constraints; the `BufferConfig` shape (private `Option` fields + builder)
  extends to both without breaking this slice's API.
