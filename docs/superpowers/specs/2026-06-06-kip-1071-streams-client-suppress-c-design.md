# KIP-1071 Streams Client — Suppress Slice C: `untilTimeLimitElapsed` + `emitEarlyWhenFull`

**Status:** design approved (2026-06-06)
**Branch:** `streams-suppress-c` — stacks on `streams-suppress-b` (PR #408). Rebase
onto `main` when #408 merges (the established stacked-slice cadence).
**Worktree:** `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`
**Ground truth:** Docker JVM Kafka-Streams 4.1.0 (no new golden in this slice).

The third slice of the suppress program (A → B → **C** → D). It adds the
**rate-limiter** emit policy — `Suppressed.untilTimeLimitElapsed(duration, BufferConfig)`
— for *any* `KTable` (not just windowed), plus the `emitEarlyWhenFull` overflow
behavior. To make one `suppress` method serve both policies despite the key-type
asymmetry (window-close needs `window.end` from the key; time-limit works on any
key), it **unifies** slice A/B's window-close path and the rate-limiter behind a
`fn(&K, i64) -> i64` buffer-time function pointer.

Per `CLAUDE.md`: greenfield (no compat shims); match Kafka semantics.

## 1. Scope (decided)

- **In:** `Suppressed::until_time_limit(wait_ms, BufferConfig)` (rate-limiter, any
  key); the `fn`-pointer unification of `suppress` + the processor; `BufferConfig`
  overflow mode (`shut_down_when_full` strict / `emit_early_when_full` eager) + the
  eager static `BufferConfig::max_records(n)` constructor; the buffer's
  `evict_oldest`.
- **Out (later slice):** `maxBytes` — stays in slice **D** (with the changelog
  serdes); applies to both policies there.
- **Behavior-preserving:** the existing `suppress(until_window_closes(...))`
  call-sites and behavior are unchanged by the refactor.
- **No golden change:** the suppress config is not wire-visible; the 13 DSL golden
  frames stay byte-identical. Validation is execution.

## 2. Architecture — the unification

The key insight: a window closes at `window.end + grace`; a rate-limit elapses at
`record_ts + wait`. Both are *"emit a buffered record once stream-time passes
`buffer_time + wait`"* — the only differences are **how `buffer_time` is derived
from the record** and **what `wait` is**. So:

- `buffer_time` is a **`fn(&K, i64) -> i64`** (record key + timestamp → buffer time).
  Function pointers are `Copy`, so they thread through the `Copy` `Suppressed<K>`
  and into the per-task processor supplier with no Clone/Box friction.
  - window-close: `|k, _ts| k.window.end` — typed `fn(&Windowed<KInner>, i64) -> i64`.
  - time-limit: `|_k, ts| ts` — works for any `K`.
- `wait` is `UpstreamGrace` (window-close, read from the KTable handle's
  `window_grace_ms`) or `Fixed(ms)` (time-limit, the duration).

One generic `KTable<K,V>::suppress(Suppressed<K>)` + one generic
`KTableSuppressProcessor<K,V>` then serve both. `until_window_closes()` only
type-resolves when `K = Windowed<KInner>` (the closure reads `window.end`), so
calling it on a non-windowed table is a compile error — preserving slice A's
compile-time guarantee while collapsing to a single method.

## 3. `BufferConfig` — overflow mode (`dsl/suppress.rs`)

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferConfig {
    max_records: Option<usize>,
    emit_early: bool, // false = shutDownWhenFull (strict); true = emitEarlyWhenFull (eager)
}

impl BufferConfig {
    /// Unbounded, strict.
    pub fn unbounded() -> Self { Self { max_records: None, emit_early: false } }

    /// Cap at `n` records, EAGER (emit-early-when-full) — the JVM static
    /// `BufferConfig.maxRecords(n)`. For the rate-limiter's default overflow.
    pub fn max_records(n: usize) -> Self {
        assert!(n >= 1, "max_records must be >= 1");
        Self { max_records: Some(n), emit_early: true }
    }

    /// Cap at `n` records, keeping the current overflow mode (strict on the
    /// `unbounded()` path). The JVM `unbounded().withMaxRecords(n)`.
    pub fn with_max_records(self, n: usize) -> Self {
        assert!(n >= 1, "max_records must be >= 1");
        Self { max_records: Some(n), ..self }
    }

    /// Evict + emit the oldest buffered record when full (eager).
    pub fn emit_early_when_full(self) -> Self { Self { emit_early: true, ..self } }
    /// Shut the task down when full (strict).
    pub fn shut_down_when_full(self) -> Self { Self { emit_early: false, ..self } }

    pub(crate) fn record_cap(&self) -> Option<usize> { self.max_records }
    pub(crate) fn is_emit_early(&self) -> bool { self.emit_early }
}
```

(The slice-B `pub(crate) fn max_records(&self)` getter is renamed `record_cap()` —
the name now belongs to the eager static constructor.) `BufferConfig` stays `Copy`.

## 4. Buffer `evict_oldest` (`dsl/processors/suppress_buffer.rs`)

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

## 5. `Suppressed<K>` (`dsl/suppress.rs`)

```rust
#[derive(Clone, Copy, Debug)]
pub struct Suppressed<K> {
    pub(crate) buffer: BufferConfig,
    pub(crate) buffer_time: fn(&K, i64) -> i64,
    pub(crate) wait: WaitKind,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum WaitKind {
    UpstreamGrace,   // window-close: wait = the upstream window's grace
    Fixed(i64),      // time-limit: wait = the configured duration (ms)
}

impl<KInner> Suppressed<crate::dsl::windows::Windowed<KInner>> {
    /// Emit each window's final result once it closes (`stream_time >= window.end +
    /// grace`). Requires a windowed KTable + a STRICT buffer (shutDownWhenFull).
    pub fn until_window_closes(buffer: BufferConfig) -> Self {
        assert!(
            !buffer.is_emit_early(),
            "untilWindowCloses requires a strict (shutDownWhenFull) buffer config"
        );
        Self { buffer, buffer_time: |k, _ts| k.window.end, wait: WaitKind::UpstreamGrace }
    }
}

impl<K> Suppressed<K> {
    /// Rate-limiter: emit at most one update per key per `wait_ms` (stream-time);
    /// a newer record for a key replaces the buffered one and resets the timer.
    pub fn until_time_limit(wait_ms: i64, buffer: BufferConfig) -> Self {
        assert!(wait_ms >= 0, "time limit must be >= 0");
        Self { buffer, buffer_time: |_k, ts| ts, wait: WaitKind::Fixed(wait_ms) }
    }
}
```

`Suppressed<K>` is `Copy` (all fields `Copy`). The `Debug` derive needs no special
handling (fn pointers are `Debug`). The key type `K` flows by inference from the
`suppress` call.

## 6. `KTableSuppressProcessor<K, V>` (`dsl/processors/suppress.rs`)

Generalized from `<KInner, V>`:

```rust
pub(crate) struct KTableSuppressProcessor<K, V> {
    buffer: TimeOrderedKeyValueBuffer<K, Change<V>>,
    observed_stream_time: i64,
    wait_ms: i64,
    buffer_time: fn(&K, i64) -> i64,
    max_records: Option<usize>,
    emit_early: bool,
    _pd: Marker<(K, V)>,
}
// new(wait_ms, buffer_time, max_records, emit_early)
```

`Processor<K, Change<V>, K, Change<V>>` where `K: Any + Send + Sync + Clone + Eq +
Hash`, `V: Any + Send + Clone`. `process`:

```rust
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
        // emitEarlyWhenFull: evict + emit the oldest until back within the cap.
        while self.buffer.len() > cap {
            let (k, change, rts) = self.buffer.evict_oldest().expect("len > cap >= 1");
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
```

For window-close (`buffer_time = window.end`, `wait = grace`) this is byte-for-byte
the slice-A/B behavior. For time-limit (`buffer_time = ts`, `wait = duration`) the
same machinery rate-limits: a buffered record at `ts` emits when stream-time passes
`ts + wait`; a newer record for the same key re-`put`s (replace-by-key) with a new
`buffer_time = newer_ts`, resetting the timer.

## 7. `KTable::suppress` — general impl (`dsl/ktable.rs`)

Move `suppress` from `impl<KInner, V> KTable<Windowed<KInner>, V>` to:

```rust
impl<K, V> KTable<K, V>
where K: Any + Send + Sync + Clone + Eq + std::hash::Hash, V: Any + Send + Clone {
    pub fn suppress(&self, suppressed: Suppressed<K>) -> KTable<K, V> {
        let wait_ms = match suppressed.wait {
            WaitKind::UpstreamGrace => self.window_grace_ms.unwrap_or(0),
            WaitKind::Fixed(ms) => ms,
        };
        let buffer_time = suppressed.buffer_time;
        let max_records = suppressed.buffer.record_cap();
        let emit_early = suppressed.buffer.is_emit_early();
        // … existing lowering thunk, but NodeHandle/add_processor typed over K
        //   (not Windowed<KInner>), constructing
        //   KTableSuppressProcessor::<K, V>::new(wait_ms, buffer_time, max_records, emit_early).
        //   Returned table keeps the grace (with_window_grace) for window-close chains.
    }
}
```

The grace plumbing (`window_grace_ms` field, set by windowed/session aggregations,
propagated through `map_values`/`filter`) is unchanged — only consumed differently
(via `WaitKind::UpstreamGrace`). Existing call-sites
`suppress(Suppressed::until_window_closes(unbounded()))` compile + behave
identically.

## 8. Tests

- **Behavior-preserved:** the slice-A/B suppress processor + execution tests
  (window-close final-results, grace-delayed close, multi-window-close,
  max-records shutdown) stay green after the refactor (update their `new(...)` calls
  to the new signature; the `Suppressed::until_window_closes` call-sites are
  unchanged).
- **Config unit tests:** `max_records(n)` is eager; `unbounded().with_max_records(n)`
  is strict; `emit_early_when_full()`/`shut_down_when_full()` toggle.
- **Time-limit processor/execution test:** `until_time_limit(50, unbounded())` on a
  non-windowed table — a record for key "a" at t=10 stays buffered; another key's
  record at t=100 advances stream-time past 10+50 → "a" emits once. A *newer* "a"
  at t=40 (before t=10+50 elapses) replaces the buffered value + resets the timer
  (emits the newer value only after 40+50).
- **emit-early test:** `until_time_limit(10_000, max_records(2))` (eager) — three
  keys buffered, the third over-fills → the oldest is evicted + emitted *early*
  (no panic); assert the early emit.
- **Strict-on-window-close panic:** `until_window_closes(max_records(2))` (eager
  config) panics at the `until_window_closes` assert (`#[should_panic]`).
- **No golden change:** `dsl_golden_frame` stays `13 passed`, byte-identical.

## 9. Phasing

- **T1:** `BufferConfig` overflow mode + eager `max_records(n)` + toggles + the
  renamed `record_cap()` getter (`dsl/suppress.rs`); buffer `evict_oldest`
  (`dsl/processors/suppress_buffer.rs`) + their unit tests.
- **T2 (the unification — one compile unit):** `Suppressed<K>` (fn-pointer +
  `WaitKind` + `until_time_limit` + refactor `until_window_closes`) +
  `KTableSuppressProcessor<K,V>` generalization (buffer_time/wait_ms/emit_early) +
  `KTable::suppress` on the general impl; migrate the existing processor/exec tests'
  `new(...)` calls. (`dsl/suppress.rs`, `dsl/processors/suppress.rs`, `dsl/ktable.rs`.)
- **T3:** time-limit + emit-early + window-close-eager-panic execution tests
  (`dsl_execution.rs`) + lib-doc note + final verification (full suite, 13 goldens,
  clippy `--all-targets`, fmt).

## 10. Risks / open items

- **Type inference for `until_window_closes`** — it's an assoc fn on
  `impl<KInner> Suppressed<Windowed<KInner>>`; `Suppressed::until_window_closes(cfg)`
  must infer `KInner` from the `suppress` call's `K`. This works because `suppress`
  binds `Suppressed<K>` with `K` = the table key; if inference ever needs a hint,
  the call site can annotate. (Validated by the existing call-sites compiling.)
- **Panic semantics** — `shutDownWhenFull` panics (slice B); unchanged.
  `emitEarlyWhenFull` never panics (evicts instead).
- **`maxBytes` absent** — deferred to D; the `BufferConfig` shape (private `Option`
  + `bool` + builders) extends to it without breaking this slice's API.
