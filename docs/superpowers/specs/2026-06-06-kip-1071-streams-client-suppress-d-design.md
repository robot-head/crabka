# KIP-1071 Streams Client — Suppress Slice D: fault tolerance (changelog + restore) + `maxBytes`

**Status:** design approved (2026-06-06)
**Branch:** `streams-suppress-d` — stacks on `streams-suppress-c` (PR #409). Rebase
onto `main` when #409 merges.
**Worktree:** `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`
**Ground truth:** Docker JVM Kafka-Streams 4.1.0 (topology golden + a new
`BufferValue` byte-vector capture).

The **final** slice of the suppress program (A → B → C → **D**) and the capstone of
the whole windowing arc. It makes the suppress buffer **durable**: the buffer
becomes a registered state store that writes a changelog and restores from it on
restart, and it adds the `maxBytes` cap (deferred from B/C because it needs
serialized byte sizes). The changelog is **byte-exact** to the JVM
`InMemoryTimeOrderedKeyValueChangeBuffer` so a Crabka suppress task can restore from
a JVM-written changelog (mixed-cluster interop).

Per `CLAUDE.md`: greenfield (no compat shims); Apache Kafka wire-protocol +
changelog byte exactness.

## 1. Scope (decided)

- **In:** the suppress buffer as a registered byte-oriented `StateStore`
  (`SuppressBytesStore`); serde plumbing (a store-factory thunk on the KTable
  handle); the **JVM-exact** `BufferValue` + `ProcessorRecordContext` changelog
  codec; `add_suppress_store` + `ChangelogKind::Suppress` → the changelog topic in
  the wire (logging on); restore (free via `StreamTask::restore`); `BufferConfig`
  `maxBytes`; a new logging-enabled golden (#14) + a `BufferValue` byte-vector
  capture; a restart-restore execution test.
- **Decisions:** changelog VALUE bytes are **JVM-exact** (interop); `suppress`
  with logging on but no resolvable serdes **panics** with a clear message;
  `maxBytes` accounting = `serialized_key.len() + serialized_value.len()`.
- **Out:** nothing further — D completes the suppress program.

## 2. Architecture

The suppress buffer moves from a processor-owned in-memory field (slices A–C) to a
**registered `StateStore`**, so the proven #3/#2b changelog-produce + restore
machinery handles durability with no new restore code (`StreamTask::restore`
already iterates registered stores, reads each `changelog_topic()`, and replays
it). The processor accesses the store via `ctx.get_suppress_store(name)` — the same
shape as `get_window_store` (4d-ii). Serdes reach the store via a thunk on the
KTable handle, captured where the producing operation knows the concrete serdes.

## 3. Serde plumbing — store-factory thunk (`dsl/ktable.rs`, aggregations, `builder.rs`)

`suppress` needs `Serde<K>` + `Serde<Change<V>>` to register the byte store. The
KTable handle gains:

```rust
pub(crate) type SuppressStoreFactory =
    Box<dyn Fn(&str /*store_name*/, &mut Topology, &[String] /*processors*/)>;
// field on KTable<K,V>:
suppress_store_factory: Option<SuppressStoreFactory>,
```

The thunk captures the **concrete** key/value serdes (so no clone-able-serde trait
object — same pattern as `KGroupedStream::repartition_lower`) and calls
`topology.add_suppress_store::<K, V, KS, VS>(store_name, ks.clone(), vs.clone(),
retention_ms, [processor])`. It is set by:
- **windowed aggregations** (`windowed_kgrouped.rs`): key serde =
  `TimeWindowedSerde::new(ks, size)`, value serde = `vs` (the aggregate's).
- **session aggregations** (`session_windowed_kgrouped.rs`): key serde =
  `SessionWindowedSerde::new(ks)`, value serde = `vs`.
- **`builder.table`**: `ks` / `vs`.
- **`Change`-preserving ops that keep `V`** (`filter`): propagate the parent's
  factory. **`map_values` (changes `V`) drops it** (the value serde no longer
  matches) — a suppress-with-logging on such a derived table panics (below).

**The suppress store is ALWAYS registered** (the JVM `TimeOrderedKeyValueBuffer` is
byte-oriented internally regardless of logging), so the processor has one uniform
code path and serdes are required for *any* suppress. `suppress` always invokes the
handle's factory to register the `SuppressBytesStore`; if the factory is `None`
(serdes unresolvable, e.g. after a `map_values` that changed `V`), it **panics**:
`"suppress needs the upstream serdes — materialize the table or keep the value type
unchanged"`. (This corrects a slice-A simplification where logging-disabled suppress
ran serde-free; the tested paths — aggregation/`builder.table` → suppress — always
have serdes.)

**Logging** toggles only the changelog, via `Suppressed<K>.logging: bool` (default
`true`) + `with_logging_disabled()` / `with_logging_enabled()`, threaded to the
store (`store.set_logging(on)` — the existing `StateStore` hook):
- **logging on** → the store produces a changelog + the changelog **topic appears
  in the wire** (golden #14).
- **logging off** → no changelog produced, **no changelog topic in the wire** (a
  registered store with logging disabled contributes no `state_changelog_topics`
  entry) → slice A's golden #13 stays byte-identical. The slice-A golden test
  switches its DSL to `…until_window_closes(unbounded()).with_logging_disabled()`
  (the Crabka equivalent of the JVM `.withLoggingDisabled()` it captured).

## 4. `SuppressBytesStore` (`store/suppress_store.rs`)

A registered, byte-oriented, time-ordered, replace-by-key buffer store:

```rust
#[async_trait]
pub trait SuppressStore: StateStore {
    /// Insert/replace by key; ordered by (buffer_time, seq). `record_ctx` is the
    /// buffered record's context (for the JVM-exact changelog BufferValue).
    async fn put(&mut self, key: Bytes, buffer_time: i64, value: Bytes, ctx: SuppressRecordCtx);
    async fn evict_while(&mut self, threshold: i64) -> Vec<(Bytes, Bytes, i64)>; // (key, value, record_ts)
    async fn evict_oldest(&mut self) -> Option<(Bytes, Bytes, i64)>;
    fn len(&self) -> usize;
    fn byte_size(&self) -> usize; // running Σ(key.len + value.len) for maxBytes
}
```

`SuppressBytesStore` holds the ordered `BTreeMap<(i64,u64), Entry>` + the by-key
`HashMap<Bytes,(i64,u64)>` (the slice-A buffer, now byte-keyed) + the changelog
buffer + `byte_size`. Each `put` appends `(key, Some(changelog_value))` to the
changelog (the JVM-exact bytes, §5); each eviction appends `(key, None)`
(tombstone). `take_changelog`/`apply_changelog` (StateStore) drive produce +
restore; `apply_changelog` parses the `bufferTime` + value to rebuild the ordered
structure (the `seq` tiebreaker is regenerated on restore — it only orders ties and
is not semantically load-bearing). `StoreRegistry::get_suppress` +
`ProcessorContext::get_suppress_store` (mirroring `get_window`/`get_window_store`).

**Processor refactor** (`dsl/processors/suppress.rs`): drop the owned `buffer`
field; hold `store_name`. `process` does `ctx.get_suppress_store(&store_name)` for
each put/evict, in scoped borrows dropped before `ctx.forward` (the join/window
discipline). Behavior is otherwise identical (fn-pointer `buffer_time`, `wait_ms`,
the `emit_early`/shutdown overflow + the new `maxBytes` branch). The
`KTableSuppressProcessor` keeps working with the in-memory store when logging is
disabled (the store is still registered as a store, just without a changelog) — so
ALL execution tests run against the registered store.

## 5. JVM-exact `BufferValue` codec (`store/suppress_bufval.rs`)

The heaviest new piece. Matches the JVM `InMemoryTimeOrderedKeyValueChangeBuffer`
changelog. **Changelog key** = the record key bytes. **Changelog value** =
`BufferValue.serialize() ‖ bufferTime:8 (BE)`.

`ProcessorRecordContext.serialize()` (field order pinned by the byte-vector
capture — topic precedes partition):
```
timestamp : i64 BE
offset    : i64 BE
topicLen  : i32 BE   (-1 if null topic)
topic     : UTF-8 bytes
partition : i32 BE
headerCount : i32 BE   (Crabka streams records are header-less → 0)
[ per header: keyLen:i32, key, valueLen:i32 (-1 if null), value ]
```

`BufferValue.serialize()`:
```
ProcessorRecordContext bytes (above)
priorLen : i32 BE  (-1 if null), prior bytes
oldLen   : i32 BE  (-1 null / -2 "old == prior" sentinel / else len), old bytes
newLen   : i32 BE  (-1 if null), new bytes
```
where, for the suppress buffer, `new` = the buffered `Change.new` value bytes,
`old` = `Change.old` value bytes, `prior` = the value previously buffered for the
key (or `null`). The `bufferTime:8BE` is appended after the `BufferValue` bytes
(the JVM `logValue(endPadding = 8)`).

Rust API: `serialize_buffer_change(ctx: &SuppressRecordCtx, prior: Option<&[u8]>,
old: Option<&[u8]>, new: Option<&[u8]>, buffer_time: i64) -> Bytes` +
`deserialize_buffer_change(&[u8]) -> (SuppressRecordCtx, old, new, buffer_time)`
(restore needs `new` + `buffer_time`; `prior`/`old`/`ctx` are parsed for
completeness). The sentinels (`-1`, `-2`) are matched exactly.

**The exact byte layout is pinned by the §6 capture** — these field orders/sentinels
are from the Kafka source but must be confirmed empirically (greenfield, no
guessing on byte exactness).

## 6. Capture + golden + tests

### 6.1 `BufferValue` byte-vector capture (new mini-harness)

A small Java program (alongside `Capture.java`, or a sibling `BufferValueCapture
.java`) constructs known `ProcessorRecordContext` + `BufferValue` instances via the
Kafka Streams classes, calls `serialize()`, and dumps hex →
`tests/testdata/suppress_bufval/<case>.hex`. Cases: a plain `(old=null,
new=count)` window-close value; an `(old=prior, new=...)` update; a null-new
tombstone. Run inside Docker with the kafka-streams jar (no broker — pure
serialization). The Rust `suppress_bufval` codec unit tests assert
`serialize_buffer_change(...) == fixture_bytes` for the same inputs.

### 6.2 Topology golden #14 (logging on)

`Capture.java` fixture #14 `suppressUntilWindowClosesLogged()` = the slice-13 app
WITHOUT `.withLoggingDisabled()` (logging on) → the suppress buffer changelog topic
appears. Capture → `suppress_until_window_closes_logged.topology.json`. Pins the
JVM suppress changelog topic **name** (`<app>-<suppressNode>-changelog` or the JVM
store-name form) + **config** (`ChangelogKind::Suppress`'s configs — captured).
`add_suppress_store` + `ChangelogKind::Suppress` in `node.rs`/`wire.rs`/`builder.rs`
mirror `add_window_store`. The slice-A golden (#13, logging disabled) stays
byte-identical (its DSL test switches to `.with_logging_disabled()`). The other 12
goldens unchanged.

### 6.3 Execution + store tests

- `SuppressBytesStore` unit tests: put/evict_while/evict_oldest/len/byte_size +
  take_changelog (put→Some, evict→tombstone) + apply_changelog rebuild.
- `maxBytes` overflow: `until_window_closes(unbounded().with_max_bytes(n))`
  (shutdown panic) + `until_time_limit(.., max_bytes(n) eager)` (emit-early).
- **Restart-restore** execution/integration test: buffer some windows, take the
  changelog, restore into a fresh store/driver, assert the buffered windows still
  emit correctly (mirrors `stateful_task_produces_changelog_and_restores`).
- All prior suppress tests stay green (now running against the registered store).

## 7. Phasing

- **T1 — `BufferValue` codec + capture** (`store/suppress_bufval.rs` + the Java
  `BufferValueCapture` + the hex fixtures + byte-vector tests). Self-contained; the
  riskiest byte-exactness work first, validated by the capture.
- **T2 — `SuppressBytesStore`** (`store/suppress_store.rs`: the byte buffer +
  `SuppressStore` trait + `StateStore`/changelog using T1's codec + registry
  `get_suppress` + `get_suppress_store` accessor) + unit tests.
- **T3 — processor refactor** (`dsl/processors/suppress.rs`: owned buffer → store
  access via `ctx`; `maxBytes` branch) + migrate the processor tests to register a
  `SuppressBytesStore`.
- **T4 — wire + DSL** (`add_suppress_store` + `ChangelogKind::Suppress` in
  node/wire/builder; `Suppressed.logging` + `with_logging_disabled`; the
  serde-thunk on the KTable handle + set it in the aggregations/`builder.table` +
  propagate/drop through `filter`/`map_values`; `suppress` invokes the thunk or
  panics) + `BufferConfig::max_bytes`/`with_max_bytes`.
- **T5 — golden #14 + restore + maxBytes execution tests + docs + final verify**
  (Capture.java fixture #14 + Docker capture (controller) + the logging-on golden
  test + update slice-A's #13 test to `.with_logging_disabled()`; restart-restore
  + maxBytes execution tests; lib docs; full suite, **14 goldens**, clippy
  `--all-targets`, fmt).

## 8. Risks / open items

- **`BufferValue`/`ProcessorRecordContext` byte exactness** — the field
  order/sentinels (`-1`/`-2`) are from the Kafka source but **pinned empirically by
  the T1 capture**; if 4.1 differs, the codec is tuned to the fixture. This is the
  highest-risk piece; T1 does it first.
- **`prior` value semantics** — the JVM `BufferValue.priorValue` (used for the
  `-2` "old == prior" sentinel) maps to the previously-buffered value for the key.
  The capture cases include the duplicate case to pin the sentinel behavior; if
  Crabka's prior-tracking diverges, the restore round-trip (not byte-interop) still
  holds, and the byte-interop cases are documented.
- **Record context fields** — Crabka streams records expose
  topic/partition/offset/timestamp (via `RecordContext`) and no headers →
  `headerCount = 0`. Header-bearing records would need header serialization; out of
  scope (streams suppress records are header-less in the current model).
- **Suppress store auto-name + changelog config** — pinned by the #14 capture; the
  `add_suppress_store` naming/`ChangelogKind::Suppress` configs are tuned to it.
- **No new restore code** — restore is reused from `StreamTask::restore`; the slice
  only implements `take_changelog`/`apply_changelog` on the store.
