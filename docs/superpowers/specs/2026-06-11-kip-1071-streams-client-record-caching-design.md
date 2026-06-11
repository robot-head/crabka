# KIP-1071 Streams Client — Record caching (statestore.cache.max.bytes)

> **Status:** design approved 2026-06-11. One slice covering KV + Window +
> Session caching store wrappers over a shared `ThreadCache`/`NamedCache` core.
> Ground truth = ported Apache Kafka `NamedCacheTest` / `ThreadCacheTest` /
> `Caching*StoreTest` scenarios (JVM-free, deterministic) + one empirical
> Kafka-Streams 4.1.0 Docker capture for the end-to-end commit-flush emit-dedup.

## 1. Goal

Add Kafka Streams' **record cache** to `crabka-client-streams`: the
`statestore.cache.max.bytes` write-back cache layered between a materializing
processor and its state store. With the cache on, a processor does **not**
forward every intermediate update downstream — it writes to a per-store LRU
cache, and forwards a single deduplicated `Change` per key only when the entry
is **evicted** (LRU under memory pressure) or **flushed** (on commit). Reads and
interactive queries see cached (not-yet-flushed) writes — read-your-writes.

This is the last large Kafka Streams *runtime* feature absent from the client
(the DSL-parity surface is otherwise complete). It is explicitly deferred in the
KGroupedTable design (§"Out of scope: Caching / record-cache suppression").

## 2. Scope

In scope (one slice):

- **Core:** `LruCacheEntry`, `NamedCache` (per-store LRU + dirty set),
  `ThreadCache` (byte budget + cross-store eviction).
- **Three caching store wrappers:** `CachingKeyValueStore`, `CachingWindowStore`,
  `CachingSessionStore` — each implements the existing byte-store trait it wraps.
- **Forwarding:** a flush listener that, on evict/flush of a dirty entry, writes
  through to the underlying store and forwards `Change { old, new }` with the
  entry's record context into the store-owning node's children.
- **Drive points:** `graph.flush_caches()` invoked from `task.commit()` before
  the producer commits; mid-process eviction through the current node's dispatch.
- **Config:** `statestore.cache.max.bytes` (default `10_485_760` = 10 MiB) on the
  `StreamsApp` / `KafkaStreams` builder; `TopologyTestDriver` forces `0`.
- **Per-store opt-out:** `Materialized::with_caching(false)` (default `true`),
  mirroring the existing `with_logging`.
- **Cache-first reads + IQ read-your-writes** through all three wrappers.
- **EOS flush ordering:** caches flushed before `sendOffsetsToTransaction`.

Out of scope (YAGNI):

- **Changelog / store byte format** — unchanged. The cache alters *when and how
  many* downstream emits happen; it never changes changelog records or the
  bytes written to the underlying store. This is the fidelity anchor.
- **Record-cache metrics** — hit-ratio / `cache-size-bytes` sensors (KIP-444).
- **`cache.max.bytes.buffering` legacy alias** — greenfield; only the
  KIP-1024 name `statestore.cache.max.bytes` exists.
- **Named-cache eviction *ordering* across stores under contention** beyond what
  the ported `ThreadCacheTest` covers (single-thread, deterministic).

## 3. Semantics (ground truth)

All behavior matches Apache Kafka 4.1.0. The two observable effects:

### 3.1 Emit dedup / delay (the defining behavior)

With the cache on, repeated updates to a key within a commit interval collapse:
only the **final** value per key is forwarded, with `old` = the value last
written through to the underlying store (i.e. the last flushed value), `new` =
the latest cached value. Sequence:

```
put(k, v1) → cache[k] = v1 dirty,  no forward
put(k, v2) → cache[k] = v2 dirty,  no forward
commit     → flush: old = underlying.get(k); underlying.put(k, v2);
                    forward Change{old, new: v2}   ← exactly ONE emit
```

This is *record-cache suppression* — distinct from `suppress()` (KIP-328): the
cache is keyed by store, bounded by **bytes** (not records/time), forwards on
**eviction or commit** (not window-close/time-limit), and is a transparent store
wrapper rather than a DSL operator.

### 3.2 Read-your-writes

`get(k)` checks the cache first (returning a cached value or cached tombstone)
before the underlying store; `range`/`fetch` merge the cache iterator with the
underlying iterator, cache winning on key collision and tombstones hiding the
underlying value. Interactive queries (v1 and IQv2) read through the wrapper, so
they observe not-yet-committed writes — matching JVM.

### 3.3 Eviction

A `put` that pushes `ThreadCache` total bytes over budget triggers
`maybe_evict`: evict LRU-head entries (across all named caches, oldest first)
until under budget; a dirty evicted entry is flushed first (write-through +
forward). Entry byte size uses Kafka's `LRUCacheEntry` formula exactly so the
eviction *threshold* matches: `key.len + value.len + record-context overhead +
per-node overhead` (constants ported from `NamedCache`/`LRUCacheEntry`).

### 3.4 Disabled (size 0)

When the budget is `0` **or** a store sets `with_caching(false)`, the store is
**not wrapped** — puts forward immediately, exactly as today. This is what keeps
every existing golden (all captured via the `0`-cache `TopologyTestDriver` path)
byte-for-byte unchanged.

## 4. Architecture

New module `src/store/cache/`:

### 4.1 `LruCacheEntry` (`cache/entry.rs`)

```
value: Option<Vec<u8>>          // None = tombstone
dirty: bool
context: RecordContext          // offset, timestamp, partition, topic, headers
```
`size_bytes()` ports Kafka's formula (drives eviction parity).

### 4.2 `NamedCache` (`cache/named.rs`)

Per-store LRU: a doubly-linked LRU over `Bytes → LruCacheEntry` plus a
`dirty_keys` set in **insertion order**.

- `put(key, entry)` / `get(key)` / `delete(key)` (tombstone put).
- `flush(&mut listener)` — drain `dirty_keys` in order, calling the listener per
  entry, clearing dirty.
- `evict(&mut listener)` — pop LRU head; if dirty, flush it via the listener
  first; return its freed bytes.
- `size_bytes()`.

The **listener** is `FnMut(&Bytes, &LruCacheEntry)` supplied by the owning store
wrapper, which performs write-through + downstream forward.

### 4.3 `ThreadCache` (`cache/thread.rs`)

`name → NamedCache` + `max_bytes` budget + running total. `put` updates the total
and calls `maybe_evict` (evict LRU across caches until `total <= max_bytes`).
Budget = `statestore.cache.max.bytes` divided across the task set (JVM divides
across StreamThreads; crabka divides across active tasks). One `ThreadCache` per
task; lives in the task's graph.

### 4.4 Store wrappers (`cache/kv.rs`, `cache/window.rs`, `cache/session.rs`)

Each implements the same byte-store trait as the store it wraps and holds
`Arc<Mutex<NamedCache>>` (registered in the `ThreadCache`) + a flush-listener
handle (see §4.5):

- **`CachingKeyValueStore`** wraps `KeyValueBytesStore`. `get` cache-first;
  `range`/`all` via a merged-sorted iterator (cache wins; tombstone hides
  underlying); `put`/`delete` write the cache dirty, then `maybe_evict`.
- **`CachingWindowStore`** wraps the window byte store. Cache keyed by the
  existing window key-schema bytes (`store/window_schema.rs`); `fetch`/`fetchAll`
  merge cache + underlying over the same key range.
- **`CachingSessionStore`** wraps the session byte store. Cache keyed by the
  session key-schema bytes (`store/session_schema.rs`); `findSessions` merges.

### 4.5 Flush listener / forwarding

The materializing processor (KTable source, KV/window/session aggregate, reduce,
table-table, etc.) registers a flush listener on its caching store at build time.
On a dirty entry flushing:

1. `old = underlying.get(key)`.
2. `underlying.put(key, entry.value)` (or delete on tombstone).
3. forward `Change { old, new: entry.value }` with `entry.context` into the
   **store-owning node's** children.

Forwarding outside a `process()` call reuses the **punctuation drive mechanism**:
`graph.flush_caches()` reconstructs a `Dispatch` rooted at each cache-owning node
(exactly as `punctuate_stream_time` does for punctuators) and forwards through
it; the task then drains the resulting sink/changelog output via the existing
`drain_punctuation_output` path.

### 4.6 Drive points

- **Commit:** `task.commit()` calls `graph.flush_caches()` **before** the
  producer commit (EOS: before `sendOffsetsToTransaction`; ALOS: before offset
  commit), then drains sinks/changelogs. The existing `flush → commit` ordering
  in `task.rs` is preserved with the cache flush prepended.
- **Mid-process eviction:** a `put` during `process()` that overflows the budget
  evicts via the **current** node's dispatch. A processor writes to its own
  store, so the store-owning node == the currently-executing node; the eviction
  forward roots there. (Eviction-free goldens avoid this path; the ported
  `ThreadCacheTest` covers it.)
- **Close:** `task.close()` flushes caches (clean shutdown emits buffered state),
  matching JVM `StreamTask.closeClean`.

### 4.7 Config plumbing

- Add `cache_max_bytes: i64` (default `10_485_760`) to the `StreamsApp` builder
  and thread it to `KafkaStreams` → the task/graph's `ThreadCache`.
- `TopologyTestDriver` constructs the graph with `cache_max_bytes = 0` (the JVM
  TTD default) — preserving all existing goldens. A test-only setter allows a
  caching-enabled driver for the behavioral golden (§5.3).
- `Materialized` gains `caching: bool` (default `true`) + `with_caching(on)`,
  parallel to the existing `logging` field. Lowering wraps a store in its caching
  wrapper iff `cache_max_bytes > 0 && materialized.caching`.

## 5. Testing / ground truth

### 5.1 Cache internals (JVM-free unit ports)

Port the scenarios from Apache Kafka `NamedCacheTest` and `ThreadCacheTest`:
LRU ordering, eviction trigger points, dirty-flush order (insertion order),
tombstone handling, and **exact byte accounting** (the `size_bytes` formula —
a wrong constant shifts the eviction threshold). These are deterministic and
need no broker.

### 5.2 Store wrappers (JVM-free unit ports)

Port `CachingKeyValueStoreTest` / `CachingWindowStoreTest` /
`CachingSessionStoreTest` essentials: cache-first `get`, merged range/fetch
ordering, tombstone-hides-underlying, put→flush→write-through→forward, and
delete semantics.

### 5.3 End-to-end emit-dedup (one Docker Streams 4.1.0 capture)

A caching-enabled topology (`count`/`reduce` into a materialized table) fed a
fixed batch of repeated-key updates, with the cache sized large enough that **no
mid-batch eviction occurs**, and a single commit. Capture the **output topic**
records and assert only the final value per key is forwarded (the §3.1 behavior).
Replay byte-for-byte through a caching-enabled `TopologyTestDriver` + explicit
`commit()`. LRU-eviction forwarding is not cleanly observable via an output
topic, so it stays covered by §5.1.

### 5.4 Regression guard

Run the full existing golden suite unchanged — the `0`-cache TTD path must keep
every current emit-on-update golden green (the disabled-path invariant, §3.4).

## 6. Risks

- **Byte-size formula drift** (the one real eviction-parity risk) — mitigated by
  porting `LRUCacheEntry.size()` constants verbatim and unit-pinning them.
- **Forward-at-flush wiring** — store→owning-node ownership must be recorded at
  build time so `flush_caches()` can root the dispatch correctly; modeled on the
  punctuator node-rooting that already works.
- **Merged-iterator ordering** — cache/underlying merge must match JVM's
  `MergedSortedCacheKeyValueBytesStoreIterator` (cache wins, tombstones skipped);
  pinned by the §5.2 ports.
- **EOS flush ordering** — cache flush must precede the txn offset send or
  buffered output is lost on commit; covered by an EOS-path test.

## 7. Files touched

New:
- `crates/client-streams/src/store/cache/mod.rs`
- `crates/client-streams/src/store/cache/entry.rs`
- `crates/client-streams/src/store/cache/named.rs`
- `crates/client-streams/src/store/cache/thread.rs`
- `crates/client-streams/src/store/cache/kv.rs`
- `crates/client-streams/src/store/cache/window.rs`
- `crates/client-streams/src/store/cache/session.rs`
- `crates/client-streams/tests/jvm-capture/.../RecordCacheTopology.java`
- `crates/client-streams/tests/testdata/record_cache/*.json`
- `crates/client-streams/tests/record_cache_golden.rs`

Modified:
- `crates/client-streams/src/store/mod.rs` (module export)
- `crates/client-streams/src/store/registry.rs` (wrap on materialize when enabled)
- `crates/client-streams/src/dsl/config.rs` (`Materialized::caching` + `with_caching`)
- `crates/client-streams/src/processor/graph.rs` (`flush_caches`, `ThreadCache` owner, node-rooted flush dispatch)
- `crates/client-streams/src/runtime/task.rs` (call `flush_caches` in `commit`/`close`)
- `crates/client-streams/src/streams_app.rs` + `KafkaStreams` builder (`cache_max_bytes`)
- `crates/client-streams/src/test_driver.rs` (force `0`; caching-enabled test ctor)
- `.github/workflows/ci.yml` (add `record_cache_golden` to the crate's llvm-cov `--test` list)

## 8. Verification gate

```
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p crabka-client-streams
cargo build --workspace
```

(Clippy cache can mask workspace lints — `touch` suspect-but-unchanged files and
check the real `$?`. New `tests/*.rs` must be in the crate's llvm-cov `--test`
list or coverage reports 0% for the patch.)
