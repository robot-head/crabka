# KIP-1071 Streams Client — Record-cache parity for `cogroup` + `to_table`

> **Status:** design approved 2026-06-11. Follow-up to the merged record-caching
> slice (#491). One slice flipping `statestore.cache.max.bytes` write-back
> caching ON for the two materialized stores Kafka caches but Crabka currently
> forces uncached: the **cogroup result store** (all four kinds) and the
> **`KStream.to_table`** store. No new cache machinery — the cache core,
> `TupleForwarder` suppression seam, `flush_cache_into`, and `cache_owner`-rooted
> flush all exist from #491; these two stores were simply never *marked* cached.

## 1. Goal

Close the last two genuine Kafka record-cache parity gaps. With caching on, the
cogroup result store and the `to_table` store must:

- **Suppress** the per-record immediate downstream forward (the deduped `Change`
  is forwarded later, at cache evict/flush).
- Serve **read-your-writes** through the cache (interactive queries and — for
  cogroup — *cross-input* aggregation within a batch see not-yet-flushed writes).
- Defer the **changelog** write to flush, and restore correctly from it.
- Leave every existing golden **byte-identical** when caching is disabled
  (`TopologyTestDriver` forces `cache_max_bytes = 0`).

## 2. Background: why this is wiring, not new machinery

The #491 slice made `KeyValueBytesStore` / `WindowBytesStore` / `SessionBytesStore`
cache-aware via a `Backing::{Plain,Cached}` enum, added the `TupleForwarder`
forward-suppression seam, `StateStore::flush_cache_into`, and a
`cache_owner: HashMap<store, node_idx>` that roots each store's flush at its
materializing node. A store participates in caching iff it is added to
`Topology.caching_stores` via `Topology::mark_store_caching(store, true)` **and**
`cache_max_bytes > 0`. `wire_record_caches`
(`topology/builder.rs:1543`) roots `cache_owner` at
`store_processors.get(store).first()` — the store's first connected processor
node — and skips stores whose `enable_cache_erased` returns `false`.

Both target stores are already cache-aware; they were just marked `false` (or
never marked). The fix is to flip the mark and ensure the forward path suppresses.

## 3. Component 1 — `KStream.to_table` (mechanical)

### 3.1 Current state

`KStreamToTableProcessor` (`dsl/processors/table.rs:80`) is the **only** KTable
materializer lacking a `TupleForwarder`: it does a raw

```rust
ctx.forward(Record::new(Some(key), Change::update(old, r.value), r.timestamp));
```

The sibling `KTableSourceProcessor` (same file, line 39) is the exact template —
it already carries `forwarder: TupleForwarder`, resolves it in `init` from
`ctx.store_is_cached(&self.store_name)`, and forwards via
`self.forwarder.maybe_forward(...)`.

The lowering site `to_table_explicit` (`dsl/kstream.rs:1867`) destructures
`Materialized { key_serde, value_serde, logging, .. }` — **dropping `caching`** —
and never calls `mark_store_caching`.

### 3.2 Changes

1. **`KStreamToTableProcessor`** (`table.rs`): add `pub forwarder: TupleForwarder`
   field; add an `async fn init` that sets
   `self.forwarder = TupleForwarder::resolve(ctx.store_is_cached(&self.store_name))`;
   replace the raw `ctx.forward` with
   `self.forwarder.maybe_forward(ctx, key, old, r.value, r.timestamp)`. Set the
   store record context before the put (mirror `KTableSourceProcessor`: stash
   `ctx.record_context().clone()` and `store.set_record_context(rc)` before
   `store.put`) so the cached store attaches the source context to the change it
   forwards on flush.
2. **`to_table_explicit`** (`kstream.rs`): include `caching` in the `Materialized`
   destructure; construct the processor with `forwarder: TupleForwarder::default()`;
   after the `add_state_store` / `add_state_store_no_changelog` call inside the
   lower thunk, call `state.topology.mark_store_caching(&store_for_thunk, caching)`.

`to_table` (default-serde) and any other entry points delegate to
`to_table_explicit`, so no other DSL edits are needed.

## 4. Component 2 — cogroup (all four kinds)

### 4.1 Current state and why it's safe to enable

cogroup reuses the **standard** aggregate processors —
`KStreamAggregateProcessor` (non-windowed) and
`KStream{Window,Session,SlidingWindow}AggregateProcessor` — one per input,
fanned into a single `KStreamPassThrough` merge node
(`dsl/processors/cogroup_merge.rs`) that relays `Change<VOut>` to the result
`KTable`. Those aggregate processors **already** carry a `TupleForwarder` and
already suppress when their store is cached (#491). The merge passthrough only
relays.

cogroup always forces **emit-on-update** (`make_agg_for_input` sets
`EmitStrategy::default()` for every windowed kind — `cogrouped.rs:176,208,246`),
so the windowed/session/sliding aggregate processors' suppression gate
(`caching && emit.is_on_update()`) is satisfied; their stores are cache-eligible.

`cache_owner` roots the flush at `store_processors.first()` — the first per-input
aggregate node. Its child is the merge passthrough, so the flushed deduped
`Change` routes through the passthrough to the downstream KTable exactly like a
live forward would. Because **all** per-input aggregators share the one cached
store, they all suppress; the passthrough never sees an immediate forward, so
there is **no double-emit**. The original deferral (merge "forwards
unconditionally") was conservative — the passthrough only ever relays flush
output once the store is cached.

### 4.2 The one new correctness property: cross-input read-your-writes

In a single batch, input A's aggregator does read-modify-write on key `k`
(`get` accumulator → apply aggregator → `put`), then input B's aggregator for the
same `k` must read **A's not-yet-flushed accumulator** to continue accumulating,
not the stale store/changelog value. The cache-aware store `get()` reads through
the cache, so this holds — but it is the property the tests must prove, because
it is unique to the multi-writer-single-store cogroup shape.

### 4.3 Changes (4 terminal aggregations)

| Site | File | Change |
|------|------|--------|
| non-windowed (KV) | `dsl/cogrouped.rs:337` | `caching` is already destructured (`:303`). Replace `mark_store_caching(&store_for_reg, false)` with `mark_store_caching(&store_for_reg, caching)` and delete the deferral comment + `let _ = caching;`. |
| time-windowed | `dsl/time_windowed_cogrouped.rs:71,87` | Add `caching` to the `Materialized` destructure; inside the registrar, after `add_window_store`, call `state.topology.mark_store_caching(&store_for_reg, caching)`. |
| session-windowed | `dsl/session_windowed_cogrouped.rs:85,101` | Same: destructure `caching`; mark after `add_session_store`. |
| sliding-windowed | `dsl/sliding_windowed_cogrouped.rs:78,101` | Same: destructure `caching`; mark after `add_window_store`. |

**Window-size hazard (already mitigated):** the cache flush rebuilds `Windowed<K>`
from store-key bytes + the store's recorded `window_size_ms`. Each windowed cogroup
registrar already passes the correct window size separate from the retention basis
— notably sliding passes `window_size = time_difference_ms` while retention `size =
2 * time_difference_ms` (`sliding_windowed_cogrouped.rs:96-99`, the 131bfd5c fix).
No registrar change is needed; enabling caching merely activates this
already-correct path. The implementer must still **verify** each registrar's
`window_size_ms` argument under test (a wrong value was latent while uncached).

`registrar` runs once in the merge thunk (`lower_cogroup`, `cogrouped.rs:481`), so
each store is marked exactly once even with N inputs.

## 5. Testing

1. **`to_table` unit (processor):** mirror the existing
   `cached_source_suppresses_immediate_forward` / `uncached_source_forwards_each_record`
   pair in `table.rs` — two same-key records: uncached → 2 forwards; cached → 0
   immediate forwards + exactly 1 record on `flush_cache_into`; cached store holds
   the latest value (read-your-writes).
2. **cogroup cross-input read-your-writes (key new property):** a 2-input
   non-windowed cogroup, both inputs touching the same key in one batch, cached.
   Assert input B's aggregator reads input A's buffered accumulator (final
   accumulated value is the cross-input combination, not B-over-stale), 0 immediate
   forwards, and `flush_cache_into` emits exactly one deduped `Change` per key.
3. **windowed / session / sliding cogroup:** one suppression+flush test each —
   cached store suppresses immediate forwards and flushes one `Change` per
   `Windowed<K>`, with the window key reconstructed at the correct size/end.
4. **changelog-at-flush + restore (embedded broker):** extend the
   `dsl_count_restart_restore_caching_on` pattern with a logging+caching cogroup
   (or `to_table`): produce a repeated-key batch, assert emit-on-commit dedup
   (one record per key, not per update), restart, and assert restore from the
   changelog written at flush.
5. **disabled-path regression:** every existing golden + DSL-integration test stays
   byte-identical under `TopologyTestDriver` (cache forced to 0 → both stores fall
   back to immediate forward). This is the guard that the mark flip is inert when
   caching is off.

## 6. Non-goals (documented; these match Kafka — do NOT implement)

- **Versioned-KTable caching.** Kafka does not cache versioned stores: the
  version-chain (`BTreeMap<validFrom, Option<V>>`) model is incompatible with the
  single-value-per-key record cache. Leaving versioned stores uncached *is* the
  parity-correct behavior.
- **emit-final / on-window-close caching.** Kafka does not cache emit-final
  aggregates: per-update emits are deliberately suppressed and the final record is
  emitted from a close-scan of the store, which the cache-flush-on-commit model
  would contradict. The existing `caching && emit.is_on_update()` gate is correct.
- **cogroup `suppress()` on windowed outputs.** A pre-existing, unrelated deferral
  (windowed cogroup tables carry no suppress factory). Out of scope here.

## 7. Execution shape

Two non-overlapping file sets:

- **Batch A — `to_table`:** `dsl/processors/table.rs`, `dsl/kstream.rs`.
- **Batch B — cogroup:** `dsl/cogrouped.rs`, `dsl/time_windowed_cogrouped.rs`,
  `dsl/session_windowed_cogrouped.rs`, `dsl/sliding_windowed_cogrouped.rs`.

Dispatch A and B in parallel (disjoint files). After both land, run the combined
gate in one place (parallel agents in the same worktree don't cross-verify):

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p crabka-client-streams
cargo build --workspace
```

The embedded-broker restore test (§5.4) and the disabled-path regression sweep
(§5.5) run in the combined gate, not per-batch.

## 8. Coverage map

- §3 `to_table` suppression + mark → Batch A; tests §5.1, §5.5.
- §4 cogroup mark flips (4 kinds) → Batch B; tests §5.2, §5.3, §5.5.
- §4.2 cross-input read-your-writes → test §5.2.
- changelog-at-flush + restore → test §5.4.
- §6 non-goals → no code; assert versioned/emit-final stores remain absent from
  `caching_stores` (a guard test, optional).
