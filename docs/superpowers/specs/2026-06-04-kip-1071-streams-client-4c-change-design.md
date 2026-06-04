# KIP-1071 Streams Client — Sub-project #4c-i: KTable `Change<old,new>` propagation + `toTable`

**Date:** 2026-06-04
**Status:** Design approved, pending spec review
**Scope:** First sub-slice of the joins+windowing program (4c/4d) — the foundational KTable change-semantics refactor.
**Builds on:** #4 first slice (PR #387: KStream/KTable DSL + optimizer + JVM-byte-exact golden frames). Branch `streams-4c-change` (stacked on `worktree-streams-4-dsl`).

## 1. Context & program decomposition

#4's first slice delivered the KStream/KTable DSL, but a KTable currently forwards
**new-value-only** — `KTable::filter` drops non-matching rows with no tombstone,
and downstream KTable ops can't see deletions or prior values. The code already
flags this: `Change<V>` propagation is deferred here.

The joins+windowing program (roadmap rows 4c/4d) decomposes by dependency:

| Slice | Delivers | Depends on |
|---|---|---|
| **4c-i** (this spec) | `Change<old,new>` propagation + `toTable` | #4 |
| 4c-ii | Joins — KStream-KTable, KTable-KTable | 4c-i |
| 4d | Windowing — window/session stores, windowed aggregations, windowed KStream-KStream join | 4c-i |

`Change` is foundational: KTable-KTable joins and correct downstream KTable
operators all require it. This slice does the refactor in isolation (lowest risk)
so the joins build on a correct base.

## 2. Goal & non-goals

### Goal
1. A KTable is internally a **change stream**: every KTable node flows
   `Record<K, Change<V>>` where `Change<V> { old: Option<V>, new: Option<V> }`.
2. **Tombstones propagate**: `filter` emits a tombstone (`new: None`) when a row
   that previously passed now fails; materialized stores delete on tombstone +
   produce a null changelog record; downstream KTable ops handle old+new.
3. **`KStream::to_table(Materialized)`** materializes a stream into a KTable.
4. The public `KTable<K,V>` / `KStream<K,V>` API is unchanged; **no wire-topology
   change for existing operators** (the 5 golden frames stay byte-identical).

### Non-goals (deferred)
- **Joins** (KStream-KTable, KTable-KTable) → 4c-ii.
- **Windowing** + windowed joins → 4d.
- **`toStream` tombstone→null output** — `to_stream()` forwards updates and *drops*
  tombstones from the output stream (a typed `Record<K,V>` can't hold a null
  value; full null-output needs crate-wide `Option<V>` plumbing). Joins read
  KTable *stores*, not `toStream` output, so this isn't on the 4c-ii path.
- **`suppress` / record caching** (KIP-328), foreign-key joins.

## 3. `Change<V>` model

`Change<V> { old: Option<V>, new: Option<V> }` lives in `dsl/processors/change.rs`
(`Send + 'static`, carried erased like any record value):
- Normal update: `Change { old: prev_or_None, new: Some(v) }`.
- Tombstone: `Change { old: Some(prev), new: None }`.

KTable nodes flow `Record<K, Change<V>>`; the public `KTable<K,V>` handle stays
typed on `V` (the lowering uses `Change<V>` for the KTable processors' VIn/VOut).
Boundaries convert: `to_stream()`/sink extract `new`; aggregations and
`table()`/`to_table()` **emit** `Change` (old = prior store value).

## 4. Refactored KTable processors (`dsl/processors/table.rs`, `aggregate.rs`)

- **`TableSource` / `to_table`** — on `(k, v)`: `old = store.get(k)`; non-null `v` →
  `store.put(k, v)`, forward `Change{old, new: Some(v)}`; null source value →
  `store.delete(k)`, forward `Change{old, new: None}`.
- **`count` / `reduce` / `aggregate`** — `old = store.get(k)`, `new = agg(...)`,
  `store.put`, forward `Change{old, new: Some(new)}`.
- **`KTable::map_values` (materialized)** — input `Change{old,new}` → map each side
  (`old.map(f)`, `new.map(f)`), update store, forward the mapped `Change`. The
  non-materialized view maps without storing.
- **`KTable::filter`** — input `Change{old,new}` → `old_p = old.filter(p)`,
  `new_p = new.filter(p)`; update store; forward `Change{old_p, new_p}`, **including
  a tombstone (`new_p: None`) when a previously-passing row now fails**.
- **`KTable::to_stream`** — extract `new` → `KStream<K, V>`; **drop tombstones**
  (documented; see §2 non-goals).

Materialized stores apply `Change`: `put` on `new: Some`, `delete` on `new: None`;
the changelog produce emits a null value for a tombstone (#3 already supports
null-value changelog entries).

## 5. `to_table` (`dsl/kstream.rs`, `dsl/processors/table.rs`)

`KStream::to_table<KS,VS>(Materialized<KS,VS>) -> KTable<K,V>`:
- Records a node + lower thunk: `add_processor(name, KStreamToTableProcessor{store})`
  + `add_state_store(store_name, ks, vs, [name])`.
- `KStreamToTableProcessor` — per record: `old = store.get(k)`, `store.put(k, v)`,
  forward `Change{old, new: Some(v)}`.
- Store name: `Materialized.store_name` if set, else the JVM auto name
  (`KSTREAM-TOTABLE-STATE-STORE-<id>` — **pinned by the captured fixture**);
  changelog `<app>-<store>-changelog`.
- This is the only topology-changing piece → a **6th golden frame** (`to_table`).

## 6. JVM capture & golden frames

Extend `tests/jvm-capture/Capture.java` with a `to_table` topology
(`stream("in").toTable(Materialized.as("store")).toStream().to("out")`), capture
via the existing Docker Kafka-Streams 4.1 harness (mechanism A, cross-validated
against a live broker), commit `testdata/golden/dsl/to_table.topology.json`.

**The existing 5 golden frames (`stateless_chain`, `count`, `repartition_merge`,
`table_reuse`, `branch_merge`) MUST stay byte-identical** — the `Change` refactor
changes execution, not topology. This is the primary regression gate.

## 7. Testing strategy (gates)

1. **`Change` unit tests** — `Change` construction; tombstone vs update.
2. **Execution tests** (`TopologyTestDriver`, `tests/dsl_execution.rs`):
   - `filter` tombstone: pipe a row that matches, then an update that makes it
     fail the predicate → the downstream materialized store has the key **deleted**
     (assert via `get_key_value_store`).
   - `map_values` over a delete: a tombstone propagates (store delete).
   - `to_table`: a stream materializes into a KTable whose store reflects the
     latest value per key (last-write-wins).
   - aggregate→KTable `Change` flow still produces correct counts (regression).
3. **Golden:** `to_table` byte-matches the JVM fixture; **the 5 existing golden
   frames stay byte-identical**.
4. **Regression:** all #1/#2/#3/#4 execution + broker integration tests stay green.

## 8. Success criteria
- KTable internally propagates `Change<old,new>`; `filter` emits tombstones;
  materialized stores delete on tombstone; `to_table` works.
- `to_table` golden frame byte-matches captured JVM 4.1 output; the 5 prior golden
  frames unchanged.
- `cargo test -p crabka-client-streams` green; `cargo clippy --workspace
  --all-targets -- -D warnings` + `cargo fmt --check` clean; `cargo build
  --workspace`.
- A documented `to_table` / Change example or doctest in `lib.rs`.

## 9. Open points for the plan
- **`Change` erasure in the driver/test-driver** — the test driver's `read_output`
  deserializes the value; for KTable-derived streams the value is the extracted
  `new` (post-`to_stream`), so existing `read_output` works. Confirm the
  repartition/loop-back path in the test driver handles `Change`-typed internal
  nodes (KTable nodes aren't repartition sources, so likely unaffected).
- **`to_table` counter position** — the exact store-name index + whether `toTable`
  inserts a repartition when the key is unchanged (it shouldn't) — pinned by the
  fixture.
- **Aggregate `Change` ripple** — making `count`/`reduce`/`aggregate` emit
  `Change<VA>` changes their VOut from `VA` to `Change<VA>`; `to_stream` after an
  aggregation must extract `new`. Confirm the existing count/reduce execution
  tests still pass (they assert the new value, which `to_stream` extracts).
