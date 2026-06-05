# KIP-1071 Streams Client — Sub-project #4d-i: async execution path + pluggable store backend (Turso)

**Date:** 2026-06-04
**Status:** Design approved, pending spec review
**Scope:** Foundational slice of the windowing program (4d) — make the execution
path async and introduce a pluggable state-store backend with a **Turso**
production impl and the existing in-memory impl as the test double. **No
windowing yet.**
**Builds on:** #4 DSL + 4c-i/ii/iii (joins). Branch `streams-4d-async-stores`
(stacked on `streams-4c-ktable-join` / PR #390; rebase onto `main` once #390
merges). Worktree `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`.

## 1. Context & program decomposition

The windowing program (roadmap row 4d) needs **ordered range scans by time**
(fetch every window for a key in `[t0, t1)`) — which the #3 in-memory KV store
(`HashMap<Bytes,Bytes>`, `get`/`put`/`delete` only) cannot do. **Turso** (the
pure-Rust SQLite rewrite) provides exactly that: `WHERE k >= ? AND k < ? ORDER BY
k` over a `BLOB PRIMARY KEY` yields memcmp-ordered iteration. Turso's API is
**async-only**, so adopting it forces the store layer — and therefore the
execution path that touches stores inside `Processor::process` — to become async.

That async refactor is a crate-wide change, so it is split off as the foundational
slice and built **in isolation** before any windowing:

| Slice | Delivers | Depends on |
|---|---|---|
| **4d-i** (this spec) | async execution path + pluggable store backend (Turso prod / in-memory test); existing KV/table stores re-backed; behavior + wire + goldens unchanged | 4c |
| 4d-ii | Turso `WindowStore` + `Windowed<K>` + `windowedBy(TimeWindows)` tumbling/hopping aggregations + windowed changelog | 4d-i |
| 4d-iii | windowed KStream-KStream join (`JoinWindows`, two window stores) | 4d-ii |
| 4d-iv | session store + `windowedBy(SessionWindows)` | 4d-ii |

## 2. Goal & non-goals

### Goal
1. The whole execution path is **async**: `Processor::process`/`init`/`close`
   (via `async-trait`), the #2a graph driver loop, the store traits, and
   `ProcessorContext` store access.
2. State stores are **pluggable** via an object-safe, async byte-level trait
   `ByteKeyValueStore`, with two impls: `InMemoryBytes` (the test double / valid
   prod option) and `TursoBytes` (the production engine).
3. The existing KV/table stores run on the pluggable layer: in-memory backend in
   unit/execution tests, **Turso** backend in the real runtime + broker
   integration test.
4. **No behavior or wire-topology change.** All prior execution tests, the **8
   golden frames (byte-identical)**, doctests, and broker integration stay green.

### Non-goals (deferred)
- **Any windowing** — window/session stores, `Windowed<K>`, `windowedBy`,
  windowed joins → 4d-ii+. 4d-i only *defines* `range` on the byte trait; it adds
  no windowing DSL.
- **Local-state checkpointing / durability** — every assignment **full-replays**
  from the changelog into a clean-slate store; skipping replay via a checkpoint is
  a later slice.
- **Removing the in-memory backend** — it stays as the test double, not deleted.
- **Re-backing non-store state** (offset store, etc.) on Turso.

## 3. Async-ifying the execution path

**Typed `Processor` trait → `async-trait`.** `process`/`init`/`close` become
`async`, written with `#[async_trait]`, which boxes each call to `Pin<Box<dyn
Future + Send>>` and supplies the `Send` bound automatically (required: the
spawned StreamTask future, hence the whole graph, must be `Send` under
`tokio::spawn`). The per-call box is free in practice — the erased dispatch layer
already boxes every record, so no previously alloc-free path gains an allocation.
(`async-trait` chosen over native AFIT because expressing `+ Send` on an
`async fn` in a public, user-implemented trait is still ergonomically painful.)

**Driver loop → async.** The #2a non-recursive `graph.pipe()` loop (pop a buffered
record → run the node → it appends children) becomes `…node.process(ctx,
rec).await`. **`forward()` stays synchronous** (it only pushes to the buffer); only
store-touching methods await, so the loop awaits once per node — the forwarding
model is otherwise unchanged.

**Store traits → async.** `KeyValueStore<K,V>` methods (`get`/`put`/`delete`) and
the `StateStore` methods that touch the backend (`flush`, `apply_changelog`) become
async. In-memory impls return immediately-ready futures; Turso awaits real I/O. The
in-memory changelog-buffer methods (`take_changelog`, `set_logging`,
`changelog_topic`, `name`) stay **synchronous** — they drain/read a `Vec` on the
store, not the backend. `ProcessorContext::get_state_store` returns the
async-capable store handle; processors write `store.get(k).await`.

**Runtime → already async.** `StreamTask::process_once`/`restore` just gain
`.await` on `graph.pipe`/`apply_changelog` — no structural change.

**`TopologyTestDriver` → stays sync via internal `block_on`.** The harness
`block_on`s the async graph internally (`pollster`), so the ~26 existing execution
tests stay plain `#[test]`, unchanged. In-memory ready-futures make that `block_on`
free; a plain `#[test]` has no runtime, so `pollster::block_on` is safe.

**Blast radius:** the `Processor` trait + all ~8 processor impls (mechanical
`async`/`.await`), the erased adapter, the graph loop, `ProcessorContext`, the
store traits + in-memory impl, the test driver. Wire topology + all 8 goldens
untouched — this changes execution, not topology (same regression gate as 4c).

## 4. Pluggability: the byte-store seam

`StoreRegistry::get_kv::<K,V>` recovers a typed store by downcasting `dyn Any` to
a **concrete** type. To keep that a single downcast regardless of backend,
pluggability lives **below** the typed layer:

- **`ByteKeyValueStore`** — object-safe, **async** trait over raw bytes:
  `async fn get(&self, key: &[u8]) -> Option<Bytes>`,
  `async fn put(&mut self, key: Bytes, value: Bytes)`,
  `async fn delete(&mut self, key: &[u8]) -> Option<Bytes>`,
  `async fn range(&self, lo: &[u8], hi: &[u8]) -> Vec<(Bytes, Bytes)>` (half-open
  `[lo, hi)`, memcmp order; **defined now, used by 4d-ii's window store** — KV
  stores don't call it).
- **`InMemoryBytes`** — a `BTreeMap<Bytes,Bytes>` (ordered, so it serves `range`
  too); all methods return ready futures.
- **`TursoBytes`** — one Turso table `kv (k BLOB PRIMARY KEY, v BLOB NOT NULL)`
  (a normal rowid table — `WITHOUT ROWID` is insert-only in turso 0.6); `put` is
  `INSERT … ON CONFLICT(k) DO UPDATE`; `range` is `WHERE k>=? AND k<? ORDER BY k`.

- **`KeyValueBytesStore<K,V>`** — the **single** typed concrete type the registry
  holds and downcasts to. Owns `Box<dyn ByteKeyValueStore>` + boxed serdes + the
  changelog buffer + logging flag + name + changelog topic. All K/V-typed logic
  (serialize through serdes, buffer changelog entries, apply tombstones) lives
  here, **written once**, backend-agnostic. Implements `KeyValueStore<K,V>` +
  `StateStore`. Convenience constructor `KeyValueBytesStore::in_memory(name, ks,
  vs, changelog)` for tests.

This refactors today's `InMemoryKeyValueStore<K,V>` into `KeyValueBytesStore<K,V>`
+ an `InMemoryBytes` backend. `get_kv::<K,V>` downcasts to exactly
`KeyValueBytesStore<K,V>`; the backend swaps underneath. 4d-ii's
`WindowBytesStore<K,V>` reuses the **same** `ByteKeyValueStore` backends.

**Churn:** code/tests constructing `InMemoryKeyValueStore::<K,V>::new(...)`
directly (e.g. the `ktable_join` unit test) move to
`KeyValueBytesStore::in_memory(...)`.

## 5. Backend selection, DB layout & restore

**Selection.** A `StoreBackend` config — `InMemory` or `Turso { state_dir }` —
threaded from `KafkaStreams::start` / the builder down to store instantiation. The
`StoreFactory` closure gains the backend at instantiation: in-memory ignores the
dir; Turso opens a DB under `state_dir`. `TopologyTestDriver` + all unit/execution
tests default to `InMemory`; real `KafkaStreams` + the broker-integration test use
`Turso`. Golden tests build only the wire topology (no store instantiation) — untouched.

**DB layout.** One DB file per store at `<state_dir>/<app-id>/<store>.db` (matches
Kafka's per-store model and the existing per-store factory). One Turso
`Connection` per store, created on the owning task thread (no cross-thread
sharing).

**Restore = identical to #3.** To keep "empty store → replay changelog 0→HWM"
semantics exactly, the task does a clean-slate `DROP TABLE IF EXISTS …; CREATE
TABLE …` at restore, then replays via `apply_changelog`. Greenfield-correct (no
local-state checkpoint yet — every assignment full-replays). `InMemory` is
naturally clean each start. Changelog produce/restore plumbing (partition-pinned,
0→HWM) is unchanged from #3.

## 6. Turso adoption — risks & the gating spike

`turso 0.6` is **beta**; adoption is acceptable here because a state store is a
**rebuildable cache** (the changelog is the source of truth — a corrupt/missing DB
is recovered by replay, not data loss). 4d-i **opens with a spike (plan Task 0)**,
a throwaway test that must prove, before the refactor is committed:

1. `turso = "0.6"` (default features — **not** the `sync` feature, which is cloud
   replication) compiles in the workspace.
2. **`turso::Connection: Send`** — required because the spawned StreamTask future
   (holding the `StoreRegistry`) must be `Send`.
3. Turso futures resolve correctly when **`.await`ed under tokio** (not only under
   `pollster`) — i.e. it completes its I/O within the poll or wakes properly.
4. CRUD + a `WHERE k>=? AND k<? ORDER BY k` range scan returns memcmp-ordered rows.

If (2) or (3) fail, the fallback (a dedicated per-connection thread, or
`rusqlite`) is swapped **behind `ByteKeyValueStore`** with no change above the
seam. **The spike gates committing to the refactor.**

**Dependencies:** `turso = "0.6"`, `async-trait`, `pollster` (only for the sync
`TopologyTestDriver` boundary).

## 7. Testing strategy (gates)

1. **Spike (Task 0)** — §6 (1)–(4). Gates the slice.
2. **Backend contract test** — one parametrized suite running the same KV contract
   (put/get/delete, changelog buffer round-trip, `apply_changelog` restore,
   ordered `range`) against **both** `InMemoryBytes` and `TursoBytes` → the two
   backends are provably interchangeable.
3. **Async-refactor regression (primary gate)** — the existing **26 execution
   tests, 8 golden frames (byte-identical), doctests, and broker integration stay
   green**. `TopologyTestDriver` stays sync via internal `block_on`, so execution
   tests don't change shape.
4. **Turso end-to-end** — the broker-integration test (or a sibling) runs the real
   runtime with `StoreBackend::Turso`: fetch→process→produce with state on Turso,
   plus a **restart-restore** assertion (drop the task, reassign, confirm the store
   rebuilds from the changelog into a fresh Turso DB and counts resume).
5. `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`,
   `cargo build --workspace`.

## 8. Success criteria
- Execution path fully async (`async-trait` `Processor`, async driver loop, async
  store traits); `ProcessorContext::get_state_store().get(k).await` works.
- Stores pluggable via `ByteKeyValueStore` with `InMemoryBytes` + `TursoBytes`;
  existing KV/table stores run on it (in-memory in tests, Turso in the runtime).
- **All prior tests + 8 goldens unchanged**; backend contract test green; Turso
  end-to-end + restart-restore proven.
- clippy `--all-targets` + fmt clean; `cargo build --workspace`.
- A short async-store / backend note in `lib.rs`.

## 9. Open points for the plan
- **`async-trait` ripple ordering** — converting the `Processor` trait first makes
  every processor impl fail to compile until updated; the plan converts the trait +
  erased adapter + driver loop + all processors in one coherent task (they don't
  compile independently). Sequence: spike → store traits/byte seam → `Processor`
  async + driver loop + all processors + context → backend selection/runtime wiring
  → Turso end-to-end → docs.
- **`dyn ByteKeyValueStore` object-safety under `async-trait`** — confirm the
  boxed-future trait object composes with the `KeyValueBytesStore` wrapper's own
  `async-trait` methods (nested boxed futures are fine; verify lifetimes on the
  `range` borrow).
- **Turso `Connection` ownership across restore** — the clean-slate DROP/CREATE
  runs on the task thread before the (async) replay loop; confirm a single
  `Connection` per store is reused for restore + processing (not reopened).
- **`Send` of the StreamTask future** — re-verify after the refactor that
  `tokio::spawn(thread.run())` still type-checks (the boxed processor futures +
  Turso `Connection` must all be `Send`); this is the spike's load-bearing result.
- **Test-driver `block_on` choice** — `pollster` vs `futures::executor::block_on`;
  either works (no reactor needed). Pick one in the plan.
