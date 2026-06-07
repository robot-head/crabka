# KIP-1071 Streams Client — Interactive Queries (IQ) Design

**Date:** 2026-06-06
**Status:** Approved (brainstorm)
**Slice:** Interactive Queries — read-only access to live local state stores from outside the topology.

---

## Goal

Let an application read the materialized state of a running `KafkaStreams` instance
from outside the processing topology — the classic Kafka Streams **Interactive Queries**
feature. After this slice, code holding a `KafkaStreams` handle can do:

```rust
let store = streams.key_value_store::<String, i64>("counts")?;
let n: Option<i64> = store.get(&"alice".to_string()).await?;          // point lookup
let all: Vec<(String, i64)> = store.range(&"a".to_string(), &"m".to_string()).await?;
let total: u64 = store.approximate_num_entries().await?;

let wstore = streams.window_store::<String, i64>("windowed-counts")?;
let hits: Vec<(i64, i64)> = wstore.fetch(&"alice".to_string(), 0, 60_000).await?; // (windowStart, value)

let sstore = streams.session_store::<String, i64>("sessions")?;
let sessions: Vec<(Windowed<String>, i64)> = sstore.fetch(&"alice".to_string()).await?;
```

The whole store layer (KV / window / session) is already built and correct; today it is
simply **unreadable** by applications. This slice exposes it.

## Scope

**In scope**
- All three store kinds: `ReadOnlyKeyValueStore`, `ReadOnlyWindowStore`, `ReadOnlySessionStore`.
- **Local active stores only** — query the stores this instance is currently assigned.
  Composite across all locally-owned partitions of a store name. Matches the JVM default
  `StoreQueryParameters` (no `enableStaleStores`).
- Read-only semantics matched to the JVM (`CompositeReadOnly*Store` + the underlying RocksDB
  store contracts), validated capture-first against `TopologyTestDriver`'s store reads.

**Out of scope (explicitly deferred to later slices)**
- **Distributed IQ / metadata**: `metadataForKey`, `streamsMetadataForStore`, `application.server`,
  and any remote-instance RPC. This slice never reaches another instance.
- **Standby / restoring stores** (`StoreQueryParameters.enableStaleStores`) — we have no standby
  tasks yet (separate slice). Queries see only RUNNING active tasks.
- **IQv2** (`KafkaStreams::query(StateQueryRequest)` / `KeyQuery` / `RangeQuery` / `WindowKeyQuery`).
  This slice ships the classic **IQv1** typed read-only views only.
- **Mirror / exotic read methods**, deferred to keep the slice tractable: reverse/backward
  iteration variants (`reverseRange`, `reverseAll`, `backwardFetch`), `prefixScan` (KIP-617),
  the window key-range × time-range `fetch(kFrom,kTo,tFrom,tTo)` combo, and session
  `findSessions(earliestEnd, latestStart)`. The method set below is the faithful core; deferred
  methods are mirror images of shipped ones and can be added incrementally.
- **Cross-key / "all" window + session scans**, deferred for a concrete structural reason:
  - **Window `all()` / `fetchAll(from,to)`** return `KeyValueIterator<Windowed<K>, V>`, which needs
    each result's window **end**. Our window composite key stores only the window **start**
    (`key‖windowStart`), and `WindowBytesStore` carries no window-size field — so the end isn't
    recoverable without threading window size through the store constructor + `add_window_store` +
    changelog wire (out of scope for this slice). The **per-key** window methods this slice ships
    (`fetch(key, t)` point, `fetch(key, from, to)` → `(windowStart, V)`) need no end and are the
    primary window IQ surface.
  - **Session `fetch(from,to)` (key range) / `fetchAll()`** are cross-key prefix scans; this slice
    ships the primary single-key `fetch(key)` (session keys carry both start+end, so `Windowed<K>`
    is fully recoverable). Cross-key session scans are a mechanical follow-up.

## Architecture

The stores live deep inside the spawned supervisor task:
`KafkaStreams` (handle) → `tokio::spawn` supervisor → `StreamThread` → `StreamTask` →
`Graph` → `StoreRegistry { HashMap<String, Box<dyn StateStore>> }`. The `KafkaStreams`
handle holds **no** reference to any of it (only `member_id` / `shutdown` / `JoinHandle` / `state`).
Store access is exclusive `&mut` and the byte backends are **async** and **not** `Arc`/lock-backed
(`InMemoryBytes { map: BTreeMap }`).

**Chosen approach: query-channel actor.** The supervisor already owns
`&mut thread → task → graph.stores`. Add one `select!` arm that services **byte-level** IQ
requests between poll/commit ticks. `KafkaStreams` holds an `mpsc::Sender<IqRequest>`. The typed
view (which knows the Serdes) serializes the lookup key, ships a byte-level request, the supervisor
runs the existing erased-store read, returns a **byte snapshot**, and the view deserializes.

This requires **zero** change to store ownership, the hot dispatch path, or the byte backends —
the bulk of already-stable code is untouched. The rejected alternative (wrap every backend in
`Arc<Mutex>` for a shared live view) has a large blast radius across the dispatch path and every
backend, and forces async-lock discipline on the processing loop for no MVP-visible benefit.

**Trade-offs accepted:** each query is one async round-trip; `range`/`all`/`fetch`-range
**materialize a `Vec` snapshot** (collected under the supervisor's exclusive access) rather than a
lazy iterator; servicing latency is bounded by the current tick's in-flight await. All acceptable
for an MVP and the same order as the JVM's per-thread store lock. The snapshot is documented as the
one intentional divergence from the JVM's lazy `KeyValueIterator`.

### Components

1. **`runtime/iq.rs`** — the IQ wire-internal types and the channel:
   - `IqRequest` — `{ store: String, kind: StoreKind, op: IqOp, reply: oneshot::Sender<IqResponse> }`
     where `IqOp` is a byte-level enum (below). `StoreKind ∈ {KeyValue, Window, Session}`.
   - `IqResponse` — `Result<IqPayload, IqError>` with `IqPayload` byte-level
     (`Value(Option<Bytes>)`, `Entries(Vec<(Bytes, Bytes)>)`, `WindowEntries(Vec<(i64, Bytes)>)`,
     `SessionEntries(Vec<((i64,i64), Bytes)>)`, `Count(u64)`, `Validated`).
   - `IqError` — `StoreNotFound`, `WrongStoreKind { found, requested }`, `NotRunning`,
     `RebalanceInProgress`. Surfaced through `StreamsClientError` (new variant `InteractiveQuery(IqError)`).
   - `IqOp` variants (byte-level, no `K`/`V`):
     - KV: `Validate`, `Get { key: Bytes }`, `Range { lo: Bytes, hi: Bytes }`, `All`, `ApproxCount`.
     - Window: `Validate`, `FetchSingle { key: Bytes, window_start: i64 }`,
       `Fetch { key: Bytes, time_from: i64, time_to: i64 }`.
     - Session: `Validate`, `FetchKey { key: Bytes }`.

2. **`IqQueryable` erased trait** (in `store/`, e.g. `store/iq.rs`) — byte-level reads, implemented
   by the three `*Bytes` stores. All `&self`-or-`&mut self` async, `&[u8]`-in / bytes-out, so no
   `K`/`V` crosses the channel. Default methods return `IqError::WrongStoreKind` so a KV store
   handed a window op fails cleanly. Each method delegates to the store's **existing** key-schema
   logic:
   - `iq_kv_get`, `iq_kv_range`, `iq_kv_all`, `iq_kv_approx_count`
   - `iq_window_fetch_single`, `iq_window_fetch`
     (reuse `WindowKeySchema` range scan over the byte backend)
   - `iq_session_fetch_key`
     (reuse `SessionKeySchema` / `find_sessions` byte logic)
   `StateStore` gains `fn as_iq(&mut self) -> Option<&mut dyn IqQueryable>` (returns `Some(self)`
   for queryable stores) so the supervisor can reach the trait through `&mut dyn StateStore`.

3. **Supervisor servicing** — `StreamThread::serve_iq(req: IqRequest)`:
   - Find every local `StreamTask` whose registry hosts `req.store`. If none → `StoreNotFound`.
   - For a `Validate` op, check the store kind matches `req.kind` (else `WrongStoreKind`) and reply `Validated`.
   - For a read op, call the matching `IqQueryable` method on each hosting store and **concatenate**
     the snapshots across partitions (per-store sorted, composite **not** globally re-sorted — matches
     JVM `CompositeReadOnly*Store`). For `Get`/`FetchSingle`, return the **first** non-empty result.
     For `ApproxCount`, **sum**. Reply the byte payload.
   - In `app.rs`, add `Some(req) = iq_rx.recv()` as a new `select!` arm calling
     `thread.serve_iq(req).await`. When no thread tasks are assigned (mid-rebalance), `serve_iq`
     replies `RebalanceInProgress` / `StoreNotFound` as appropriate.

4. **Composite views** (`runtime/iq_view.rs`) — typed, hold `mpsc::Sender<IqRequest>` + store name + Serdes:
   - `ReadOnlyKeyValueStore<K, V>`: `get(&K) -> Option<V>`, `range(&K,&K) -> Vec<(K,V)>`,
     `all() -> Vec<(K,V)>`, `approximate_num_entries() -> u64`. All `async`, `Result<_, StreamsClientError>`.
   - `ReadOnlyWindowStore<K, V>`: `fetch_single(&K, i64) -> Option<V>`,
     `fetch(&K, i64, i64) -> Vec<(i64, V)>` (window-start, value).
   - `ReadOnlySessionStore<K, V>`: `fetch(&K) -> Vec<(Windowed<K>, V)>`.
   - Each method: serialize args via the held Serdes → send `IqRequest` + `oneshot` → await →
     deserialize the byte payload → return. Window/session keys deserialize into the existing
     `Windowed<K>` (`dsl/windows.rs`).

5. **`KafkaStreams` accessors** (`runtime/app.rs`) — `KafkaStreams` gains a stored
   `iq_tx: mpsc::Sender<IqRequest>` field (created before `tokio::spawn`, receiver moved into the
   supervisor). Three accessors:
   - `key_value_store::<K, V, KS, VS>(name, key_serde, value_serde) -> Result<ReadOnlyKeyValueStore<K,V>, _>`
   - `window_store::<…>(…) -> Result<ReadOnlyWindowStore<K,V>, _>`
   - `session_store::<…>(…) -> Result<ReadOnlySessionStore<K,V>, _>`

   Each is **async** and performs a `Validate` round-trip: errors with `NotRunning` if
   `state != Running`, `StoreNotFound` if absent, `WrongStoreKind` if the named store is a different
   kind — mirroring the JVM's eager `UnknownStateStoreException` / `InvalidStateStoreException`.
   (Serde arguments mirror the rest of the DSL surface, which threads explicit Serdes; an
   ergonomic `Default`-Serde overload is out of scope.)

## Data flow (KV `get`)

```
view.get(&k)
  → key_serde.serialize(&k) = key_bytes
  → IqRequest{ store, kind: KeyValue, op: Get{ key_bytes }, reply }  ──mpsc──▶ supervisor
                                                                              serve_iq:
                                                                                tasks hosting `store`
                                                                                → store.as_iq().iq_kv_get(&key_bytes).await
                                                                                → first Some(value_bytes)
  view ◀── oneshot ── IqResponse::Ok(Value(Some(value_bytes)))
  → value_serde.deserialize(&value_bytes) = v
  → Ok(Some(v))
```

`range`/`all` are identical but return `Entries(Vec<(Bytes,Bytes)>)` concatenated across partition
stores; the view deserializes each pair. Window/session analogous with their payload shapes.

## Semantics to match (capture-first ground truth)

Captured from the JVM `TopologyTestDriver` store reads
(`getKeyValueStore` / `getWindowStore` / `getSessionStore`):

- **KV `get`** absent → `None`.
- **KV `range(lo, hi)`** inclusive both ends; **ascending serialized-byte** order; `lo > hi` → empty.
- **KV `all`** → ascending serialized-byte order.
- **`approximateNumEntries`** → exact for in-memory; **sum** across partition stores.
- **Composite ordering** across partitions: each store's entries are in order, then concatenated —
  **not** globally re-sorted. (Single-partition tests make this moot, but we implement it faithfully.)
- **Window `fetch(key, t)`** (point) → value at exact window start, else `None`.
- **Window `fetch(key, from, to)`** → ascending by window start, inclusive `[from, to]`; iterator of
  `(windowStart: i64, V)`.
- **Session `fetch(key)`** → all sessions for key, ordered by `(end, start)` per the JVM session
  store (matches the existing `find_sessions` store-order test).

Where a JVM detail is version-dependent or undocumented, capture the running cp-kafka 4.1 image's
behavior empirically rather than guessing (per project policy).

## Error handling

- All view methods and accessors return `Result<_, StreamsClientError>`; new variant
  `StreamsClientError::InteractiveQuery(IqError)`.
- `state != Running` → `NotRunning` (eager, in the accessor).
- Store name not assigned locally → `StoreNotFound`.
- Named store is a different kind than requested → `WrongStoreKind { found, requested }`.
- Supervisor gone (handle closing) / `oneshot` dropped → mapped to a `RebalanceInProgress`-style
  error rather than a panic.

## Testing

1. **Per-view unit tests** — drive each view against an in-memory store through a direct/mock
   servicer (an `mpsc` receiver loop over a hand-built `StoreRegistry`), asserting get/range/all/
   count + window/session fetch semantics, including the absent/empty/`lo>hi` edges.
2. **`IqQueryable` byte-level unit tests** — on the three `*Bytes` stores directly (ordering,
   inclusivity, point vs range, composite concat at the trait layer).
3. **JVM golden capture** — a new `InteractiveQueryBehavior.java` capture program in
   `crates/client-streams/tests/jvm-capture/` feeds known records through a TTD topology for each
   store kind, dumps store reads (get/range/all/fetch) to JSON goldens under
   `tests/testdata/iq/`. A Rust parity test asserts our composite
   views reproduce the captured results byte-for-byte (after serde). **No fabricated fixtures** —
   goldens come only from a real JVM run; if the harness can't run in an environment, the parity
   test is `#[ignore]`-gated with a clear reason, never hand-authored.
4. **In-process broker e2e** — spin a single `127.0.0.1` broker, run a counting topology, produce
   records, wait for materialization, then assert `key_value_store("counts").get(k)` returns the
   expected count; one windowed (`window_store(...).fetch(k, from, to)`) and one session
   (`session_store(...).fetch(k)`) assertion likewise. Reuses the existing `eos_broker.rs` / DSL
   broker-integration harness patterns.

## File structure

- `crates/client-streams/src/runtime/iq.rs` — channel types: `IqRequest`/`IqResponse`/`IqOp`/`IqError`/`StoreKind`.
- `crates/client-streams/src/runtime/iq_view.rs` — the three typed composite read-only views.
- `crates/client-streams/src/store/iq.rs` — `IqQueryable` trait + impls for the three `*Bytes` stores.
- `crates/client-streams/src/store/api.rs` — add `StateStore::as_iq`.
- `crates/client-streams/src/runtime/thread.rs` — `StreamThread::serve_iq`.
- `crates/client-streams/src/runtime/app.rs` — `iq_tx` field, supervisor `select!` arm, three accessors.
- `crates/client-streams/src/runtime/mod.rs`, `src/lib.rs` — module wiring + public re-exports
  (`ReadOnlyKeyValueStore`, `ReadOnlyWindowStore`, `ReadOnlySessionStore`).
- `crates/client-streams/src/error.rs` — `InteractiveQuery(IqError)` variant.
- `crates/client-streams/tests/jvm-capture/src/main/java/crabka/capture/InteractiveQueryBehavior.java` — capture program.
- `crates/client-streams/tests/iq_*.rs` — unit + golden + broker e2e tests.

## Task sketch (≈8 tasks, batched by file-disjointness)

- **T1** — `runtime/iq.rs` channel types + `StreamsClientError::InteractiveQuery` + module scaffold.
- **T2** — `IqQueryable` trait (`store/iq.rs`) + impls on KV/window/session `*Bytes` stores +
  `StateStore::as_iq` + byte-level unit tests. *(Disjoint from T1 → batch T1+T2.)*
- **T3** — `StreamThread::serve_iq` + supervisor `select!` arm + `iq_tx` field. *(Depends on T1/T2.)*
- **T4** — `ReadOnlyKeyValueStore` view + `key_value_store` accessor + unit tests.
- **T5** — `ReadOnlyWindowStore` view + `window_store` accessor + unit tests.
- **T6** — `ReadOnlySessionStore` view + `session_store` accessor + unit tests.
  *(T4–T6 share `app.rs` accessors + `iq_view.rs`; fold into one task or serialize — not parallel.)*
- **T7** — JVM capture program + goldens (kv/window/session) + parity tests.
- **T8** — in-process broker e2e + docs (`lib.rs` IQ section + re-exports) + final verification
  (clippy `--all-targets`, fmt, full suite).

## Faithfulness notes

- **Greenfield**: no back-compat shims; change interfaces freely (CLAUDE.md).
- **No wire surface** in this slice (local-only IQ is pure client API), so Kafka byte-exactness is
  not at stake here; the constraint that matters is **store read-semantics parity** with the JVM,
  enforced capture-first.
- The one deliberate divergence from the JVM is **eager `Vec` materialization** of range/fetch
  results instead of a lazy `KeyValueIterator`; documented, MVP-acceptable.
