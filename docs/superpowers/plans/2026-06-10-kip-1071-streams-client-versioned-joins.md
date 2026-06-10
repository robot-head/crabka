# Versioned KTables Slice 2 — Joins (KIP-914 join half + KIP-923 grace) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make joins over versioned KTables temporally correct: stream–table joins look up the table value as-of the stream record's timestamp, an optional grace period buffers out-of-order stream records, and table–table joins suppress out-of-order updates.

**Architecture:** Three behaviors land on the existing join operators in `crates/client-streams`. Stream–table routing picks an as-of processor when the table is versioned (no new topology node/store). A new `Joined` config + `join_table_with` methods add a KIP-923 grace buffer (a new changelog-backed store + a suppress-style stream-time-driven flush processor). Table–table joins gain a `versioned` gate that drops out-of-order changes. Ground truth is an empirical Kafka-Streams 4.1.0 Docker capture, replayed byte-for-byte.

**Tech Stack:** Rust 2024, `async-trait`, `tokio`, `bytes`; crate `crabka-client-streams`. Tests: `cargo nextest` / `cargo test -p crabka-client-streams`. JVM capture: Gradle harness under `crates/client-streams/tests/jvm-capture`.

---

## Background the engineer must know

- **The triplet pattern.** Stateful DSL features = a DSL surface + a processor + a store(+codec). Slice 1 added the versioned store; this slice reuses it for as-of lookups and adds **one** new store (the grace buffer).
- **Slice 1 facts (already merged, do not re-implement):**
  - `Materialized::as_versioned(name, history_retention_ms)` sets `Materialized.versioned: Option<VersionedConfig>` (`config.rs`).
  - `VersionedKeyValueStore<K,V>` trait (`store/versioned.rs`): `get(key) -> Option<VersionedRecord<V>>` (latest), `get_as_of(key, as_of) -> Option<VersionedRecord<V>>`. `VersionedRecord { value, valid_from, valid_to }`. `get_as_of` returns `None` if the version is a tombstone or `as_of` predates the oldest retained version.
  - `ProcessorContext::get_versioned_store::<K,V>(name) -> Option<&mut dyn VersionedKeyValueStore<K,V>>` (`processor/api.rs`).
  - `VersionedKTableSourceProcessor` (`dsl/processors/table.rs`) forwards a `Change` for **every** record, including out-of-order ones. Slice 2's table–table gate (Task 4) is what suppresses out-of-order downstream — NOT the source.
- **Stream-time-driven flushing** is modeled by `KTableSuppressProcessor` (`dsl/processors/suppress.rs`): track `observed_stream_time = max(observed, r.timestamp)`, buffer into a registered store, then evict everything with `buffer_time ≤ observed_stream_time − wait_ms`. The grace buffer (Task 7) is the same shape with `wait_ms = grace_ms` and `buffer_time = record.ts`.
- **Greenfield (per CLAUDE.md):** no back-compat shims. When a signature changes, change it. Match Kafka byte-exactness; capture empirically when undocumented.
- **Codegen note:** this slice touches **no** protocol schemas, so `tools/regenerate.sh` is NOT involved.
- **CI gates:** `cargo fmt --check`, `cargo clippy --workspace --all-targets -D warnings`, full `cargo test`. Run all three before claiming done.

### Behavior contract (from the spec — the tests encode these)

1. **Stream–table as-of:** versioned table → `get_as_of(key, streamRec.ts)`; null result = miss (inner skips, left passes `None`); output forwarded at `streamRec.ts`. Non-versioned table = unchanged latest `get`.
2. **Grace (KIP-923):** `grace_ms` set → buffer each stream record; drain `bufTs ≤ streamTime − grace_ms` in ascending `(ts, seq)` order, doing the as-of lookup at drain time, forwarding at `bufTs`. A record already late on arrival drains in the same pass. Build-time asserts: grace requires a versioned table and `grace_ms < history_retention_ms`.
3. **Table–table out-of-order:** still `get(key)` (latest) for the other side; when an input side is versioned, an out-of-order change (record ts < that side's versioned store latest `valid_from`) emits nothing.

---

## File Structure

| File | Responsibility | Tasks |
|------|----------------|-------|
| `src/dsl/ktable.rs` | `versioned_retention_ms` field on `KTable` + propagation + table–table `versioned` wiring | 1, 4 |
| `src/dsl/builder.rs` | `table_explicit` sets `versioned_retention_ms` from `Materialized` | 1 |
| `src/dsl/processors/join.rs` | `KStreamKTableJoinAsOfProcessor` (as-of stream–table) | 2 |
| `src/dsl/kstream.rs` | route `join_table_impl` to as-of; add `join_table_with`/`left_join_table_with` + grace wiring + asserts | 2, 8 |
| `src/dsl/processors/ktable_join.rs` | `versioned` out-of-order gate on This/Other processors | 4 |
| `src/dsl/config.rs` | `Joined` grace config | 5 |
| `src/store/join_grace_buffer.rs` (new) | time-ordered stream-record buffer store + changelog | 6 |
| `src/store/registry.rs`, `src/store/mod.rs`, `src/topology/*` | register/connect the grace buffer store | 6 |
| `src/dsl/processors/join_grace.rs` (new) | grace buffer flush processor | 7 |
| `tests/jvm-capture/src/main/java/crabka/capture/*.java`, `run.sh` | 3 capture programs | 3 |
| `tests/versioned_joins_golden.rs` (new) | golden replay (topology + behavioral + changelog) | 9, 10, 11 |

---

## Batch plan (parallel where file sets are disjoint)

- **Batch 1 (parallel):** Task 1 (handle field) ‖ Task 3 (JVM capture).
- **Batch 2 (parallel):** Task 2 (as-of processor + route) ‖ Task 4 (table–table gate).
- **Batch 3:** Task 5 (Joined config) ‖ Task 6 (buffer store) ‖ Task 7 (grace processor), THEN Task 8 (DSL wiring — depends on 5/6/7).
- **Batch 4 (parallel):** Task 9 ‖ Task 10 ‖ Task 11 (golden replays), THEN Task 12 (full-suite reconciliation).

Task 3 must complete before Batch 4 (goldens are the replay fixtures) and informs the **buffer store name + changelog config constants** consumed by Task 6 (a real empirical dependency, not a placeholder — capture first).

---

## Task 1: `versioned_retention_ms` on the `KTable` handle (C1)

**Files:**
- Modify: `src/dsl/ktable.rs` (struct `KTable`, `new`, `with_key_serde`, `with_value_serde`, `with_window_grace` neighborhood)
- Modify: `src/dsl/builder.rs` (`table_explicit`, where the returned `KTable` is built)
- Test: `src/dsl/ktable.rs` `#[cfg(test)]`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/dsl/ktable.rs` (create one if absent, mirroring `config.rs` tests):

```rust
#[test]
fn versioned_table_handle_carries_retention() {
    use crate::dsl::builder::StreamsBuilder;
    use crate::dsl::config::Materialized;
    use crate::processor::serde::{I64Serde, StringSerde};

    let b = StreamsBuilder::new();
    let t = b.table_explicit::<StringSerde, I64Serde>(
        "in",
        crate::processor::serde::Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_versioned("vt", 600_000),
    );
    assert_eq!(t.versioned_retention_ms, Some(600_000));

    let plain = b.table_explicit::<StringSerde, I64Serde>(
        "in2",
        crate::processor::serde::Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_store("pt"),
    );
    assert_eq!(plain.versioned_retention_ms, None);
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p crabka-client-streams versioned_table_handle_carries_retention`
Expected: FAIL — `no field versioned_retention_ms on type KTable`.

- [ ] **Step 3: Add the field + propagate it**

In `src/dsl/ktable.rs`, add the field to `struct KTable` (next to `window_grace_ms`):

```rust
    /// `Some(history_retention_ms)` when this table is materialized into a
    /// versioned store (KIP-889). Drives as-of stream–table join lookups
    /// (KIP-914) + the table–table out-of-order gate + grace validation.
    /// Mirrors `window_grace_ms`. `None` for non-versioned / derived tables.
    pub(crate) versioned_retention_ms: Option<i64>,
```

In `KTable::new`, initialise it (next to `window_grace_ms: None,`):

```rust
            versioned_retention_ms: None,
```

In `with_key_serde` and `with_value_serde`, carry it through (next to `window_grace_ms: self.window_grace_ms,`):

```rust
            versioned_retention_ms: self.versioned_retention_ms,
```

Add a setter next to `with_window_grace`:

```rust
    /// Tag this table with its versioned-store history retention (set by
    /// `builder.table` when `Materialized::as_versioned` was used). Read by the
    /// stream–table join (as-of routing) and table–table join (out-of-order gate).
    #[must_use]
    pub(crate) fn with_versioned_retention(mut self, retention_ms: Option<i64>) -> Self {
        self.versioned_retention_ms = retention_ms;
        self
    }
```

- [ ] **Step 4: Set it in `table_explicit`**

In `src/dsl/builder.rs::table_explicit`, find where the `materialized` config is read and where the `KTable` is returned. Compute the retention once:

```rust
        let versioned_retention =
            materialized.versioned.map(|v| v.history_retention_ms);
```

(place it alongside the existing `materialized.store_name` / `materialized.versioned` reads), and chain `.with_versioned_retention(versioned_retention)` onto the returned `KTable` builder expression (the same place `.with_source_topic(...)` / serde setters are chained). If the return constructs via `KTable::new(...)`, append `.with_versioned_retention(versioned_retention)`.

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p crabka-client-streams versioned_table_handle_carries_retention`
Expected: PASS.

- [ ] **Step 6: fmt + clippy + commit**

```bash
cargo fmt -p crabka-client-streams
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
git add src/dsl/ktable.rs src/dsl/builder.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(client-streams): carry versioned history-retention on the KTable handle (KIP-914 join prep)"
```

---

## Task 2: As-of stream–table join processor + routing (C2)

**Files:**
- Modify: `src/dsl/processors/join.rs` (add `KStreamKTableJoinAsOfProcessor` + unit tests)
- Modify: `src/dsl/kstream.rs` (`join_table_impl` routes on `table.versioned_retention_ms`)
- Test: `src/dsl/processors/join.rs` `#[cfg(test)]`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/dsl/processors/join.rs`. This reuses the existing `Dispatch`/`ProcessorContext` harness but a **versioned** store seeded with two versions. Add a helper + two tests:

```rust
    use crate::store::versioned::VersionedBytesStore;
    use crate::store::api::StateStore; // if not already imported

    /// Versioned store "vt" with key "a": value 10 valid_from=100, value 20 valid_from=200.
    async fn make_versioned_stores() -> StoreRegistry {
        let mut stores = StoreRegistry::default();
        let mut v = VersionedBytesStore::<String, i64>::in_memory(
            "vt".into(),
            1_000_000,
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-vt-changelog".into(),
        );
        v.put("a".to_string(), Some(10), 100).await;
        v.put("a".to_string(), Some(20), 200).await;
        stores.insert(Box::new(v));
        stores
    }

    /// Run one record through the as-of processor at timestamp `ts`.
    async fn run_one_asof(
        proc: &mut KStreamKTableJoinAsOfProcessor<
            String, i64, i64, i64,
            impl Fn(&i64, Option<&i64>) -> i64 + Send + 'static,
        >,
        stores: &mut StoreRegistry,
        key: &str,
        value: i64,
        ts: i64,
    ) -> Option<i64> {
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let globals = crate::runtime::global::GlobalStateManager::default();
        let rc = RecordContext { topic: "in".into(), partition: 0, offset: 0, timestamp: ts };
        let mut scheds = Vec::new();
        let mut dispatch = Dispatch {
            buffer: &mut buffer, children: &children, output: &mut output,
            record_ctx: &rc, stores, globals: &globals, node_idx: 0,
            schedules: &mut scheds, sched_stream_time: i64::MIN, sched_wall_clock: 0,
        };
        let mut ctx = ProcessorContext::<'_, '_, String, i64>::new(&mut dispatch);
        proc.process(&mut ctx, Record::new(Some(key.to_string()), value, ts)).await;
        buffer.pop_front().map(|(_, rec)| *rec.value.downcast::<i64>().unwrap())
    }

    #[tokio::test]
    async fn asof_inner_picks_version_at_record_ts() {
        let mut stores = make_versioned_stores().await;
        let mut proc = KStreamKTableJoinAsOfProcessor {
            table_store: "vt".into(),
            joiner: |v: &i64, vt: Option<&i64>| v + vt.copied().unwrap_or(0),
            emit_on_miss: false,
            _pd: PhantomData::<fn() -> (String, i64, i64, i64)>,
        };
        // ts=150 → version valid at 150 is 10 → 1 + 10 = 11
        check!(run_one_asof(&mut proc, &mut stores, "a", 1, 150).await == Some(11));
        // ts=250 → version 20 → 1 + 20 = 21
        check!(run_one_asof(&mut proc, &mut stores, "a", 1, 250).await == Some(21));
        // ts=50 → predates first version → inner miss → None
        check!(run_one_asof(&mut proc, &mut stores, "a", 1, 50).await == None);
    }

    #[tokio::test]
    async fn asof_left_emits_none_on_miss() {
        let mut stores = make_versioned_stores().await;
        let mut proc = KStreamKTableJoinAsOfProcessor {
            table_store: "vt".into(),
            joiner: |v: &i64, vt: Option<&i64>| v + vt.copied().unwrap_or(0),
            emit_on_miss: true,
            _pd: PhantomData::<fn() -> (String, i64, i64, i64)>,
        };
        // ts=50 → miss → joiner gets None → 1 + 0 = 1
        check!(run_one_asof(&mut proc, &mut stores, "a", 1, 50).await == Some(1));
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p crabka-client-streams -- asof_`
Expected: FAIL — `KStreamKTableJoinAsOfProcessor` not found.

- [ ] **Step 3: Implement the processor**

Add to `src/dsl/processors/join.rs` (after `KStreamKTableJoinProcessor`):

```rust
/// As-of stream–table join processor (KIP-914). Identical to
/// [`KStreamKTableJoinProcessor`] except the table lookup is a versioned
/// `get_as_of(key, streamRec.ts)` — the table value valid *as of the stream
/// record's timestamp*. A null as-of result is treated like a miss (inner skips,
/// left passes `None`). Output is forwarded at the stream record's timestamp.
#[allow(dead_code)]
pub(crate) struct KStreamKTableJoinAsOfProcessor<K, V, VT, VO, F> {
    pub table_store: String,
    pub joiner: F,
    pub emit_on_miss: bool,
    pub _pd: Marker<(K, V, VT, VO)>,
}

#[async_trait]
impl<K, V, VT, VO, F> Processor<K, V, K, VO> for KStreamKTableJoinAsOfProcessor<K, V, VT, VO, F>
where
    K: std::any::Any + Send + Sync + Clone,
    V: Send + 'static,
    VT: Send + 'static,
    VO: std::any::Any + Send + Clone,
    F: Fn(&V, Option<&VT>) -> VO + Send + 'static,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, VO>, r: Record<K, V>) {
        let key = r.key.expect("join requires a non-null key");
        let ts = r.timestamp;
        let vt = match ctx.get_versioned_store::<K, VT>(&self.table_store) {
            Some(s) => s.get_as_of(&key, ts).await.map(|rec| rec.value),
            None => None,
        };
        if vt.is_some() || self.emit_on_miss {
            let out = (self.joiner)(&r.value, vt.as_ref());
            ctx.forward(Record::new(Some(key), out, ts));
        }
    }
}
```

- [ ] **Step 4: Run the unit tests to verify they pass**

Run: `cargo test -p crabka-client-streams -- asof_`
Expected: PASS (3 assertions in inner, 1 in left).

- [ ] **Step 5: Route `join_table_impl` to the as-of processor when versioned**

In `src/dsl/kstream.rs::join_table_impl`, the lowering thunk currently always builds `KStreamKTableJoinProcessor`. Capture the table's versioned flag *before* the thunk (the `table` handle isn't available inside the thunk):

```rust
        let table_versioned = table.versioned_retention_ms.is_some();
```

(place it next to the existing `let table_store = ...` / `let table_src = ...` reads). Then inside the thunk, branch the `add_processor` call. Replace the single `KStreamKTableJoinProcessor` registration with:

```rust
            let h = if table_versioned {
                state.topology.add_processor::<K, V, K, VO, _, _, _>(
                    join_name.clone(),
                    {
                        let store_for_proc = store_for_proc.clone();
                        let lf = lf.clone();
                        move || crate::dsl::processors::join::KStreamKTableJoinAsOfProcessor {
                            table_store: store_for_proc.clone(),
                            joiner: lf.clone(),
                            emit_on_miss,
                            _pd: PhantomData,
                        }
                    },
                    [parent.clone()],
                )
            } else {
                state.topology.add_processor::<K, V, K, VO, _, _, _>(
                    join_name.clone(),
                    move || KStreamKTableJoinProcessor {
                        table_store: store_for_proc.clone(),
                        joiner: lf.clone(),
                        emit_on_miss,
                        _pd: PhantomData,
                    },
                    [parent],
                )
            };
```

Adjust the surrounding `let parent = ...` / `let lf = ...` / `store_for_proc` bindings so both closures can clone them (clone `parent` for the first branch as shown; the existing code already clones `store_for_thunk`). Keep `connect_processor_store(h.name(), &store_for_thunk)` and the copartition declaration **unchanged** — node name, store wiring, subtopology, copartition are identical for versioned and non-versioned. The only byte difference is the store's own versioned changelog config (emitted by slice 1's table source).

- [ ] **Step 6: Verify topology is unchanged (regression guard)**

Run the existing stream–table join topology/wire tests:

Run: `cargo test -p crabka-client-streams -- join_table`
Expected: PASS — no node-name or store-name shift (the as-of route adds no node).

- [ ] **Step 7: fmt + clippy + commit**

```bash
cargo fmt -p crabka-client-streams
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
git add src/dsl/processors/join.rs src/dsl/kstream.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(client-streams): as-of stream-table join for versioned tables (KIP-914)"
```

---

## Task 3: JVM capture programs for the three behaviors (C5 — capture)

**Files:**
- Create: `tests/jvm-capture/src/main/java/crabka/capture/StreamTableAsOfBehavior.java`
- Create: `tests/jvm-capture/src/main/java/crabka/capture/StreamTableGraceBehavior.java`
- Create: `tests/jvm-capture/src/main/java/crabka/capture/TableTableVersionedBehavior.java`
- Modify: `tests/jvm-capture/run.sh` (add `--versioned-joins` targets)
- Create (output, committed): `tests/testdata/versioned_joins/{asof,grace,tabletable}.json`

> Model these on the existing `VersionedTableBehavior.java` + its `run.sh` target and the `testdata/` layout used by slice 1. Read `VersionedTableBehavior.java` and the slice-1 golden test first to copy the exact capture/serialization shape (topology description, output records, changelog records).

- [ ] **Step 1: Write `StreamTableAsOfBehavior.java`**

Topology: `builder.stream("stream-in")` join `builder.table("table-in", Materialized.as("vt").withVersionedStoreSupplier(...history=10min...))` with an inner joiner, output to `out`. Drive records:
- table: `(a,10)@100`, `(a,20)@200`
- stream: `(a,1)@150` → expect `11`; `(a,1)@250` → expect `21`; `(a,1)@50` → expect **no output** (inner, predates).
Capture the topology description, the output records (with timestamps), and assert against them in Rust later.

- [ ] **Step 2: Write `StreamTableGraceBehavior.java`**

Topology: same versioned table; stream join uses `Joined.with(...).withGracePeriod(Duration.ofMillis(GRACE))` with `GRACE < history`. Drive **out-of-order** stream records so the buffer reorders them, plus one already-late record. Record the **buffer store name** the JVM mints and its **changelog config** (cleanup.policy, retention.ms) from the topology description — these are the constants Task 6 consumes. Capture output records + ordering.

- [ ] **Step 3: Write `TableTableVersionedBehavior.java`**

Topology: two versioned tables joined (inner). Drive an in-order update (expect a join result) and an **out-of-order** update on one side (expect **no** new join result). Capture output records.

- [ ] **Step 4: Add `run.sh` targets + run the capture**

Add a `--versioned-joins` branch to `run.sh` that runs the three mains against an `apache/kafka:4.1.0` broker (mirror the existing `--emit-final` / versioned-table targets) and writes `testdata/versioned_joins/{asof,grace,tabletable}.json`.

Run: `cd tests/jvm-capture && ./run.sh --versioned-joins`
Expected: three JSON fixtures written; note the captured **buffer store name** + **changelog config** in the commit message for Task 6.

- [ ] **Step 5: Commit the captures**

```bash
git add tests/jvm-capture tests/testdata/versioned_joins
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "test(client-streams): capture JVM versioned-join goldens (as-of, grace, table-table) [Kafka 4.1.0]"
```

> **Output for downstream tasks:** record in the PR description the exact grace buffer **store name** and **changelog topic config** observed; Task 6 + Task 10 assert these.

---

## Task 4: Table–table out-of-order gate (C4)

**Files:**
- Modify: `src/dsl/processors/ktable_join.rs` (add `versioned: bool` to This/Other processors + gate)
- Modify: `src/dsl/ktable.rs` (`join_impl` sets `versioned` per side)
- Test: `src/dsl/processors/ktable_join.rs` `#[cfg(test)]`

**Mechanism (golden-pinned by Task 3's `tabletable.json`):** the join input side that is versioned suppresses *out-of-order* changes. Detection: the join reads **its own input side's** versioned store latest `valid_from`; if the incoming record's timestamp is older than that latest, the change is out-of-order → emit nothing. (The *other* side's value is still read via latest `get(key)`.) Each processor therefore needs the name of *its own* side's store to check ordering when `versioned`.

> Implementation note: the This-processor handles input `Change<VA>` and reads the **other** store for `VB`; to detect its own out-of-order it needs its own side's versioned store name. Add a `self_versioned_store: Option<String>` field — `Some(store)` only when this side is versioned. When `Some`, look up `get(&key)` on that versioned store (latest) and compare `latest.valid_from` to `r.timestamp`.

- [ ] **Step 1: Write the failing test**

Add to `tests` in `src/dsl/processors/ktable_join.rs`. Seed the This-side's **own** versioned store with a latest `valid_from = 200`; process an out-of-order `Change` at ts=100 → expect **no** forward; an in-order `Change` at ts=300 → expect a forward.

```rust
    use crate::store::versioned::VersionedBytesStore;

    /// This-side own versioned store "a" with key "k": latest valid_from=200.
    /// Other store "b" with key "k" = "B".
    async fn make_versioned_this_and_other() -> StoreRegistry {
        let mut stores = StoreRegistry::default();
        let mut a = VersionedBytesStore::<String, String>::in_memory(
            "a".into(), 1_000_000, Box::new(StringSerde), Box::new(StringSerde), "a-cl".into());
        a.put("k".into(), Some("A".into()), 200).await;
        stores.insert(Box::new(a));
        let mut b = KeyValueBytesStore::<String, String>::in_memory(
            "b".into(), Box::new(StringSerde), Box::new(StringSerde), "b-cl".into());
        b.put("k".to_string(), "B".to_string()).await;
        stores.insert(Box::new(b));
        stores
    }

    async fn run_this(proc: &mut TestJoinProc, stores: &mut StoreRegistry,
                      change: Change<String>, ts: i64) -> Option<Change<String>> {
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();
        let globals = crate::runtime::global::GlobalStateManager::default();
        let mut scheds = Vec::new();
        let mut dispatch = Dispatch {
            buffer: &mut buffer, children: &children, output: &mut output,
            record_ctx: &rc, stores, globals: &globals, node_idx: 0,
            schedules: &mut scheds, sched_stream_time: i64::MIN, sched_wall_clock: 0,
        };
        let mut ctx = ProcessorContext::<'_, '_, String, Change<String>>::new(&mut dispatch);
        proc.process(&mut ctx, Record::new(Some("k".into()), change, ts)).await;
        buffer.pop_front().map(|(_, rec)| *rec.value.downcast::<Change<String>>().unwrap())
    }

    #[tokio::test]
    async fn versioned_this_suppresses_out_of_order() {
        let mut stores = make_versioned_this_and_other().await;
        let mut proc = TestJoinProc {
            other_store: "b".into(),
            joiner: concat_joiner as StrJoiner,
            kind: JoinKind::inner(),
            self_versioned_store: Some("a".into()),
            _pd: PhantomData,
        };
        // out-of-order: ts=100 < latest valid_from 200 → no emit
        check!(run_this(&mut proc, &mut stores, Change::update(None, "A2".into()), 100).await.is_none());
        // in-order: ts=300 ≥ 200 → emit
        check!(run_this(&mut proc, &mut stores, Change::update(None, "A3".into()), 300).await.is_some());
    }
```

> Update the existing `make_proc()` helper + `TestJoinProc` constructions to pass `self_versioned_store: None` (non-versioned default) so existing tests still compile.

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p crabka-client-streams -- versioned_this_suppresses_out_of_order`
Expected: FAIL — `no field self_versioned_store`.

- [ ] **Step 3: Add the field + gate to both processors**

In `src/dsl/processors/ktable_join.rs`, add to **both** `KTableKTableJoinThisProcessor` and `KTableKTableJoinOtherProcessor`:

```rust
    /// `Some(store)` when *this* input side is versioned (KIP-914): the join
    /// suppresses out-of-order changes (record ts older than this store's latest
    /// `valid_from`). `None` = non-versioned side, never suppresses.
    pub self_versioned_store: Option<String>,
```

At the top of each `process`, after computing `key`, add the out-of-order gate (uses `r.timestamp`; drop the borrow before the other-store lookup):

```rust
        if let Some(ref vs) = self.self_versioned_store {
            let out_of_order = match ctx.get_versioned_store::<K, /*own value type*/>(vs) {
                Some(s) => s.get(&key).await.is_some_and(|rec| rec.valid_from > r.timestamp),
                None => false,
            };
            if out_of_order {
                return; // KIP-914: out-of-order versioned update emits nothing
            }
        }
```

For the **This** processor the own value type is `VA`; for **Other** it is `VB`. Use `ctx.get_versioned_store::<K, VA>(vs)` (This) / `::<K, VB>(vs)` (Other). Add `VA: Send + Sync + 'static` / `VB: Send + Sync + 'static` bounds as required by `get_versioned_store` (it needs `V2: Send + 'static`; the value type already is `Send + 'static`). Note `get` borrows the store immutably and returns an owned `Option<VersionedRecord<V>>`, so the borrow drops before the existing other-store `match`.

- [ ] **Step 4: Wire `join_impl` to set the flag per side**

In `src/dsl/ktable.rs::join_impl`, the two processors are constructed in the lowering thunks. Before the thunks, capture:

```rust
        let this_versioned_store =
            self.versioned_retention_ms.is_some().then(|| this_store.clone());
        let other_versioned_store =
            other.versioned_retention_ms.is_some().then(|| other_store_name.clone());
```

(use the existing local names for this side's store and the other table's store; if they differ, adapt). Pass `self_versioned_store: this_versioned_store.clone()` into the **This** processor and `self_versioned_store: other_versioned_store.clone()` into the **Other** processor. This makes mixed versioning fall out: the unversioned side passes `None`.

> The This-processor reads the **other** store for `VB` (existing `other_store` field) AND its **own** store for `VA` (new `self_versioned_store`). Confirm both stores are connected to the processor — the This-processor is already connected to its own materialized store (it lives on that side); if not, add a `connect_processor_store(this_join_name, &this_store)` in the thunk.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p crabka-client-streams -- ktable_join`
Expected: PASS (new gate test + all existing This/Other tests still green with `self_versioned_store: None`).

- [ ] **Step 6: fmt + clippy + commit**

```bash
cargo fmt -p crabka-client-streams
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
git add src/dsl/processors/ktable_join.rs src/dsl/ktable.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(client-streams): table-table join out-of-order suppression for versioned sides (KIP-914)"
```

---

## Task 5: `Joined` grace config (C3 — config)

**Files:**
- Modify: `src/dsl/config.rs` (add `Joined`)
- Test: `src/dsl/config.rs` `#[cfg(test)]`

- [ ] **Step 1: Write the failing test**

Add to `tests` in `src/dsl/config.rs`:

```rust
    #[test]
    fn joined_carries_grace_and_name() {
        let j = Joined::with_grace_period(5_000).as_named("jb");
        check!(j.grace_ms == Some(5_000));
        check!(j.name.as_deref() == Some("jb"));
        check!(Joined::default().grace_ms == None);
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p crabka-client-streams joined_carries_grace_and_name`
Expected: FAIL — `Joined` not found.

- [ ] **Step 3: Add the `Joined` type**

In `src/dsl/config.rs`:

```rust
/// Stream–table join config (KIP-923). Carries an optional grace period that
/// buffers stream records so out-of-order records still join the table value
/// as-of their own timestamp. `grace_ms` requires the joined table to be
/// versioned and must be `< history_retention_ms` (asserted at build time).
/// `name` optionally names the grace buffer store.
#[derive(Debug, Clone, Default)]
pub struct Joined {
    pub(crate) grace_ms: Option<i64>,
    pub(crate) name: Option<String>,
}
impl Joined {
    #[must_use]
    pub fn with_grace_period(grace_ms: i64) -> Self {
        Self { grace_ms: Some(grace_ms), name: None }
    }
    #[must_use]
    pub fn as_named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p crabka-client-streams joined_carries_grace_and_name`
Expected: PASS.

- [ ] **Step 5: Export `Joined` if config types are re-exported**

If `src/dsl/config.rs` types are re-exported (check `src/dsl/mod.rs` / `src/lib.rs` for `pub use ...config::{Materialized, StreamJoined, ...}`), add `Joined` to that list.

- [ ] **Step 6: fmt + clippy + commit**

```bash
cargo fmt -p crabka-client-streams
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
git add src/dsl/config.rs src/dsl/mod.rs src/lib.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(client-streams): Joined stream-table join grace config (KIP-923)"
```

---

## Task 6: Grace buffer store (C3 — store)

**Files:**
- Create: `src/store/join_grace_buffer.rs`
- Modify: `src/store/mod.rs` (module decl + re-export)
- Modify: `src/store/registry.rs` (a `get_join_grace` accessor, mirroring `get_suppress`)
- Modify: `src/processor/api.rs` (`ctx.get_join_grace_store`, mirroring `get_suppress_store`)
- Modify: `src/topology/*` (`Topology::add_join_grace_store`, mirroring `add_suppress_store`)
- Test: `src/store/join_grace_buffer.rs` `#[cfg(test)]`

> **Reference implementations to copy structure from (read first):** `src/store/suppress_store.rs` (time-keyed buffer + `evict_while`), `src/store/versioned.rs` (changelog `Vec` + restore), and `add_suppress_store` in the topology layer. The grace buffer differs from suppress in two ways: (a) it buffers **plain records `(key, value)`** not `Change`, keyed by **`(ts, seq)`** allowing duplicate keys (a stream is not a changelog — two records with the same key at different ts must both buffer); (b) `drain_due(threshold)` returns entries in ascending `(ts, seq)` order.

> **Constants from Task 3:** the store **name pattern** and **changelog config** (cleanup.policy, retention.ms) must byte-match `tests/testdata/versioned_joins/grace.json`. Use the captured values; do not invent them.

- [ ] **Step 1: Write the failing test**

Create `src/store/join_grace_buffer.rs` with a `#[cfg(test)]` module:

```rust
    #[tokio::test]
    async fn buffers_and_drains_in_ts_order() {
        let mut s = JoinGraceBufferStore::<String, i64>::in_memory(
            "jb".into(), Box::new(StringSerde), Box::new(I64Serde), "jb-cl".into());
        // out-of-order inserts
        s.put("a".into(), 2, 200).await;
        s.put("a".into(), 1, 100).await;
        s.put("b".into(), 3, 150).await;
        // drain everything with ts <= 150, ascending (ts, seq)
        let due = s.drain_due(150).await;
        let got: Vec<(String, i64, i64)> = due;
        check!(got == vec![("a".into(), 1, 100), ("b".into(), 3, 150)]);
        // remaining: only ts=200
        let rest = s.drain_due(i64::MAX).await;
        check!(rest == vec![("a".into(), 2, 200)]);
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p crabka-client-streams buffers_and_drains_in_ts_order`
Expected: FAIL — `JoinGraceBufferStore` not found.

- [ ] **Step 3: Implement the store**

In `src/store/join_grace_buffer.rs` — model the byte/changelog layer on `VersionedBytesStore` and the eviction on `suppress_store`:

```rust
//! Stream-side buffer for the KIP-923 stream–table join grace period. Holds
//! incoming stream records `(key, value)` keyed by `(timestamp, seqnum)` so
//! out-of-order records buffer and `drain_due` returns them in ascending
//! `(ts, seq)` order once stream-time passes `ts + grace`. Unlike the suppress
//! store (replace-by-key over `Change`), this keeps EVERY record — a stream is
//! not a changelog.
use std::any::Any;
use std::collections::BTreeMap;

use async_trait::async_trait;
use bytes::Bytes;

use crate::processor::serde::Serde;
use crate::store::api::StateStore;

pub struct JoinGraceBufferStore<K, V> {
    name: String,
    changelog_topic: String,
    key_serde: Box<dyn Serde<K>>,
    value_serde: Box<dyn Serde<V>>,
    // (ts, seq) -> (key bytes, value bytes)
    buffer: BTreeMap<(i64, u32), (Bytes, Bytes)>,
    seq: u32,
    changelog: Vec<((i64, u32), Option<(Bytes, Bytes)>)>,
    logging: bool,
}

impl<K: Any + Send + Sync + Clone, V: Any + Send + Clone> JoinGraceBufferStore<K, V> {
    #[must_use]
    pub fn in_memory(
        name: String,
        key_serde: Box<dyn Serde<K>>,
        value_serde: Box<dyn Serde<V>>,
        changelog_topic: String,
    ) -> Self {
        Self {
            name, changelog_topic, key_serde, value_serde,
            buffer: BTreeMap::new(), seq: 0, changelog: Vec::new(), logging: true,
        }
    }

    pub async fn put(&mut self, key: K, value: V, ts: i64) {
        let kb = self.key_serde.serialize(&key);
        let vb = self.value_serde.serialize(&value);
        let id = (ts, self.seq);
        self.seq = self.seq.wrapping_add(1);
        if self.logging {
            self.changelog.push((id, Some((kb.clone(), vb.clone()))));
        }
        self.buffer.insert(id, (kb, vb));
    }

    /// Remove + return all entries with `ts <= threshold`, ascending `(ts, seq)`.
    pub async fn drain_due(&mut self, threshold: i64) -> Vec<(K, V, i64)> {
        let due_keys: Vec<(i64, u32)> = self
            .buffer
            .range(..=(threshold, u32::MAX))
            .map(|(k, _)| *k)
            .collect();
        let mut out = Vec::with_capacity(due_keys.len());
        for id in due_keys {
            let (kb, vb) = self.buffer.remove(&id).expect("present");
            if self.logging {
                self.changelog.push((id, None)); // tombstone the drained slot
            }
            let k = self.key_serde.deserialize(&kb);
            let v = self.value_serde.deserialize(&vb);
            out.push((k, v, id.0));
        }
        out
    }
}

#[async_trait]
impl<K: Any + Send + Sync, V: Any + Send> StateStore for JoinGraceBufferStore<K, V> {
    fn name(&self) -> &str { &self.name }
    fn changelog_topic(&self) -> &str { &self.changelog_topic }
    // Drain/restore changelog methods — mirror the EXACT trait surface that
    // `VersionedBytesStore`/`SuppressBytesStore` implement in this file's sibling
    // modules (take_changelog / apply_changelog / set_logging). Copy their
    // signatures verbatim; serialize the `((ts,seq), Option<(kb,vb)>)` records.
}
```

> Match the real `StateStore` trait surface (`name`, `changelog_topic`, changelog take/apply, logging toggle) exactly as `suppress_store.rs` does — read it and copy method-for-method. Serialize changelog keys/values to match `grace.json` from Task 3.

- [ ] **Step 4: Wire the registry + ctx accessor + topology registration**

Mirror `get_suppress` end-to-end:
- `src/store/registry.rs`: add `get_join_grace::<K,V>(name) -> Option<&mut JoinGraceBufferStore<K,V>>` + the downcast arm in `insert`/lookup.
- `src/processor/api.rs`: add `pub(crate) fn get_join_grace_store::<K2,V2>(&mut self, name) -> Option<&mut JoinGraceBufferStore<K2,V2>>` delegating to the registry.
- `src/topology/*` (where `add_suppress_store` lives): add `add_join_grace_store::<K,V,KS,VS>(name, key_serde, value_serde, logging, connected_processors)` that registers a `JoinGraceBufferStore` with the captured changelog config.

- [ ] **Step 5: Run the store test to verify it passes**

Run: `cargo test -p crabka-client-streams buffers_and_drains_in_ts_order`
Expected: PASS.

- [ ] **Step 6: fmt + clippy + commit**

```bash
cargo fmt -p crabka-client-streams
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
git add src/store/join_grace_buffer.rs src/store/mod.rs src/store/registry.rs src/processor/api.rs src/topology
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(client-streams): join-grace buffer store (KIP-923)"
```

---

## Task 7: Grace flush processor (C3 — processor)

**Files:**
- Create: `src/dsl/processors/join_grace.rs`
- Modify: `src/dsl/processors/mod.rs` (module decl)
- Test: `src/dsl/processors/join_grace.rs` `#[cfg(test)]`

- [ ] **Step 1: Write the failing test**

Create `src/dsl/processors/join_grace.rs` with tests that seed a versioned store + a grace buffer store and assert ordered, as-of, stream-time-gated emission. Seed versioned store "vt": `(a,10)@100`, `(a,20)@200`. Grace = 50.

```rust
    // record (a,1)@250: stream_time=250, threshold=200; buffered then drained
    // immediately (ts 250 > 200? no — 250 <= 250-50=200 is false → stays). To
    // force a drain, send a later record. Sequence:
    //   (a,1)@150 -> threshold 100 -> buffer (150 not <=100) stays
    //   (a,1)@260 -> stream_time 260 threshold 210 -> drains ts=150 (<=210):
    //       as_of(150)=10 -> 1+10=11 @150 ; 260 stays (260>210)
    #[tokio::test]
    async fn grace_drains_in_order_with_asof() { /* full harness as in join.rs tests */ }
```

(Write the full `Dispatch`/`ProcessorContext` harness as in Task 2/4; assert the forwarded `(value, timestamp)` sequence.)

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p crabka-client-streams grace_drains_in_order_with_asof`
Expected: FAIL — processor not found.

- [ ] **Step 3: Implement the processor**

```rust
//! KIP-923 stream–table join grace processor. Buffers each stream record into a
//! `JoinGraceBufferStore`, advances `observed_stream_time`, then drains every
//! buffered record with `bufTs ≤ streamTime − grace_ms` in ascending `(ts, seq)`
//! order — performing the versioned as-of join (`get_as_of(key, bufTs)`) at drain
//! time and forwarding at `bufTs`. A record already late on arrival drains in the
//! same pass. Inner skips on miss; left passes `None`.
use std::marker::PhantomData;
use async_trait::async_trait;

use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;

type Marker<T> = PhantomData<fn() -> T>;

pub(crate) struct KStreamKTableJoinGraceProcessor<K, V, VT, VO, F> {
    pub table_store: String,
    pub buffer_store: String,
    pub grace_ms: i64,
    pub joiner: F,
    pub emit_on_miss: bool,
    pub observed_stream_time: i64,
    pub _pd: Marker<(K, V, VT, VO)>,
}

#[async_trait]
impl<K, V, VT, VO, F> Processor<K, V, K, VO> for KStreamKTableJoinGraceProcessor<K, V, VT, VO, F>
where
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Clone,
    VT: Send + 'static,
    VO: std::any::Any + Send + Clone,
    F: Fn(&V, Option<&VT>) -> VO + Send + 'static,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, VO>, r: Record<K, V>) {
        let key = r.key.expect("join requires a non-null key");
        self.observed_stream_time = self.observed_stream_time.max(r.timestamp);

        // Buffer the incoming record.
        {
            let buf = ctx
                .get_join_grace_store::<K, V>(&self.buffer_store)
                .expect("join grace buffer store not found");
            buf.put(key, r.value, r.timestamp).await;
        }

        // Drain everything now due (ascending (ts, seq)).
        let threshold = self.observed_stream_time - self.grace_ms;
        let due = {
            let buf = ctx
                .get_join_grace_store::<K, V>(&self.buffer_store)
                .expect("join grace buffer store not found");
            buf.drain_due(threshold).await
        };

        for (k, v, ts) in due {
            let vt = match ctx.get_versioned_store::<K, VT>(&self.table_store) {
                Some(s) => s.get_as_of(&k, ts).await.map(|rec| rec.value),
                None => None,
            };
            if vt.is_some() || self.emit_on_miss {
                let out = (self.joiner)(&v, vt.as_ref());
                ctx.forward(Record::new(Some(k), out, ts));
            }
        }
    }
}
```

Add `pub(crate) mod join_grace;` to `src/dsl/processors/mod.rs`.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p crabka-client-streams grace_drains_in_order_with_asof`
Expected: PASS.

- [ ] **Step 5: fmt + clippy + commit**

```bash
cargo fmt -p crabka-client-streams
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
git add src/dsl/processors/join_grace.rs src/dsl/processors/mod.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(client-streams): stream-table join grace flush processor (KIP-923)"
```

---

## Task 8: DSL `join_table_with` / `left_join_table_with` + grace wiring (C3 — DSL)

**Files:**
- Modify: `src/dsl/kstream.rs` (`join_table_with`, `left_join_table_with`, extend `join_table_impl` to take `Option<Joined>`)
- Test: `src/dsl/kstream.rs` `#[cfg(test)]` (build-time asserts) + a `TopologyTestDriver` behavioral test

- [ ] **Step 1: Write the failing tests**

Add to `tests` in `src/dsl/kstream.rs`:

```rust
    #[test]
    #[should_panic(expected = "grace requires a versioned table")]
    fn grace_on_unversioned_table_panics() {
        use crate::dsl::builder::StreamsBuilder;
        use crate::dsl::config::{Joined, Materialized};
        use crate::processor::serde::{I64Serde, StringSerde};
        let b = StreamsBuilder::new();
        let s = b.stream_explicit::<StringSerde, I64Serde>(
            ["s"], crate::processor::serde::Consumed::with(StringSerde, I64Serde));
        let t = b.table_explicit::<StringSerde, I64Serde>(
            "t", crate::processor::serde::Consumed::with(StringSerde, I64Serde),
            Materialized::with(StringSerde, I64Serde).as_store("plain"));
        let _ = s.join_table_with(&t, |a: &i64, c: &i64| a + c, Joined::with_grace_period(1000));
    }

    #[test]
    #[should_panic(expected = "grace_ms must be < history_retention_ms")]
    fn grace_geq_retention_panics() {
        use crate::dsl::builder::StreamsBuilder;
        use crate::dsl::config::{Joined, Materialized};
        use crate::processor::serde::{I64Serde, StringSerde};
        let b = StreamsBuilder::new();
        let s = b.stream_explicit::<StringSerde, I64Serde>(
            ["s"], crate::processor::serde::Consumed::with(StringSerde, I64Serde));
        let t = b.table_explicit::<StringSerde, I64Serde>(
            "t", crate::processor::serde::Consumed::with(StringSerde, I64Serde),
            Materialized::with(StringSerde, I64Serde).as_versioned("vt", 1000));
        let _ = s.join_table_with(&t, |a: &i64, c: &i64| a + c, Joined::with_grace_period(1000));
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p crabka-client-streams -- grace_on_unversioned_table_panics grace_geq_retention_panics`
Expected: FAIL — `join_table_with` not found.

- [ ] **Step 3: Add the methods + asserts + thunk wiring**

In `src/dsl/kstream.rs`, add `join_table_with`/`left_join_table_with` mirroring `join_table`/`left_join_table` but taking `joined: Joined`. Extend `join_table_impl` with a `grace: Option<(i64, Option<String>)>` parameter. At the top of `join_table_impl`, when `grace.is_some()`:

```rust
        if let Some((grace_ms, _)) = grace {
            let retention = table.versioned_retention_ms
                .expect("grace requires a versioned table");
            assert!(grace_ms < retention, "grace_ms must be < history_retention_ms");
        }
```

In the lowering thunk, when grace is set, build `KStreamKTableJoinGraceProcessor` instead of the as-of/plain processor, register the grace buffer store via `Topology::add_join_grace_store(...)` (buffer store name from `Joined.name` or the JVM default counter pattern pinned by Task 3), and `connect_processor_store` the join to **both** the table store and the buffer store. The existing two methods (`join_table`/`left_join_table`) call `join_table_impl(..., None)`.

- [ ] **Step 4: Run the asserts to verify they pass**

Run: `cargo test -p crabka-client-streams -- grace_on_unversioned_table_panics grace_geq_retention_panics`
Expected: PASS (both panic with the expected messages).

- [ ] **Step 5: Behavioral test via `TopologyTestDriver`**

Add a `TopologyTestDriver` test that drives the out-of-order sequence from Task 3's `grace.json` and asserts the output records match. (Use the existing driver test pattern in the crate — search for `TopologyTestDriver` usages.)

Run: `cargo test -p crabka-client-streams -- join_grace_driver`
Expected: PASS.

- [ ] **Step 6: fmt + clippy + commit**

```bash
cargo fmt -p crabka-client-streams
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
git add src/dsl/kstream.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(client-streams): join_table_with grace DSL + buffer wiring (KIP-923)"
```

---

## Task 9: Golden replay — stream–table as-of (C5)

**Files:**
- Create: `tests/versioned_joins_golden.rs` (shared file; this task adds the as-of test)
- Test target must be added to the crate's `llvm-cov --test` list in `ci.yml` (per memory: new `tests/<x>.rs` reports 0% patch otherwise).

- [ ] **Step 1: Write the test**

Load `tests/testdata/versioned_joins/asof.json`, build the equivalent topology in Rust (`builder.stream` join versioned `builder.table` via `join_table`), run a `TopologyTestDriver`, and assert the output records (value + timestamp) match the captured JVM output, and the topology description (node + store names + versioned changelog config) matches. Mirror the slice-1 versioned-table golden test's load/assert helpers.

- [ ] **Step 2: Run + verify**

Run: `cargo test -p crabka-client-streams --test versioned_joins_golden -- asof`
Expected: PASS (byte/behavioral match).

- [ ] **Step 3: Register in CI + commit**

Add `tests/versioned_joins_golden.rs` to the crate's `--test` list in `.github/workflows/ci.yml` llvm-cov invocation.

```bash
git add tests/versioned_joins_golden.rs .github/workflows/ci.yml
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "test(client-streams): golden replay for as-of stream-table join (KIP-914)"
```

---

## Task 10: Golden replay — join grace (C5)

**Files:**
- Modify: `tests/versioned_joins_golden.rs` (add grace test)

- [ ] **Step 1: Write the test**

Load `grace.json`; build the topology with `join_table_with(..., Joined::with_grace_period(GRACE))`; drive the out-of-order sequence; assert output ordering/values/timestamps **and** the grace buffer store's **changelog records** + name + config match the capture. This is where the buffer-store-name trap surfaces — if it mismatches, the topology assertion fails first; fix the name in Task 6/8 to match `grace.json`.

- [ ] **Step 2: Run + verify**

Run: `cargo test -p crabka-client-streams --test versioned_joins_golden -- grace`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/versioned_joins_golden.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "test(client-streams): golden replay for stream-table join grace (KIP-923)"
```

---

## Task 11: Golden replay — table–table versioned (C5)

**Files:**
- Modify: `tests/versioned_joins_golden.rs` (add table-table test)

- [ ] **Step 1: Write the test**

Load `tabletable.json`; build two versioned `builder.table`s joined inner; drive the in-order + out-of-order updates; assert the out-of-order update produces **no** output record and the in-order one does. This pins the Task 4 detection mechanism against the JVM.

- [ ] **Step 2: Run + verify**

Run: `cargo test -p crabka-client-streams --test versioned_joins_golden -- tabletable`
Expected: PASS. If the JVM capture shows the suppression keys off something other than "record ts < store latest valid_from", adjust Task 4's gate to match and re-run.

- [ ] **Step 3: Commit**

```bash
git add tests/versioned_joins_golden.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "test(client-streams): golden replay for table-table versioned out-of-order (KIP-914)"
```

---

## Task 12: Full-suite reconciliation

**Files:** none (verification only) unless fixes are needed.

- [ ] **Step 1: Full workspace fmt + clippy**

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
```
Expected: clean. (Per memory: clippy cache can mask workspace lints — if a file is unchanged but suspect, `touch` it and re-run; check the real `$?`.)

- [ ] **Step 2: Full crate test suite**

Run: `cargo test -p crabka-client-streams`
Expected: PASS — the whole suite is the gate (erasure mismatch is a runtime downcast, not a compile error).

- [ ] **Step 3: Workspace build (no accidental breakage)**

Run: `cargo build --workspace`
Expected: success.

- [ ] **Step 4: Update memory + final commit (if any fixes)**

Update `project-kip1071-streams` memory: slice 2 (KIP-914 join half + KIP-923 grace) done; remaining = versioned-tables slice 3 (IQv2 KIP-960/968). Note the grace buffer store name + the table-table out-of-order detection mechanism as confirmed by goldens.

```bash
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -am "chore(client-streams): reconcile versioned-joins slice (fmt/clippy/test green)" || echo "nothing to commit"
```

---

## Self-Review notes (carried into execution)

- **Spec coverage:** C1=Task1, C2=Task2, C3(config/store/processor/DSL)=Tasks5/6/7/8, C4=Task4, C5=Tasks3/9/10/11. All spec sections mapped.
- **Empirical dependencies (not placeholders):** grace buffer **store name** + **changelog config** (Task 6/8/10) and the table–table **out-of-order detection** (Task 4/11) are resolved against Task 3's JVM capture — capture-first ordering enforced by the batch plan.
- **Type consistency:** `versioned_retention_ms` (field) / `with_versioned_retention` (setter); `KStreamKTableJoinAsOfProcessor` / `KStreamKTableJoinGraceProcessor`; `JoinGraceBufferStore::{put, drain_due}`; `get_join_grace_store`; `Joined::{with_grace_period, as_named, grace_ms, name}` — used identically across tasks.
- **Topology fidelity anchor:** Task 2 adds no node/store (as-of route); only Task 8 (grace) adds the buffer store + makes the join stateful — both golden-pinned.
