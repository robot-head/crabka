# IQv2 Framework — Slice 3b (Versioned Queries) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `VersionedKeyQuery` (KIP-960) and `MultiVersionedKeyQuery` (KIP-968) to the existing IQv2 framework (slice 3a, merged), querying `VersionedBytesStore` through the same `StateQueryRequest`→`KafkaStreams::query()`→`StateQueryResult` envelope.

**Architecture:** Reuses the entire 3a envelope unchanged. Two new `Iq2Query` variants + an `iq2_execute` impl on `VersionedBytesStore` (the byte methods `iq_versioned_get`/`iq_versioned_get_as_of` already exist; the multi-version range walk is the only new store logic), plus two new query builders in `runtime/iqv2/query.rs`. `serve_iq2`/`dispatch`/`app::query`/`test_driver::query` are generic over `Query` and need **no** changes.

**Tech Stack:** Rust 2024, `tokio`, `async_trait`, `bytes`. Reference design: `docs/superpowers/specs/2026-06-10-kip-1071-streams-client-iqv2-design.md` (slice 3b = §2; result types = §1; versioned-range op = §7). Builds on slice 3a (merged to `main` as #484).

**Result types (KIP-960/968):**
- `VersionedKeyQuery<K,V>` → `Option<VersionedRecord<V>>` (latest, or version valid at `as_of`).
- `MultiVersionedKeyQuery<K,V>` → `Vec<VersionedRecord<V>>` (all versions whose validity interval overlaps `[from_time, to_time]`, ascending by `valid_from` by default).

`VersionedRecord<V> { value: V, valid_from: i64, valid_to: Option<i64> }` already exists and is re-exported at the crate root (`crates/client-streams/src/lib.rs:932`).

**KIP-968 overlap semantics (match JVM exactly):** a version with interval `[valid_from, valid_to)` (where `valid_to = None` means ∞) overlaps the inclusive query range `[from, to]` iff `valid_from <= to && valid_to.map_or(true, |vt| vt > from)`. Defaults: `from = i64::MIN`, `to = i64::MAX`. Tombstone versions (chain value `None`) are skipped. Ascending by `valid_from` (BTreeMap order); `with_descending_timestamps()` reverses.

**Batching (parallel where file sets are disjoint):**
- Batch 1: Task 1 (`store/iq.rs` — two new query-descriptor variants).
- Batch 2 ∥: Task 2 (`store/versioned.rs`), Task 3 (`runtime/iqv2/{query,mod}.rs` + `lib.rs`).
- Batch 3: Task 4 (goldens).
- Batch 4: Task 5 (reconciliation + memory).

All commits use the identity override (git identity unset locally):
`git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit …`
**Subagents must `git -C <worktree>` and assert the branch is `claude/iqv2-3b-versioned-queries` before committing** (subagent shells reset cwd to the main repo). Worktree path: `/Users/mattstone/git/crabka/.claude/worktrees/lucid-sanderson-b67884`.

---

## Task 1: Two versioned query descriptors

**Files:**
- Modify: `crates/client-streams/src/store/iq.rs`

Adds `Iq2Query::VersionedKey` and `Iq2Query::MultiVersionedKey`. Keys travel as `Box<dyn Any + Send + Sync>` (same as the 3a variants); time bounds are `Option<i64>` (`None` = unbounded).

- [ ] **Step 1: Write the failing test**

In the `tests` module of `crates/client-streams/src/store/iq.rs`, extend the existing `iq2_execute_default_is_unknown_query_type` test (it already builds a `SessionBytesStore` and asserts the default `iq2_execute` rejects). Add a second assertion to that test body, right before its closing brace, exercising a versioned variant against the default impl:

```rust
        // Versioned variants also hit the default (a session store has no handler).
        let mv = Iq2Query::MultiVersionedKey {
            key: Box::new("k".to_string()),
            from_ts: None,
            to_ts: None,
            descending: false,
        };
        assert_eq!(q.iq2_execute(&mv).await.err(), Some(Iq2Failure::UnknownQueryType));
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-client-streams --lib store::iq::tests::iq2_execute_default_is_unknown_query_type`
Expected: FAIL — `Iq2Query::MultiVersionedKey` variant does not exist.

- [ ] **Step 3: Add the variants**

In `crates/client-streams/src/store/iq.rs`, add these two variants to the `Iq2Query` enum, after the existing `WindowRange { … }` variant:

```rust
    /// `VersionedKeyQuery` (KIP-960) — one key; `as_of = None` ⇒ latest live
    /// version, `Some(t)` ⇒ the version valid at `t`. Result:
    /// `Option<VersionedRecord<V>>`.
    VersionedKey {
        key: Box<dyn Any + Send + Sync>,
        as_of: Option<i64>,
    },
    /// `MultiVersionedKeyQuery` (KIP-968) — one key; every version whose
    /// validity `[valid_from, valid_to)` overlaps `[from_ts, to_ts]` (`None`
    /// bound = unbounded that side), ascending by `valid_from` unless
    /// `descending`. Result: `Vec<VersionedRecord<V>>`.
    MultiVersionedKey {
        key: Box<dyn Any + Send + Sync>,
        from_ts: Option<i64>,
        to_ts: Option<i64>,
        descending: bool,
    },
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p crabka-client-streams --lib store::iq::tests::iq2_execute_default_is_unknown_query_type`
Expected: PASS. Then `cargo build -p crabka-client-streams` — success.

- [ ] **Step 5: Commit**

```bash
git -C <worktree> add crates/client-streams/src/store/iq.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(client-streams): IQv2 versioned query descriptors (KIP-960/968)"
```

---

## Task 2: `VersionedBytesStore::iq2_execute`

**Files:**
- Modify: `crates/client-streams/src/store/versioned.rs`

Depends on Task 1. Widens the `IqQueryable` impl bound to `K: Send + 'static, V: Send + 'static` (needed to box `Option<VersionedRecord<V>>` / `Vec<VersionedRecord<V>>` as `Box<dyn Any + Send>`; the `StateStore` impl already has these bounds) and adds `iq2_execute`.

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/client-streams/src/store/versioned.rs` (it has a `fn store(retention: i64) -> VersionedBytesStore<String, i64>` helper and imports `I64Serde`, `StringSerde`):

```rust
    #[tokio::test]
    async fn iq2_versioned_key_and_multi() {
        use crate::store::iq::{Iq2Query, IqQueryable, StoreKind};
        let mut s = store(1_000_000);
        // Three in-order versions of "k": 10@100, 20@200, 30@300.
        s.put("k".into(), Some(10), 100).await;
        s.put("k".into(), Some(20), 200).await;
        s.put("k".into(), Some(30), 300).await;
        let q: &dyn IqQueryable = s.as_iq().unwrap();
        assert_eq!(q.kind(), StoreKind::Versioned);

        // VersionedKeyQuery latest.
        let latest = q
            .iq2_execute(&Iq2Query::VersionedKey { key: Box::new("k".to_string()), as_of: None })
            .await
            .unwrap();
        assert_eq!(
            *latest.downcast::<Option<VersionedRecord<i64>>>().unwrap(),
            Some(VersionedRecord { value: 30, valid_from: 300, valid_to: None })
        );

        // VersionedKeyQuery as_of(250) → the 20@200 version, superseded at 300.
        let asof = q
            .iq2_execute(&Iq2Query::VersionedKey { key: Box::new("k".to_string()), as_of: Some(250) })
            .await
            .unwrap();
        assert_eq!(
            *asof.downcast::<Option<VersionedRecord<i64>>>().unwrap(),
            Some(VersionedRecord { value: 20, valid_from: 200, valid_to: Some(300) })
        );

        // as_of(50) predates the oldest version → None.
        let miss = q
            .iq2_execute(&Iq2Query::VersionedKey { key: Box::new("k".to_string()), as_of: Some(50) })
            .await
            .unwrap();
        assert_eq!(*miss.downcast::<Option<VersionedRecord<i64>>>().unwrap(), None);

        // MultiVersionedKeyQuery all (unbounded), ascending.
        let all = q
            .iq2_execute(&Iq2Query::MultiVersionedKey {
                key: Box::new("k".to_string()),
                from_ts: None,
                to_ts: None,
                descending: false,
            })
            .await
            .unwrap();
        assert_eq!(
            *all.downcast::<Vec<VersionedRecord<i64>>>().unwrap(),
            vec![
                VersionedRecord { value: 10, valid_from: 100, valid_to: Some(200) },
                VersionedRecord { value: 20, valid_from: 200, valid_to: Some(300) },
                VersionedRecord { value: 30, valid_from: 300, valid_to: None },
            ]
        );

        // MultiVersionedKeyQuery [150,250] → versions overlapping that range
        // (10@[100,200) and 20@[200,300); 30@[300,∞) excluded), descending.
        let win = q
            .iq2_execute(&Iq2Query::MultiVersionedKey {
                key: Box::new("k".to_string()),
                from_ts: Some(150),
                to_ts: Some(250),
                descending: true,
            })
            .await
            .unwrap();
        assert_eq!(
            *win.downcast::<Vec<VersionedRecord<i64>>>().unwrap(),
            vec![
                VersionedRecord { value: 20, valid_from: 200, valid_to: Some(300) },
                VersionedRecord { value: 10, valid_from: 100, valid_to: Some(200) },
            ]
        );

        // Wrong key type → KeyTypeMismatch.
        use crate::store::iq::Iq2Failure;
        let bad = q
            .iq2_execute(&Iq2Query::VersionedKey { key: Box::new(7_i64), as_of: None })
            .await;
        assert_eq!(bad.err(), Some(Iq2Failure::KeyTypeMismatch));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-client-streams --lib store::versioned::tests::iq2_versioned_key_and_multi`
Expected: FAIL — default `iq2_execute` returns `UnknownQueryType`.

- [ ] **Step 3: Widen the impl bound + implement `iq2_execute`**

In `crates/client-streams/src/store/versioned.rs`, change the `IqQueryable` impl header from:

```rust
impl<K: 'static, V: 'static> crate::store::iq::IqQueryable for VersionedBytesStore<K, V> {
```

to:

```rust
impl<K: Send + 'static, V: Send + 'static> crate::store::iq::IqQueryable for VersionedBytesStore<K, V> {
```

Then add this method to that impl block (after `iq_versioned_get_as_of`). Note `std::any::Any` — add `use std::any::Any;` at the top of the file if not already imported (check the existing `use` lines; `bytes::Bytes` is already imported):

```rust
    async fn iq2_execute(
        &self,
        query: &crate::store::iq::Iq2Query,
    ) -> Result<Box<dyn Any + Send>, crate::store::iq::Iq2Failure> {
        use crate::store::iq::{Iq2Failure, Iq2Query};
        let ser = |b: &dyn Any| -> Result<Bytes, Iq2Failure> {
            let k = b.downcast_ref::<K>().ok_or(Iq2Failure::KeyTypeMismatch)?;
            Ok(self.key_serde.serialize(&self.changelog_topic, k))
        };
        let deser = |raw: &[u8]| -> V {
            self.value_serde
                .deserialize(&self.changelog_topic, raw)
                .expect("iqv2 versioned value deserialize")
        };
        match query {
            Iq2Query::VersionedKey { key, as_of } => {
                let kb = ser(&**key)?;
                let out: Option<VersionedRecord<V>> = self.chains.get(&kb).and_then(|chain| {
                    let (valid_from, value, valid_to) = match as_of {
                        // Latest live version: last chain entry, valid_to = ∞.
                        None => {
                            let (&vf, value) = chain.iter().next_back()?;
                            (vf, value, None)
                        }
                        // Version valid at `t`: greatest validFrom <= t; valid_to
                        // = the next validFrom after `t` (if any).
                        Some(t) => {
                            let (&vf, value) = chain.range(..=*t).next_back()?;
                            let vt = chain.range((*t + 1)..).next().map(|(x, _)| *x);
                            (vf, value, vt)
                        }
                    };
                    let raw = value.as_ref()?; // tombstone => None
                    Some(VersionedRecord { value: deser(raw), valid_from, valid_to })
                });
                Ok(Box::new(out))
            }
            Iq2Query::MultiVersionedKey { key, from_ts, to_ts, descending } => {
                let kb = ser(&**key)?;
                let from = from_ts.unwrap_or(i64::MIN);
                let to = to_ts.unwrap_or(i64::MAX);
                let mut out: Vec<VersionedRecord<V>> = Vec::new();
                if let Some(chain) = self.chains.get(&kb) {
                    let entries: Vec<(i64, &Option<Bytes>)> =
                        chain.iter().map(|(t, v)| (*t, v)).collect();
                    for (i, (valid_from, value)) in entries.iter().enumerate() {
                        let Some(raw) = value.as_ref() else { continue }; // skip tombstones
                        let valid_to = entries.get(i + 1).map(|(t, _)| *t);
                        // Overlap of [valid_from, valid_to) with inclusive [from, to].
                        let overlaps = *valid_from <= to && valid_to.is_none_or(|vt| vt > from);
                        if overlaps {
                            out.push(VersionedRecord {
                                value: deser(raw),
                                valid_from: *valid_from,
                                valid_to,
                            });
                        }
                    }
                }
                if *descending {
                    out.reverse();
                }
                Ok(Box::new(out))
            }
            _ => Err(Iq2Failure::UnknownQueryType),
        }
    }
```

> `chain` is `BTreeMap<i64, Option<Bytes>>`, so iteration is ascending by
> `valid_from`. `valid_to` of a version is the next chain entry's `valid_from`
> (a tombstone counts as the terminator) — identical to the existing
> `iq_versioned_get_as_of`. `is_none_or` is stable; if the toolchain rejects it,
> use `valid_to.map_or(true, |vt| vt > from)`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p crabka-client-streams --lib store::versioned::tests::iq2_versioned_key_and_multi`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C <worktree> add crates/client-streams/src/store/versioned.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(client-streams): Versioned iq2_execute (VersionedKeyQuery + MultiVersionedKeyQuery)"
```

---

## Task 3: Versioned query builders + re-exports

**Files:**
- Modify: `crates/client-streams/src/runtime/iqv2/query.rs`
- Modify: `crates/client-streams/src/runtime/iqv2/mod.rs`
- Modify: `crates/client-streams/src/lib.rs`

Depends on Task 1. Adds the two public builders mirroring the 3a query types, plus re-exports.

- [ ] **Step 1: Write the failing test**

In the `tests` module of `crates/client-streams/src/runtime/iqv2/query.rs` (it already has a `lowering_*` test using `matches!` on `Iq2Query`), add:

```rust
    #[test]
    fn versioned_queries_lower_correctly() {
        let vk = VersionedKeyQuery::<String, i64>::with_key("k".into()).as_of(250);
        assert_eq!(vk.store_kind(), StoreKind::Versioned);
        assert!(matches!(vk.lower(), Iq2Query::VersionedKey { as_of: Some(250), .. }));

        let vk_latest = VersionedKeyQuery::<String, i64>::with_key("k".into());
        assert!(matches!(vk_latest.lower(), Iq2Query::VersionedKey { as_of: None, .. }));

        let mv = MultiVersionedKeyQuery::<String, i64>::with_key("k".into())
            .from_time(150)
            .to_time(250)
            .with_descending_timestamps();
        assert_eq!(mv.store_kind(), StoreKind::Versioned);
        assert!(matches!(
            mv.lower(),
            Iq2Query::MultiVersionedKey { from_ts: Some(150), to_ts: Some(250), descending: true, .. }
        ));

        let mv_all = MultiVersionedKeyQuery::<String, i64>::with_key("k".into());
        assert!(matches!(
            mv_all.lower(),
            Iq2Query::MultiVersionedKey { from_ts: None, to_ts: None, descending: false, .. }
        ));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-client-streams --lib runtime::iqv2::query::tests::versioned_queries_lower_correctly`
Expected: FAIL — `VersionedKeyQuery` / `MultiVersionedKeyQuery` not found.

- [ ] **Step 3: Add the builders**

At the top of `crates/client-streams/src/runtime/iqv2/query.rs`, add an import for `VersionedRecord` next to the existing imports:

```rust
use crate::store::versioned::VersionedRecord;
```

Then add these two query types at the end of the file, before the `#[cfg(test)] mod tests` block:

```rust
/// Single versioned-key lookup (KIP-960). `as_of = None` ⇒ latest live version.
/// Result: `Option<VersionedRecord<V>>`.
pub struct VersionedKeyQuery<K, V> {
    key: K,
    as_of: Option<i64>,
    _v: PhantomData<fn() -> V>,
}
impl<K, V> VersionedKeyQuery<K, V> {
    #[must_use]
    pub fn with_key(key: K) -> Self {
        Self { key, as_of: None, _v: PhantomData }
    }
    #[must_use]
    pub fn as_of(mut self, timestamp: i64) -> Self {
        self.as_of = Some(timestamp);
        self
    }
}
impl<K: Send + Sync + 'static, V: 'static> sealed::Sealed for VersionedKeyQuery<K, V> {}
impl<K: Send + Sync + 'static, V: 'static> Query for VersionedKeyQuery<K, V> {
    type Result = Option<VersionedRecord<V>>;
    fn store_kind(&self) -> StoreKind {
        StoreKind::Versioned
    }
    fn lower(self) -> Iq2Query {
        Iq2Query::VersionedKey { key: Box::new(self.key), as_of: self.as_of }
    }
}

/// All versions of a key whose validity overlaps `[from_time, to_time]`
/// (KIP-968). `None` bound = unbounded that side; ascending by `valid_from`
/// unless `with_descending_timestamps()`. Result: `Vec<VersionedRecord<V>>`.
pub struct MultiVersionedKeyQuery<K, V> {
    key: K,
    from_ts: Option<i64>,
    to_ts: Option<i64>,
    descending: bool,
    _v: PhantomData<fn() -> V>,
}
impl<K, V> MultiVersionedKeyQuery<K, V> {
    #[must_use]
    pub fn with_key(key: K) -> Self {
        Self { key, from_ts: None, to_ts: None, descending: false, _v: PhantomData }
    }
    #[must_use]
    pub fn from_time(mut self, t: i64) -> Self {
        self.from_ts = Some(t);
        self
    }
    #[must_use]
    pub fn to_time(mut self, t: i64) -> Self {
        self.to_ts = Some(t);
        self
    }
    #[must_use]
    pub fn with_ascending_timestamps(mut self) -> Self {
        self.descending = false;
        self
    }
    #[must_use]
    pub fn with_descending_timestamps(mut self) -> Self {
        self.descending = true;
        self
    }
}
impl<K: Send + Sync + 'static, V: 'static> sealed::Sealed for MultiVersionedKeyQuery<K, V> {}
impl<K: Send + Sync + 'static, V: 'static> Query for MultiVersionedKeyQuery<K, V> {
    type Result = Vec<VersionedRecord<V>>;
    fn store_kind(&self) -> StoreKind {
        StoreKind::Versioned
    }
    fn lower(self) -> Iq2Query {
        Iq2Query::MultiVersionedKey {
            key: Box::new(self.key),
            from_ts: self.from_ts,
            to_ts: self.to_ts,
            descending: self.descending,
        }
    }
}
```

- [ ] **Step 4: Re-export**

In `crates/client-streams/src/runtime/iqv2/mod.rs`, add the two types to the `pub use query::{…}` line:

```rust
pub use query::{
    KeyQuery, MultiVersionedKeyQuery, Query, RangeQuery, VersionedKeyQuery, WindowKeyQuery,
    WindowRangeQuery,
};
```

In `crates/client-streams/src/lib.rs`, add `MultiVersionedKeyQuery` and `VersionedKeyQuery` to the existing `pub use runtime::iqv2::{…}` block (keep it alphabetical / matching the existing style).

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test -p crabka-client-streams --lib runtime::iqv2::query::tests::versioned_queries_lower_correctly`
Expected: PASS. Then `cargo build -p crabka-client-streams` — success.

- [ ] **Step 6: Commit**

```bash
git -C <worktree> add crates/client-streams/src/runtime/iqv2/query.rs \
  crates/client-streams/src/runtime/iqv2/mod.rs crates/client-streams/src/lib.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(client-streams): VersionedKeyQuery + MultiVersionedKeyQuery builders (KIP-960/968)"
```

---

## Task 4: Versioned IQv2 goldens

**Files:**
- Modify: `crates/client-streams/tests/iqv2_golden.rs`
- Modify: `crates/client-streams/tests/testdata/iqv2/behavior.json`

Depends on Tasks 2 & 3. Drives a versioned KTable through `TopologyTestDriver`, then asserts `VersionedKeyQuery` (latest + as-of) and `MultiVersionedKeyQuery` (all + range × asc/desc) via `TopologyTestDriver::query()`. JVM-free; values are KIP-derived and match Docker Streams 4.1.

- [ ] **Step 1: Add the golden data**

Append a `"versioned"` object to `crates/client-streams/tests/testdata/iqv2/behavior.json` (add the key alongside the existing `kv` / `window` keys — keep it valid JSON):

```json
  "versioned": {
    "retention_ms": 1000000,
    "records": [["k", 10, 100], ["k", 20, 200], ["k", 30, 300]],
    "latest": [30, 300, null],
    "as_of_250": [20, 200, 300],
    "as_of_50": null,
    "all_asc": [[10, 100, 200], [20, 200, 300], [30, 300, null]],
    "range_150_250_desc": [[20, 200, 300], [10, 100, 200]]
  }
```

> Each version triple is `[value, valid_from, valid_to]` (`valid_to=null` = ∞).
> Provenance: three in-order versions of key `k`; KIP-960/968 semantics give the
> values above (as-of 250 sees the 200-version superseded at 300; the
> `[150,250]` range overlaps the 100- and 200-versions but not the 300-version).
> Equivalent to Docker Streams 4.1 IQv2 over a versioned table — re-capture and
> overwrite if a JVM diff is found.

- [ ] **Step 2: Write the test**

Add to `crates/client-streams/tests/iqv2_golden.rs`. The crate root re-exports `VersionedKeyQuery`, `MultiVersionedKeyQuery`, `VersionedRecord`, `Materialized`, `StreamsBuilder`, `Consumed`, `StringSerde`, `I64Serde`, `TopologyTestDriver`, `StateQueryRequest` — confirm each against `src/lib.rs` and match the existing imports at the top of this test file. A versioned table is built with `builder.table(topic, Consumed, Materialized::with(ks, vs).as_versioned(store_name, retention))` — confirm the exact `builder.table` argument shape against an existing versioned-table test (`crates/client-streams/src/dsl/ktable.rs:1254` and `kstream.rs:2092` use `Materialized::with(StringSerde, I64Serde).as_versioned("vt", 600_000)`).

```rust
#[tokio::test]
async fn iqv2_versioned_key_and_multi_parity() {
    use crabka_client_streams::{
        Consumed, I64Serde, Materialized, MultiVersionedKeyQuery, StateQueryRequest, StreamsBuilder,
        StringSerde, TopologyTestDriver, VersionedKeyQuery, VersionedRecord,
    };

    let g = golden();
    let v = &g["versioned"];
    let retention = v["retention_ms"].as_i64().unwrap();

    let b = StreamsBuilder::new();
    b.table::<String, i64>(
        ["vt"],
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_versioned("vstore", retention),
    );
    let built = b.build("app").unwrap();
    let mut d = TopologyTestDriver::new(&built).unwrap();
    for rec in v["records"].as_array().unwrap() {
        let key = rec[0].as_str().unwrap().to_string();
        let val = rec[1].as_i64().unwrap();
        let ts = rec[2].as_i64().unwrap();
        d.pipe_input("vt", Consumed::with(StringSerde, I64Serde), Some(key.clone()), val, ts);
    }

    let rec = |triple: &serde_json::Value| -> Option<VersionedRecord<i64>> {
        if triple.is_null() {
            return None;
        }
        Some(VersionedRecord {
            value: triple[0].as_i64().unwrap(),
            valid_from: triple[1].as_i64().unwrap(),
            valid_to: triple[2].as_i64(), // null → None
        })
    };
    let recs = |arr: &serde_json::Value| -> Vec<VersionedRecord<i64>> {
        arr.as_array().unwrap().iter().map(|t| rec(t).unwrap()).collect()
    };

    // VersionedKeyQuery: latest.
    let latest = d
        .query(StateQueryRequest::in_store("vstore").with_query(VersionedKeyQuery::<String, i64>::with_key("k".into())))
        .await;
    assert_eq!(latest.only_partition_result().unwrap().result(), Some(&rec(&v["latest"])));

    // VersionedKeyQuery: as_of 250 and as_of 50.
    let asof = d
        .query(StateQueryRequest::in_store("vstore").with_query(VersionedKeyQuery::<String, i64>::with_key("k".into()).as_of(250)))
        .await;
    assert_eq!(asof.only_partition_result().unwrap().result(), Some(&rec(&v["as_of_250"])));
    let asof_miss = d
        .query(StateQueryRequest::in_store("vstore").with_query(VersionedKeyQuery::<String, i64>::with_key("k".into()).as_of(50)))
        .await;
    assert_eq!(asof_miss.only_partition_result().unwrap().result(), Some(&rec(&v["as_of_50"])));

    // MultiVersionedKeyQuery: all ascending.
    let all = d
        .query(StateQueryRequest::in_store("vstore").with_query(MultiVersionedKeyQuery::<String, i64>::with_key("k".into())))
        .await;
    assert_eq!(all.only_partition_result().unwrap().result(), Some(&recs(&v["all_asc"])));

    // MultiVersionedKeyQuery: [150,250] descending.
    let win = d
        .query(
            StateQueryRequest::in_store("vstore").with_query(
                MultiVersionedKeyQuery::<String, i64>::with_key("k".into())
                    .from_time(150)
                    .to_time(250)
                    .with_descending_timestamps(),
            ),
        )
        .await;
    assert_eq!(win.only_partition_result().unwrap().result(), Some(&recs(&v["range_150_250_desc"])));
}
```

> Adjust `builder.table`'s argument shape and the import paths to the crate's
> actual API if they differ (copy from the existing versioned-table tests). If
> piping a `(key, value, ts)` into the table doesn't set the version's
> `valid_from` to `ts`, check how the existing versioned-table tests pipe input
> (the versioned KTable source uses the record timestamp as `valid_from`).

- [ ] **Step 3: Run the test**

Run: `cargo test -p crabka-client-streams --test iqv2_golden -- versioned`
Expected: PASS. Also run the whole file: `cargo test -p crabka-client-streams --test iqv2_golden` (the existing kv/window/failure tests must still pass — the new `"versioned"` JSON key must not break their parsing).

- [ ] **Step 4: Commit**

```bash
git -C <worktree> add crates/client-streams/tests/iqv2_golden.rs \
  crates/client-streams/tests/testdata/iqv2/behavior.json
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "test(client-streams): IQv2 versioned-query goldens (KIP-960/968)"
```

---

## Task 5: Reconciliation + memory

**Files:** none (verification only) unless fixes are needed.

- [ ] **Step 1: Format + clippy**

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
```
Expected: clean. (Backtick bare doc identifiers — `IQv2`, `VersionedKeyQuery`, `MultiVersionedKeyQuery`, `valid_from` — if `doc_markdown` fires. Clippy cache can mask workspace lints: `touch` a suspect file and re-check the real `$?`.)

- [ ] **Step 2: Full crate test suite**

Run: `cargo test -p crabka-client-streams`
Expected: PASS — the whole suite is the gate (erasure mismatch is a runtime downcast).

- [ ] **Step 3: Workspace build**

Run: `cargo build --workspace`
Expected: success.

- [ ] **Step 4: Update memory**

Update the `project-kip1071-streams` memory: IQv2 slice **3b** done (`VersionedKeyQuery` KIP-960 + `MultiVersionedKeyQuery` KIP-968 on the 3a envelope; new `Iq2Query::{VersionedKey, MultiVersionedKey}` variants + `iq2_execute` on `VersionedBytesStore`; KIP-968 overlap rule `valid_from <= to && valid_to.map_or(true, |vt| vt > from)`, ascending by `valid_from`, tombstones skipped). The **entire IQv2 framework (KIP-796/960/968) is now complete** — that was the last item in the versioned-tables roadmap; note no remaining DSL-parity gaps from the original list.

- [ ] **Step 5: Final commit (if any fixes)**

```bash
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -am "chore(client-streams): reconcile IQv2 slice 3b (fmt/clippy/test green)" || echo "nothing to commit"
```

---

## Self-Review notes (carried into execution)

- **Spec coverage:** spec §1 versioned result types = Tasks 2/3 (`Option<VersionedRecord<V>>`, `Vec<VersionedRecord<V>>`); §2 slice 3b scope = all tasks; §7 versioned-range op = Task 2. Envelope (§3–§6) reused unchanged — `serve_iq2`/`dispatch`/`app::query`/`test_driver::query` are generic over `Query`, no edits.
- **Type consistency:** `Iq2Query::{VersionedKey { key, as_of }, MultiVersionedKey { key, from_ts, to_ts, descending }}` (Task 1) used identically in Task 2 (match arms) and Task 3 (`lower()`). Result types `Option<VersionedRecord<V>>` / `Vec<VersionedRecord<V>>` match across Task 2 (boxed), Task 3 (`Query::Result`), Task 4 (downcast/assert). `VersionedRecord { value, valid_from, valid_to }` field names consistent. `with_key`/`as_of`/`from_time`/`to_time`/`with_ascending_timestamps`/`with_descending_timestamps` consistent between Task 3 builders and Task 4 usage.
- **Empirical adjustments (flagged, not placeholders):** Task 4's `builder.table` argument shape + crate-root import names are to be matched against existing versioned-table tests (`ktable.rs:1254`, `kstream.rs:2092`); `is_none_or` vs `map_or(true, …)` in Task 2 depends on the toolchain. Each task says exactly where to confirm.
- **No envelope changes:** verified the 3a dispatch path is generic — the only files touched are `store/iq.rs`, `store/versioned.rs`, `runtime/iqv2/{query,mod}.rs`, `lib.rs`, and the goldens.
