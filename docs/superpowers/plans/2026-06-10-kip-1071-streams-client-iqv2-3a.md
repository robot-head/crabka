# IQv2 Framework — Slice 3a Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the JVM IQv2 dispatch envelope (`StateQueryRequest` → `KafkaStreams::query()` → `StateQueryResult`/`QueryResult`, `Position`, `PositionBound`, `FailureReason`) plus the four non-versioned query types (`KeyQuery`, `RangeQuery`, `WindowKeyQuery`, `WindowRangeQuery`) in `crabka-client-streams`, leaving the v1 `ReadOnly*Store` views completely untouched.

**Architecture:** Implied serdes (design "A′"): the user's key crosses the IQ channel as `Box<dyn Any + Send>`; the concrete store (which owns its serdes and stays generic over `K,V` behind `dyn IqQueryable`) downcasts the key, runs the op, and returns the typed result `R` boxed. `query::<Q>()` downcasts each partition's box back to `Q::Result`. A **second** mpsc channel (`iq2_tx`) carries IQv2 requests so the v1 byte path (`runtime/iq.rs`, `iq_view.rs`, `answer_iq`) is not modified at all. Per-partition results come from iterating the supervisor's per-task store copies (each tagged with `partition: i32` and a live `positions` map).

**Tech Stack:** Rust 2024, `tokio` (mpsc/oneshot), `async_trait`, `bytes`. Reference design: `docs/superpowers/specs/2026-06-10-kip-1071-streams-client-iqv2-design.md`.

**Batching (per CLAUDE.md — parallel where file sets are disjoint):**
- Batch 1: Task 1 (`store/iq.rs`).
- Batch 2 ∥: Task 2 (`store/kv.rs`), Task 3 (`store/window.rs`).
- Batch 3: Task 4 (the `iqv2/` envelope + module wiring).
- Batch 4 ∥: Task 5 (`runtime/task.rs`), Task 6 (`iqv2/dispatch.rs`), Task 7 (`test_driver.rs`).
- Batch 5: Task 8 (`runtime/thread.rs`).
- Batch 6: Task 9 (`runtime/app.rs`).
- Batch 7: Task 10 (golden tests + `ci.yml`).
- Batch 8: Task 11 (full-suite reconciliation).

All commits use the identity override (git identity is unset locally):
`git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit …`
**Subagents must `git -C <worktree>` and assert the branch is `claude/lucid-sanderson-b67884` before committing** (subagent shells reset cwd to the main repo; commits can otherwise land on `main`).

---

## Task 1: Store-layer query descriptor + execution hook

**Files:**
- Modify: `crates/client-streams/src/store/iq.rs`

This adds the lowered query enum (`Iq2Query`), a store-level failure enum (`Iq2Failure`), and the dyn-safe `iq2_execute` hook on `IqQueryable`. Keys travel as `Box<dyn Any + Send>`; the default impl returns `UnknownQueryType` so stores that don't handle a variant are safe.

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/client-streams/src/store/iq.rs`:

```rust
    #[tokio::test]
    async fn iq2_execute_default_is_unknown_query_type() {
        use super::{Iq2Failure, Iq2Query};
        // A session store has no IQv2 handler — default impl must reject.
        let s = SessionBytesStore::<String, i64>::in_memory(
            "s".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "s-changelog".into(),
        );
        let q: &dyn IqQueryable = s.as_iq().unwrap();
        let query = Iq2Query::Key {
            key: Box::new("k".to_string()),
        };
        assert_eq!(q.iq2_execute(&query).await.err(), Some(Iq2Failure::UnknownQueryType));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-client-streams --lib store::iq::tests::iq2_execute_default_is_unknown_query_type`
Expected: FAIL — `Iq2Query` / `Iq2Failure` / `iq2_execute` not found.

- [ ] **Step 3: Add the types + trait method**

At the top of `crates/client-streams/src/store/iq.rs`, change the imports line `use bytes::Bytes;` to also pull in `Any`:

```rust
use std::any::Any;

use async_trait::async_trait;
use bytes::Bytes;
```

After the `StoreKind` enum (before the `IqQueryable` trait), add:

```rust
/// A typed IQv2 query lowered to the store boundary. Keys travel as
/// `Box<dyn Any + Send>` (the raw `K`); the concrete store downcasts to its own
/// `K`, serializes with its own key serde, runs the op, and returns the typed
/// result (`Option<V>`, `Vec<(K,V)>`, …) boxed as `Box<dyn Any + Send>`.
///
/// Time bounds are plain `i64`; ordering/bound choices are flags. No serde and
/// no `K`/`V` appear here — that is the whole point of the byte-level boundary.
pub enum Iq2Query {
    /// `KeyQuery` — single key. Result: `Option<V>`.
    Key { key: Box<dyn Any + Send> },
    /// `RangeQuery` — `None` bound = unbounded that side. Result: `Vec<(K,V)>`.
    Range {
        lo: Option<Box<dyn Any + Send>>,
        hi: Option<Box<dyn Any + Send>>,
        descending: bool,
    },
    /// `WindowKeyQuery` — one key, window starts in `[from_ts, to_ts]`.
    /// Result: `Vec<(i64 /*windowStart*/, V)>`, ascending by start.
    WindowKey {
        key: Box<dyn Any + Send>,
        from_ts: i64,
        to_ts: i64,
    },
    /// `WindowRangeQuery` — key range × window-start range. `None` bound =
    /// unbounded that side. Result: `Vec<((K, i64 /*windowStart*/), V)>`,
    /// ascending by (key bytes, windowStart).
    WindowRange {
        lo: Option<Box<dyn Any + Send>>,
        hi: Option<Box<dyn Any + Send>>,
        from_ts: i64,
        to_ts: i64,
    },
}

/// Why a store could not execute an IQv2 query. The runtime maps these (plus its
/// own conditions: rebalancing, not-up-to-bound, not-active) into the public
/// `FailureReason`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Iq2Failure {
    /// This store kind has no handler for the requested query variant.
    UnknownQueryType,
    /// A key `Box<dyn Any>` did not downcast to this store's `K`.
    KeyTypeMismatch,
}
```

Then add this method to the `IqQueryable` trait body (after `iq_versioned_get_as_of`):

```rust
    /// IQv2 entry point. The store downcasts keys, (de)serializes with its own
    /// serdes, runs the op, and returns the typed result boxed. Default: this
    /// store kind handles no IQv2 query variant.
    async fn iq2_execute(
        &self,
        _query: &Iq2Query,
    ) -> Result<Box<dyn Any + Send>, Iq2Failure> {
        Err(Iq2Failure::UnknownQueryType)
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p crabka-client-streams --lib store::iq::tests::iq2_execute_default_is_unknown_query_type`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C <worktree> add crates/client-streams/src/store/iq.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(client-streams): IQv2 store-layer query descriptor + iq2_execute hook"
```

---

## Task 2: `KeyValueBytesStore::iq2_execute` (KeyQuery, RangeQuery)

**Files:**
- Modify: `crates/client-streams/src/store/kv.rs`

Depends on Task 1. Handles `Iq2Query::Key` (→ `Option<V>`) and `Iq2Query::Range` (→ `Vec<(K,V)>`, ascending by key bytes, reversed when `descending`). Uses `scan_all` + memcmp bound filtering so all four bound combinations (full / lower-only / upper-only / none) work uniformly.

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/client-streams/src/store/kv.rs`:

```rust
    #[tokio::test]
    async fn iq2_key_and_range() {
        use crate::store::iq::{Iq2Query, IqQueryable};
        let mut s = store();
        s.put("a".into(), 1).await;
        s.put("b".into(), 2).await;
        s.put("c".into(), 3).await;
        let q: &dyn IqQueryable = s.as_iq().unwrap();

        // KeyQuery hit / miss.
        let got = q
            .iq2_execute(&Iq2Query::Key { key: Box::new("b".to_string()) })
            .await
            .unwrap();
        assert_eq!(*got.downcast::<Option<i64>>().unwrap(), Some(2));
        let miss = q
            .iq2_execute(&Iq2Query::Key { key: Box::new("z".to_string()) })
            .await
            .unwrap();
        assert_eq!(*miss.downcast::<Option<i64>>().unwrap(), None);

        // RangeQuery inclusive [a,b] ascending.
        let r = q
            .iq2_execute(&Iq2Query::Range {
                lo: Some(Box::new("a".to_string())),
                hi: Some(Box::new("b".to_string())),
                descending: false,
            })
            .await
            .unwrap();
        assert_eq!(
            *r.downcast::<Vec<(String, i64)>>().unwrap(),
            vec![("a".to_string(), 1), ("b".to_string(), 2)]
        );

        // Unbounded both sides, descending → all, reversed.
        let all_desc = q
            .iq2_execute(&Iq2Query::Range { lo: None, hi: None, descending: true })
            .await
            .unwrap();
        assert_eq!(
            *all_desc.downcast::<Vec<(String, i64)>>().unwrap(),
            vec![("c".to_string(), 3), ("b".to_string(), 2), ("a".to_string(), 1)]
        );

        // Wrong key type → KeyTypeMismatch.
        use crate::store::iq::Iq2Failure;
        let bad = q.iq2_execute(&Iq2Query::Key { key: Box::new(7_i64) }).await;
        assert_eq!(bad.err(), Some(Iq2Failure::KeyTypeMismatch));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-client-streams --lib store::kv::tests::iq2_key_and_range`
Expected: FAIL — default `iq2_execute` returns `UnknownQueryType`, downcasts panic / asserts fail.

- [ ] **Step 3: Implement `iq2_execute` on the `IqQueryable` impl**

In `crates/client-streams/src/store/kv.rs`, change the impl header import region — the file already has `use std::any::Any;`. Add to the `impl … IqQueryable for KeyValueBytesStore` block (after `iq_kv_approx_count`):

```rust
    async fn iq2_execute(
        &self,
        query: &crate::store::iq::Iq2Query,
    ) -> Result<Box<dyn Any + Send>, crate::store::iq::Iq2Failure> {
        use crate::store::iq::{Iq2Failure, Iq2Query};
        let ser = |b: &Box<dyn Any + Send>| -> Result<bytes::Bytes, Iq2Failure> {
            let k = b.downcast_ref::<K>().ok_or(Iq2Failure::KeyTypeMismatch)?;
            Ok(self.key_serde.serialize(&self.changelog_topic, k))
        };
        match query {
            Iq2Query::Key { key } => {
                let kb = ser(key)?;
                let out: Option<V> = self.backend.get(&kb).await.map(|vb| {
                    self.value_serde
                        .deserialize(&self.changelog_topic, &vb)
                        .expect("iqv2 kv value deserialize")
                });
                Ok(Box::new(out))
            }
            Iq2Query::Range { lo, hi, descending } => {
                let lo_b = lo.as_ref().map(&ser).transpose()?;
                let hi_b = hi.as_ref().map(&ser).transpose()?;
                let mut rows: Vec<(K, V)> = Vec::new();
                for (kb, vb) in self.backend.scan_all().await {
                    if let Some(l) = &lo_b {
                        if kb.as_ref() < l.as_ref() {
                            continue;
                        }
                    }
                    if let Some(h) = &hi_b {
                        if kb.as_ref() > h.as_ref() {
                            continue;
                        }
                    }
                    rows.push((
                        self.key_serde
                            .deserialize(&self.changelog_topic, &kb)
                            .expect("iqv2 kv range key deserialize"),
                        self.value_serde
                            .deserialize(&self.changelog_topic, &vb)
                            .expect("iqv2 kv range value deserialize"),
                    ));
                }
                if *descending {
                    rows.reverse();
                }
                Ok(Box::new(rows))
            }
            _ => Err(Iq2Failure::UnknownQueryType),
        }
    }
```

> Note: `scan_all` returns entries in ascending key-byte order (the in-memory
> backend is a `BTreeMap`), so `rows` is ascending and `reverse()` yields exact
> descending order.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p crabka-client-streams --lib store::kv::tests::iq2_key_and_range`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C <worktree> add crates/client-streams/src/store/kv.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(client-streams): KeyValue iq2_execute (KeyQuery + RangeQuery)"
```

---

## Task 3: `WindowBytesStore::iq2_execute` (WindowKeyQuery, WindowRangeQuery)

**Files:**
- Modify: `crates/client-streams/src/store/window.rs`

Depends on Task 1. `WindowKey` mirrors the existing `iq_window_fetch` (one key, starts in `[from,to]`, ascending → `Vec<(i64,V)>`). `WindowRange` is the new op: a `scan_all` filtered by key-byte range × window-start range → `Vec<((K,i64),V)>`, ascending by `(key bytes, windowStart)`.

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/client-streams/src/store/window.rs`:

```rust
    #[tokio::test]
    async fn iq2_window_key_and_range() {
        use crate::store::iq::{Iq2Query, IqQueryable};
        let mut s = WindowBytesStore::<String, i64>::in_memory(
            "w".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "w-changelog".into(),
        );
        s.put("a".into(), 0, 10, 5).await;
        s.put("a".into(), 1000, 20, 1005).await;
        s.put("b".into(), 0, 30, 6).await;
        let q: &dyn IqQueryable = s.as_iq().unwrap();

        // WindowKeyQuery: key "a", starts in [0,1000], ascending.
        let wk = q
            .iq2_execute(&Iq2Query::WindowKey {
                key: Box::new("a".to_string()),
                from_ts: 0,
                to_ts: 1000,
            })
            .await
            .unwrap();
        assert_eq!(
            *wk.downcast::<Vec<(i64, i64)>>().unwrap(),
            vec![(0, 10), (1000, 20)]
        );

        // WindowRangeQuery: all keys, starts in [0,0] → a@0 and b@0, ascending by key.
        let wr = q
            .iq2_execute(&Iq2Query::WindowRange {
                lo: None,
                hi: None,
                from_ts: 0,
                to_ts: 0,
            })
            .await
            .unwrap();
        assert_eq!(
            *wr.downcast::<Vec<((String, i64), i64)>>().unwrap(),
            vec![(("a".to_string(), 0), 10), (("b".to_string(), 0), 30)]
        );

        // WindowRangeQuery: key range [b, b] only.
        let wr_b = q
            .iq2_execute(&Iq2Query::WindowRange {
                lo: Some(Box::new("b".to_string())),
                hi: Some(Box::new("b".to_string())),
                from_ts: 0,
                to_ts: 2000,
            })
            .await
            .unwrap();
        assert_eq!(
            *wr_b.downcast::<Vec<((String, i64), i64)>>().unwrap(),
            vec![(("b".to_string(), 0), 30)]
        );
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-client-streams --lib store::window::tests::iq2_window_key_and_range`
Expected: FAIL — default `iq2_execute`.

- [ ] **Step 3: Implement `iq2_execute` on the `IqQueryable` impl**

The file already has `use std::any::Any;`. Add to the `impl … IqQueryable for WindowBytesStore` block (after `iq_window_fetch`):

```rust
    async fn iq2_execute(
        &self,
        query: &crate::store::iq::Iq2Query,
    ) -> Result<Box<dyn Any + Send>, crate::store::iq::Iq2Failure> {
        use crate::store::iq::{Iq2Failure, Iq2Query};
        let ser = |b: &Box<dyn Any + Send>| -> Result<bytes::Bytes, Iq2Failure> {
            let k = b.downcast_ref::<K>().ok_or(Iq2Failure::KeyTypeMismatch)?;
            Ok(self.key_serde.serialize(&self.changelog_topic, k))
        };
        match query {
            Iq2Query::WindowKey { key, from_ts, to_ts } => {
                let kb = ser(key)?;
                let lo = store_key(&kb, *from_ts, 0);
                let hi = store_key(&kb, to_ts.saturating_add(1), 0);
                let mut out: Vec<(i64, V)> = Vec::new();
                for (sk, wrapped) in self.backend.range(&lo, &hi).await {
                    if key_bytes_of(&sk) != kb.as_ref() {
                        continue;
                    }
                    let (_ts, raw) = unwrap_value(&wrapped);
                    out.push((
                        window_start_of(&sk),
                        self.value_serde
                            .deserialize(&self.changelog_topic, raw)
                            .expect("iqv2 window value deserialize"),
                    ));
                }
                Ok(Box::new(out))
            }
            Iq2Query::WindowRange { lo, hi, from_ts, to_ts } => {
                let lo_b = lo.as_ref().map(&ser).transpose()?;
                let hi_b = hi.as_ref().map(&ser).transpose()?;
                let mut out: Vec<((K, i64), V)> = Vec::new();
                for (sk, wrapped) in self.backend.scan_all().await {
                    let ws = window_start_of(&sk);
                    if ws < *from_ts || ws > *to_ts {
                        continue;
                    }
                    let kbytes = key_bytes_of(&sk);
                    if let Some(l) = &lo_b {
                        if kbytes < l.as_ref() {
                            continue;
                        }
                    }
                    if let Some(h) = &hi_b {
                        if kbytes > h.as_ref() {
                            continue;
                        }
                    }
                    let key = self
                        .key_serde
                        .deserialize(&self.changelog_topic, kbytes)
                        .expect("iqv2 window range key deserialize");
                    let (_ts, raw) = unwrap_value(&wrapped);
                    let value = self
                        .value_serde
                        .deserialize(&self.changelog_topic, raw)
                        .expect("iqv2 window range value deserialize");
                    out.push(((key, ws), value));
                }
                Ok(Box::new(out))
            }
            _ => Err(Iq2Failure::UnknownQueryType),
        }
    }
```

> `store_key` layout is key-bytes-prefixed then 8-byte window start, so
> `scan_all` (BTreeMap) yields ascending `(key bytes, windowStart)` — exactly
> the WindowRangeQuery order. No explicit sort needed.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p crabka-client-streams --lib store::window::tests::iq2_window_key_and_range`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C <worktree> add crates/client-streams/src/store/window.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(client-streams): Window iq2_execute (WindowKeyQuery + WindowRangeQuery)"
```

---

## Task 4: IQv2 public envelope (`Query`, `StateQueryRequest`, results, `Position`)

**Files:**
- Create: `crates/client-streams/src/runtime/iqv2/mod.rs`
- Create: `crates/client-streams/src/runtime/iqv2/request.rs`
- Create: `crates/client-streams/src/runtime/iqv2/result.rs`
- Create: `crates/client-streams/src/runtime/iqv2/query.rs`
- Modify: `crates/client-streams/src/runtime/mod.rs` (add `pub mod iqv2;`)
- Modify: `crates/client-streams/src/lib.rs` (re-export the public surface)

Depends on Task 1 (lowers to `store::iq::Iq2Query`). These four files are interdependent (one coherent public API), so they ship as one task that compiles as a unit. The entry point reads exactly like the JVM: `StateQueryRequest::in_store("s").with_query(KeyQuery::with_key("a"))`.

- [ ] **Step 1: Write `request.rs`**

Create `crates/client-streams/src/runtime/iqv2/request.rs`:

```rust
//! IQv2 request envelope: `Position`/`PositionBound` (KIP-796 bounded
//! staleness), partition selection, and the finalized `StateQuery<Q>` that
//! `KafkaStreams::query` consumes.

use std::collections::{BTreeMap, BTreeSet};

use super::query::Query;

/// Source-topic consumed offsets folded into a store: topic → partition →
/// offset (the next offset to read, i.e. one past the last consumed record).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Position(pub BTreeMap<String, BTreeMap<i32, i64>>);

impl Position {
    /// Offset recorded for one topic-partition, if any.
    #[must_use]
    pub fn offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.0.get(topic).and_then(|m| m.get(&partition)).copied()
    }

    /// True if `self` meets or exceeds every `(topic, partition)` offset in
    /// `bound`. A bound naming a partition `self` has never advanced fails.
    #[must_use]
    pub(crate) fn dominates(&self, bound: &Position) -> bool {
        bound.0.iter().all(|(topic, parts)| {
            parts
                .iter()
                .all(|(p, off)| self.offset(topic, *p).is_some_and(|cur| cur >= *off))
        })
    }
}

/// Freshness bound for a query (KIP-796). `At` requires each partition's
/// `Position` to dominate the given one, else that partition fails fast with
/// `NotUpToBound` — the query never blocks.
#[derive(Debug, Clone, Default)]
pub enum PositionBound {
    #[default]
    Unbounded,
    At(Position),
}

/// Which local partitions to query.
#[derive(Debug, Clone, Default)]
pub(crate) enum PartitionSel {
    #[default]
    All,
    Set(BTreeSet<i32>),
}

/// A finalized IQv2 request: built via
/// `StateQueryRequest::in_store(name).with_query(q)`.
pub struct StateQuery<Q: Query> {
    pub(crate) store: String,
    pub(crate) query: Q,
    pub(crate) partitions: PartitionSel,
    pub(crate) bound: PositionBound,
    pub(crate) require_active: bool,
}

impl<Q: Query> StateQuery<Q> {
    /// Restrict to a specific set of local partitions (default: all).
    #[must_use]
    pub fn with_partitions(mut self, set: BTreeSet<i32>) -> Self {
        self.partitions = PartitionSel::Set(set);
        self
    }

    /// Query all locally assigned partitions (the default).
    #[must_use]
    pub fn with_all_partitions(mut self) -> Self {
        self.partitions = PartitionSel::All;
        self
    }

    /// Require each queried partition to meet a freshness bound.
    #[must_use]
    pub fn with_position_bound(mut self, bound: PositionBound) -> Self {
        self.bound = bound;
        self
    }

    /// Only serve from active (not standby/restoring) tasks.
    #[must_use]
    pub fn require_active(mut self) -> Self {
        self.require_active = true;
        self
    }
}

/// Entry point namespace: `StateQueryRequest::in_store("s").with_query(q)`.
pub struct StateQueryRequest;

impl StateQueryRequest {
    /// Begin a request against state store `name`.
    #[must_use]
    pub fn in_store(name: impl Into<String>) -> StateQueryRequestBuilder {
        StateQueryRequestBuilder { store: name.into() }
    }
}

/// Half-built request awaiting `.with_query(q)`.
pub struct StateQueryRequestBuilder {
    store: String,
}

impl StateQueryRequestBuilder {
    /// Attach the query, finalizing the request.
    #[must_use]
    pub fn with_query<Q: Query>(self, query: Q) -> StateQuery<Q> {
        StateQuery {
            store: self.store,
            query,
            partitions: PartitionSel::All,
            bound: PositionBound::Unbounded,
            require_active: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pos(entries: &[(&str, i32, i64)]) -> Position {
        let mut m: BTreeMap<String, BTreeMap<i32, i64>> = BTreeMap::new();
        for (t, p, o) in entries {
            m.entry((*t).to_string()).or_default().insert(*p, *o);
        }
        Position(m)
    }

    #[test]
    fn dominates_requires_all_bound_partitions_met() {
        let cur = pos(&[("in", 0, 10), ("in", 1, 5)]);
        assert!(cur.dominates(&pos(&[("in", 0, 10)])));
        assert!(cur.dominates(&pos(&[("in", 0, 9), ("in", 1, 5)])));
        assert!(!cur.dominates(&pos(&[("in", 0, 11)]))); // behind
        assert!(!cur.dominates(&pos(&[("other", 0, 1)]))); // unknown tp
    }
}
```

- [ ] **Step 2: Write `result.rs`**

Create `crates/client-streams/src/runtime/iqv2/result.rs`:

```rust
//! IQv2 results: per-partition `QueryResult<R>` aggregated into
//! `StateQueryResult<R>`.

use std::collections::BTreeMap;

use super::request::Position;

/// Why a partition's query did not produce a result (mirrors the JVM
/// `FailureReason` subset crabka can produce locally).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureReason {
    /// The store kind does not support this query variant.
    UnknownQueryType,
    /// The partition's `Position` did not meet the requested bound.
    NotUpToBound,
    /// The store exists in the topology but not on this partition's task.
    NotPresent,
    /// The partition is standby/restoring and an active-only query was asked.
    NotActive,
    /// The store name is not in the topology.
    DoesNotExist,
    /// Internal failure (e.g. a result/key type mismatch across the boundary).
    StoreException,
}

/// One partition's outcome.
pub enum QueryResult<R> {
    Success { result: R, position: Position },
    Failure { reason: FailureReason, message: String },
}

impl<R> QueryResult<R> {
    #[must_use]
    pub fn is_success(&self) -> bool {
        matches!(self, QueryResult::Success { .. })
    }
    #[must_use]
    pub fn result(&self) -> Option<&R> {
        match self {
            QueryResult::Success { result, .. } => Some(result),
            QueryResult::Failure { .. } => None,
        }
    }
    #[must_use]
    pub fn into_result(self) -> Option<R> {
        match self {
            QueryResult::Success { result, .. } => Some(result),
            QueryResult::Failure { .. } => None,
        }
    }
    #[must_use]
    pub fn position(&self) -> Option<&Position> {
        match self {
            QueryResult::Success { position, .. } => Some(position),
            QueryResult::Failure { .. } => None,
        }
    }
    #[must_use]
    pub fn failure_reason(&self) -> Option<FailureReason> {
        match self {
            QueryResult::Failure { reason, .. } => Some(*reason),
            QueryResult::Success { .. } => None,
        }
    }
    #[must_use]
    pub fn failure_message(&self) -> Option<&str> {
        match self {
            QueryResult::Failure { message, .. } => Some(message),
            QueryResult::Success { .. } => None,
        }
    }
}

/// All local partitions' outcomes for one query.
pub struct StateQueryResult<R> {
    partition_results: BTreeMap<i32, QueryResult<R>>,
}

impl<R> StateQueryResult<R> {
    #[must_use]
    pub(crate) fn new(partition_results: BTreeMap<i32, QueryResult<R>>) -> Self {
        Self { partition_results }
    }
    #[must_use]
    pub fn partition_results(&self) -> &BTreeMap<i32, QueryResult<R>> {
        &self.partition_results
    }
    /// The single partition's result, iff exactly one partition responded.
    #[must_use]
    pub fn only_partition_result(&self) -> Option<&QueryResult<R>> {
        if self.partition_results.len() == 1 {
            self.partition_results.values().next()
        } else {
            None
        }
    }
    /// Global-store result — always `None` in slice 3a (out of scope).
    #[must_use]
    pub fn global_result(&self) -> Option<&QueryResult<R>> {
        None
    }
}
```

- [ ] **Step 3: Write `query.rs`**

Create `crates/client-streams/src/runtime/iqv2/query.rs`:

```rust
//! IQv2 query objects. Each builder lowers (serde-free) to a
//! `store::iq::Iq2Query`; the store supplies the actual serdes.

use std::any::Any;
use std::marker::PhantomData;

use crate::store::iq::{Iq2Query, StoreKind};

mod sealed {
    pub trait Sealed {}
}

/// A typed IQv2 query. `Result` is the type `KafkaStreams::query` returns per
/// partition. Sealed: only the in-crate query types implement it.
pub trait Query: sealed::Sealed {
    /// What a successful `QueryResult` carries.
    type Result: 'static;
    #[doc(hidden)]
    fn store_kind(&self) -> StoreKind;
    #[doc(hidden)]
    fn lower(self) -> Iq2Query;
}

/// Single-key lookup. Result: `Option<V>`.
pub struct KeyQuery<K, V> {
    key: K,
    _v: PhantomData<fn() -> V>,
}
impl<K, V> KeyQuery<K, V> {
    #[must_use]
    pub fn with_key(key: K) -> Self {
        Self { key, _v: PhantomData }
    }
}
impl<K: Send + 'static, V: 'static> sealed::Sealed for KeyQuery<K, V> {}
impl<K: Send + 'static, V: 'static> Query for KeyQuery<K, V> {
    type Result = Option<V>;
    fn store_kind(&self) -> StoreKind {
        StoreKind::KeyValue
    }
    fn lower(self) -> Iq2Query {
        Iq2Query::Key { key: Box::new(self.key) }
    }
}

/// Key-range scan. `None` bound = unbounded that side. Result: `Vec<(K,V)>`.
pub struct RangeQuery<K, V> {
    lo: Option<K>,
    hi: Option<K>,
    descending: bool,
    _v: PhantomData<fn() -> V>,
}
impl<K, V> RangeQuery<K, V> {
    #[must_use]
    pub fn with_range(lo: K, hi: K) -> Self {
        Self { lo: Some(lo), hi: Some(hi), descending: false, _v: PhantomData }
    }
    #[must_use]
    pub fn with_lower_bound(lo: K) -> Self {
        Self { lo: Some(lo), hi: None, descending: false, _v: PhantomData }
    }
    #[must_use]
    pub fn with_upper_bound(hi: K) -> Self {
        Self { lo: None, hi: Some(hi), descending: false, _v: PhantomData }
    }
    #[must_use]
    pub fn with_no_bounds() -> Self {
        Self { lo: None, hi: None, descending: false, _v: PhantomData }
    }
    #[must_use]
    pub fn with_ascending_keys(mut self) -> Self {
        self.descending = false;
        self
    }
    #[must_use]
    pub fn with_descending_keys(mut self) -> Self {
        self.descending = true;
        self
    }
}
impl<K: Send + 'static, V: 'static> sealed::Sealed for RangeQuery<K, V> {}
impl<K: Send + 'static, V: 'static> Query for RangeQuery<K, V> {
    type Result = Vec<(K, V)>;
    fn store_kind(&self) -> StoreKind {
        StoreKind::KeyValue
    }
    fn lower(self) -> Iq2Query {
        let bx = |k: K| -> Box<dyn Any + Send> { Box::new(k) };
        Iq2Query::Range {
            lo: self.lo.map(bx),
            hi: self.hi.map(bx),
            descending: self.descending,
        }
    }
}

/// One key, window starts in `[from_time, to_time]`. Result: `Vec<(i64, V)>`.
pub struct WindowKeyQuery<K, V> {
    key: K,
    from_ts: i64,
    to_ts: i64,
    _v: PhantomData<fn() -> V>,
}
impl<K, V> WindowKeyQuery<K, V> {
    #[must_use]
    pub fn with_key(key: K) -> Self {
        Self { key, from_ts: i64::MIN, to_ts: i64::MAX, _v: PhantomData }
    }
    #[must_use]
    pub fn from_time(mut self, t: i64) -> Self {
        self.from_ts = t;
        self
    }
    #[must_use]
    pub fn to_time(mut self, t: i64) -> Self {
        self.to_ts = t;
        self
    }
}
impl<K: Send + 'static, V: 'static> sealed::Sealed for WindowKeyQuery<K, V> {}
impl<K: Send + 'static, V: 'static> Query for WindowKeyQuery<K, V> {
    type Result = Vec<(i64, V)>;
    fn store_kind(&self) -> StoreKind {
        StoreKind::Window
    }
    fn lower(self) -> Iq2Query {
        Iq2Query::WindowKey {
            key: Box::new(self.key),
            from_ts: self.from_ts,
            to_ts: self.to_ts,
        }
    }
}

/// Key range × window-start range. Result: `Vec<((K, i64), V)>`.
pub struct WindowRangeQuery<K, V> {
    lo: Option<K>,
    hi: Option<K>,
    from_ts: i64,
    to_ts: i64,
    _v: PhantomData<fn() -> V>,
}
impl<K, V> WindowRangeQuery<K, V> {
    #[must_use]
    pub fn with_key_range(lo: K, hi: K) -> Self {
        Self { lo: Some(lo), hi: Some(hi), from_ts: i64::MIN, to_ts: i64::MAX, _v: PhantomData }
    }
    #[must_use]
    pub fn with_all_keys() -> Self {
        Self { lo: None, hi: None, from_ts: i64::MIN, to_ts: i64::MAX, _v: PhantomData }
    }
    #[must_use]
    pub fn from_time(mut self, t: i64) -> Self {
        self.from_ts = t;
        self
    }
    #[must_use]
    pub fn to_time(mut self, t: i64) -> Self {
        self.to_ts = t;
        self
    }
}
impl<K: Send + 'static, V: 'static> sealed::Sealed for WindowRangeQuery<K, V> {}
impl<K: Send + 'static, V: 'static> Query for WindowRangeQuery<K, V> {
    type Result = Vec<((K, i64), V)>;
    fn store_kind(&self) -> StoreKind {
        StoreKind::Window
    }
    fn lower(self) -> Iq2Query {
        let bx = |k: K| -> Box<dyn Any + Send> { Box::new(k) };
        Iq2Query::WindowRange {
            lo: self.lo.map(bx),
            hi: self.hi.map(bx),
            from_ts: self.from_ts,
            to_ts: self.to_ts,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lowering_picks_the_right_variant_and_kind() {
        let kq = KeyQuery::<String, i64>::with_key("a".into());
        assert_eq!(kq.store_kind(), StoreKind::KeyValue);
        assert!(matches!(kq.lower(), Iq2Query::Key { .. }));

        let rq = RangeQuery::<String, i64>::with_lower_bound("a".into()).with_descending_keys();
        assert!(matches!(rq.lower(), Iq2Query::Range { lo: Some(_), hi: None, descending: true }));

        let wk = WindowKeyQuery::<String, i64>::with_key("a".into()).from_time(0).to_time(9);
        assert_eq!(wk.store_kind(), StoreKind::Window);
        assert!(matches!(wk.lower(), Iq2Query::WindowKey { from_ts: 0, to_ts: 9, .. }));

        let wr = WindowRangeQuery::<String, i64>::with_all_keys();
        assert!(matches!(wr.lower(), Iq2Query::WindowRange { lo: None, hi: None, .. }));
    }
}
```

- [ ] **Step 4: Write `mod.rs`**

Create `crates/client-streams/src/runtime/iqv2/mod.rs`:

```rust
//! Interactive Queries v2 (KIP-796 / 960 / 968): the `StateQueryRequest` →
//! `KafkaStreams::query` → `StateQueryResult` envelope and its query objects.
//! Coexists with the v1 `ReadOnly*Store` views (see `runtime::iq_view`) on a
//! separate channel; v1 is untouched.

pub(crate) mod dispatch;
pub mod query;
pub mod request;
pub mod result;

pub use query::{KeyQuery, Query, RangeQuery, WindowKeyQuery, WindowRangeQuery};
pub use request::{Position, PositionBound, StateQuery, StateQueryRequest};
pub use result::{FailureReason, QueryResult, StateQueryResult};
```

> `dispatch` is declared here but created in Task 6. To keep this task
> compiling on its own, **temporarily** comment the `pub(crate) mod dispatch;`
> line; Task 6 uncomments it. (Or create an empty `dispatch.rs` now — Task 6
> fills it. Prefer the empty-file approach so `mod.rs` stays final.)

Create an empty placeholder so this task compiles standalone:
`crates/client-streams/src/runtime/iqv2/dispatch.rs` containing only:

```rust
//! Filled in Task 6 (IQv2 channel message + result assembly).
```

- [ ] **Step 5: Wire the module + re-exports**

In `crates/client-streams/src/runtime/mod.rs`, add (next to the other `pub mod` lines):

```rust
pub mod iqv2;
```

In `crates/client-streams/src/lib.rs`, add to the public re-exports (find where `KafkaStreams` / IQ types are re-exported and mirror the style):

```rust
pub use runtime::iqv2::{
    FailureReason, KeyQuery, Position, PositionBound, Query, QueryResult, RangeQuery,
    StateQuery, StateQueryRequest, StateQueryResult, WindowKeyQuery, WindowRangeQuery,
};
```

- [ ] **Step 6: Run tests + build**

Run: `cargo test -p crabka-client-streams --lib runtime::iqv2::`
Expected: PASS (`request::tests::dominates_*`, `query::tests::lowering_*`).
Run: `cargo build -p crabka-client-streams`
Expected: success.

- [ ] **Step 7: Commit**

```bash
git -C <worktree> add crates/client-streams/src/runtime/iqv2/ \
  crates/client-streams/src/runtime/mod.rs crates/client-streams/src/lib.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(client-streams): IQv2 public envelope (Query/StateQueryRequest/results/Position)"
```

---

## Task 5: `StreamTask::position()` accessor

**Files:**
- Modify: `crates/client-streams/src/runtime/task.rs`

Depends on Task 4 (`Position` type). Exposes the task's live consumed-offset map as a `Position`.

- [ ] **Step 1: Write the failing test**

Add a test to `crates/client-streams/src/runtime/task.rs` (in its `#[cfg(test)] mod tests`; if none exists, add one at the end of the file). Construct a task via the existing test helpers if present; otherwise assert through a minimal constructor. Use this test, adapting the constructor call to the file's existing test setup:

```rust
    #[test]
    fn position_reflects_seeded_source_partitions() {
        use crate::runtime::io::TopicPartition;
        // A task seeded with one source partition starts at offset 0.
        let task = make_test_task(vec![TopicPartition { topic: "in".into(), partition: 2 }]);
        let pos = task.position();
        assert_eq!(pos.offset("in", 2), Some(0));
        assert_eq!(pos.offset("in", 9), None);
    }
```

> If `task.rs` has no `make_test_task` helper, add one in the test module that
> calls `StreamTask::new(...)` with mock `producer`/`store` (reuse whatever
> mock the crate already uses in other `runtime` tests — search
> `crates/client-streams/src/runtime` for an existing `RecordProducer` /
> `OffsetStore` test double and import it). Keep the helper minimal: it only
> needs a task whose `positions` map is seeded.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-client-streams --lib runtime::task::tests::position_reflects_seeded_source_partitions`
Expected: FAIL — `position` method not found.

- [ ] **Step 3: Add the accessor**

In `crates/client-streams/src/runtime/task.rs`, add to `impl StreamTask` (near `registry()`):

```rust
    /// Snapshot the task's consumed source offsets as an IQv2 `Position`
    /// (topic → partition → next-offset). Used to tag query results.
    pub(crate) fn position(&self) -> crate::runtime::iqv2::request::Position {
        use std::collections::BTreeMap;
        let mut m: BTreeMap<String, BTreeMap<i32, i64>> = BTreeMap::new();
        for ((topic, p), off) in &self.positions {
            m.entry(topic.clone()).or_default().insert(*p, *off);
        }
        crate::runtime::iqv2::request::Position(m)
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p crabka-client-streams --lib runtime::task::tests::position_reflects_seeded_source_partitions`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C <worktree> add crates/client-streams/src/runtime/task.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(client-streams): StreamTask::position() accessor for IQv2"
```

---

## Task 6: IQv2 channel message + result assembly (`dispatch.rs`)

**Files:**
- Modify: `crates/client-streams/src/runtime/iqv2/dispatch.rs` (created empty in Task 4)

Depends on Task 4. Defines the supervisor request (`Iq2Request`), the raw per-partition outcome (`Iq2Outcome`), and `assemble()` which downcasts each partition's `Box<dyn Any>` into `R` to build a `StateQueryResult<R>`.

- [ ] **Step 1: Write the failing test**

Replace `crates/client-streams/src/runtime/iqv2/dispatch.rs` placeholder with the implementation below (Step 2), which includes this test; run order is the same (write test, see it compile-fail without the impl, then pass). Test:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::iqv2::request::Position;
    use crate::runtime::iqv2::result::FailureReason;

    #[test]
    fn assemble_downcasts_success_and_maps_failures() {
        let ok: Box<dyn std::any::Any + Send> = Box::new(Some(7_i64));
        let outcome = Iq2Outcome {
            per_partition: vec![
                (0, Position::default(), Ok(ok)),
                (1, Position::default(), Err(FailureReason::NotUpToBound)),
            ],
            had_tasks: true,
        };
        let res = assemble::<Option<i64>>(outcome);
        assert_eq!(res.partition_results().len(), 2);
        assert_eq!(res.partition_results()[&0].result(), Some(&Some(7)));
        assert_eq!(
            res.partition_results()[&1].failure_reason(),
            Some(FailureReason::NotUpToBound)
        );
    }

    #[test]
    fn assemble_type_mismatch_is_store_exception() {
        let wrong: Box<dyn std::any::Any + Send> = Box::new("not an i64".to_string());
        let outcome = Iq2Outcome {
            per_partition: vec![(0, Position::default(), Ok(wrong))],
            had_tasks: true,
        };
        let res = assemble::<Option<i64>>(outcome);
        assert_eq!(
            res.partition_results()[&0].failure_reason(),
            Some(FailureReason::StoreException)
        );
    }
}
```

- [ ] **Step 2: Write the implementation**

Write `crates/client-streams/src/runtime/iqv2/dispatch.rs`:

```rust
//! IQv2 supervisor channel message and per-partition result assembly. This is
//! the bridge between the public envelope and the byte-level store hook.

use std::any::Any;
use std::collections::BTreeMap;

use tokio::sync::oneshot;

use crate::store::iq::{Iq2Query, StoreKind};

use super::request::{PartitionSel, Position, PositionBound};
use super::result::{FailureReason, QueryResult, StateQueryResult};

/// One IQv2 query addressed to the supervisor (sent on the dedicated `iq2`
/// channel; the v1 byte channel is untouched).
pub(crate) struct Iq2Request {
    pub store: String,
    pub kind: StoreKind,
    pub query: Iq2Query,
    pub partitions: PartitionSel,
    pub bound: PositionBound,
    pub require_active: bool,
    pub reply: oneshot::Sender<Iq2Outcome>,
}

/// Raw per-partition outcomes from the supervisor, before downcast to `R`.
pub(crate) struct Iq2Outcome {
    /// `(partition, position, Ok(boxed R) | Err(failure))` for each responding task.
    pub per_partition: Vec<(i32, Position, Result<Box<dyn Any + Send>, FailureReason>)>,
    /// Whether the instance had any tasks (distinguishes rebalancing from absent).
    pub had_tasks: bool,
}

/// Downcast each partition's boxed result into `R` and build the typed
/// `StateQueryResult`. A box that does not downcast to `R` becomes a
/// `StoreException` failure for that partition.
pub(crate) fn assemble<R: 'static>(outcome: Iq2Outcome) -> StateQueryResult<R> {
    let mut map: BTreeMap<i32, QueryResult<R>> = BTreeMap::new();
    for (partition, position, res) in outcome.per_partition {
        let qr = match res {
            Ok(boxed) => match boxed.downcast::<R>() {
                Ok(r) => QueryResult::Success { result: *r, position },
                Err(_) => QueryResult::Failure {
                    reason: FailureReason::StoreException,
                    message: "IQv2 result type mismatch".to_string(),
                },
            },
            Err(reason) => QueryResult::Failure {
                reason,
                message: format!("{reason:?}"),
            },
        };
        map.insert(partition, qr);
    }
    StateQueryResult::new(map)
}
```

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test -p crabka-client-streams --lib runtime::iqv2::dispatch::tests`
Expected: PASS (both tests).

- [ ] **Step 4: Commit**

```bash
git -C <worktree> add crates/client-streams/src/runtime/iqv2/dispatch.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(client-streams): IQv2 channel message + StateQueryResult assembly"
```

---

## Task 7: `TopologyTestDriver::query()` helper

**Files:**
- Modify: `crates/client-streams/src/test_driver.rs`

Depends on Task 4 (envelope) + Tasks 2/3 (store `iq2_execute`). Runs an IQv2 query against the driver's single graph at partition 0, so behavioral goldens (Task 10) need no broker. `Position` is `default()` (the driver doesn't track source offsets).

- [ ] **Step 1: Write the failing test**

Add to the `tests` in `crates/client-streams/src/test_driver.rs` (or wherever its existing IQ helpers are exercised — mirror the `iq_golden.rs` style). A minimal in-file test:

```rust
    #[tokio::test]
    async fn iqv2_query_kv_via_driver() {
        use crate::processor::serde::{Consumed, I64Serde, StringSerde};
        use crate::runtime::iqv2::{KeyQuery, StateQueryRequest};
        use crate::StreamsBuilder;

        let b = StreamsBuilder::new();
        b.stream::<String, String>(["in"]).group_by_key().count("counts");
        let built = b.build("app").unwrap();
        let mut d = TopologyTestDriver::new(&built).unwrap();
        for v in ["a", "a", "b"] {
            d.pipe_input("in", Consumed::with(StringSerde, StringSerde), Some(v.to_string()), v.to_string(), 0);
        }

        let res = d
            .query(StateQueryRequest::in_store("counts").with_query(KeyQuery::<String, i64>::with_key("a".into())))
            .await;
        let only = res.only_partition_result().unwrap();
        assert_eq!(only.result(), Some(&Some(2)));
    }
```

> Adjust the topology-building call to match the crate's actual DSL surface
> (`StreamsBuilder`, `group_by_key`, `count`) as used in
> `crates/client-streams/tests/iq_golden.rs`. Copy that file's exact builder
> calls if signatures differ.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-client-streams --lib test_driver::tests::iqv2_query_kv_via_driver`
Expected: FAIL — `TopologyTestDriver::query` not found.

- [ ] **Step 3: Implement the helper**

Add to `impl TopologyTestDriver` in `crates/client-streams/src/test_driver.rs`:

```rust
    /// Run an IQv2 query against the single test graph (partition 0). `Position`
    /// is empty (the driver does not track source offsets); use unit tests on
    /// `Position::dominates` for bound behavior.
    pub async fn query<Q: crate::runtime::iqv2::Query>(
        &self,
        req: crate::runtime::iqv2::StateQuery<Q>,
    ) -> crate::runtime::iqv2::StateQueryResult<Q::Result> {
        use std::collections::BTreeMap;

        use crate::runtime::iqv2::request::Position;
        use crate::runtime::iqv2::result::{FailureReason, QueryResult, StateQueryResult};

        let store_name = req.store.clone();
        let kind = req.query.store_kind();
        let Some(store) = self.graph.stores.iq_get(&store_name) else {
            return StateQueryResult::new(BTreeMap::new());
        };
        if store.kind() != kind {
            let mut m = BTreeMap::new();
            m.insert(
                0,
                QueryResult::Failure { reason: FailureReason::DoesNotExist, message: "wrong store kind".into() },
            );
            return StateQueryResult::new(m);
        }
        let lowered = req.query.lower();
        let qr = match store.iq2_execute(&lowered).await {
            Ok(boxed) => match boxed.downcast::<Q::Result>() {
                Ok(r) => QueryResult::Success { result: *r, position: Position::default() },
                Err(_) => QueryResult::Failure {
                    reason: FailureReason::StoreException,
                    message: "IQv2 result type mismatch".into(),
                },
            },
            Err(crate::store::iq::Iq2Failure::UnknownQueryType) => QueryResult::Failure {
                reason: FailureReason::UnknownQueryType,
                message: "unknown query type".into(),
            },
            Err(crate::store::iq::Iq2Failure::KeyTypeMismatch) => QueryResult::Failure {
                reason: FailureReason::StoreException,
                message: "key type mismatch".into(),
            },
        };
        let mut m = BTreeMap::new();
        m.insert(0, qr);
        StateQueryResult::new(m)
    }
```

> `StateQuery`, `QueryResult::new`/`StateQueryResult::new`, and
> `request::Position` are `pub(crate)`-accessible from within the crate. The
> public re-export added in Task 4 covers external use.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p crabka-client-streams --lib test_driver::tests::iqv2_query_kv_via_driver`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C <worktree> add crates/client-streams/src/test_driver.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(client-streams): TopologyTestDriver::query() for IQv2 goldens"
```

---

## Task 8: Supervisor `serve_iq2`

**Files:**
- Modify: `crates/client-streams/src/runtime/thread.rs`

Depends on Task 5 (`position()`) + Task 6 (`Iq2Request`/`Iq2Outcome`). Adds a NEW method beside `serve_iq` (the v1 method is not touched). Iterates per-task store copies, applies partition/active/bound gates, calls `iq2_execute`, and tags each result with the task's partition + position.

- [ ] **Step 1: Add the method**

In `crates/client-streams/src/runtime/thread.rs`, add after `serve_iq`:

```rust
    /// Serve one IQv2 query: per-partition (no merge). Filters tasks by the
    /// requested partition set, applies the active-only and position-bound
    /// gates, and tags each store's typed result with its partition + position.
    pub(crate) async fn serve_iq2(&mut self, req: crate::runtime::iqv2::dispatch::Iq2Request) {
        use crate::runtime::iqv2::dispatch::Iq2Outcome;
        use crate::runtime::iqv2::request::{PartitionSel, PositionBound};
        use crate::runtime::iqv2::result::FailureReason;
        use crate::runtime::task::TaskRole;
        use crate::store::iq::Iq2Failure;

        let had_tasks = !self.tasks.is_empty();
        let mut per_partition = Vec::new();
        for t in self.tasks.values() {
            if let PartitionSel::Set(set) = &req.partitions {
                if !set.contains(&t.partition) {
                    continue;
                }
            }
            let Some(store) = t.registry().iq_get(&req.store) else {
                continue;
            };
            let pos = t.position();
            if store.kind() != req.kind {
                per_partition.push((t.partition, pos, Err(FailureReason::NotPresent)));
                continue;
            }
            if req.require_active && t.role != TaskRole::Active {
                per_partition.push((t.partition, pos, Err(FailureReason::NotActive)));
                continue;
            }
            if let PositionBound::At(bound) = &req.bound {
                if !pos.dominates(bound) {
                    per_partition.push((t.partition, pos, Err(FailureReason::NotUpToBound)));
                    continue;
                }
            }
            let outcome = match store.iq2_execute(&req.query).await {
                Ok(boxed) => Ok(boxed),
                Err(Iq2Failure::UnknownQueryType) => Err(FailureReason::UnknownQueryType),
                Err(Iq2Failure::KeyTypeMismatch) => Err(FailureReason::StoreException),
            };
            per_partition.push((t.partition, pos, outcome));
        }
        let _ = req.reply.send(Iq2Outcome { per_partition, had_tasks });
    }
```

> Same `&mut self` rationale as `serve_iq`: `&dyn IqQueryable` is `Send` but not
> `Sync`, and `iq2_execute` is awaited while the borrow is held; `&mut self`
> keeps the supervisor future `Send` without requiring `StreamThread: Sync`.
> `Position::dominates` is `pub(crate)`; `Position` is `Clone` but we only need
> `pos` once per branch — note `pos` is moved into the push, and the
> `PositionBound::At` branch needs `pos` after the `dominates` borrow, so clone
> there: change that branch's check to `if !pos.dominates(bound)` (borrow) then
> push `pos` (move) — the borrow ends before the move, so no clone is needed.

If `TaskRole` is not `PartialEq`, compare via `matches!(t.role, TaskRole::Active)` instead of `!=`. Confirm `TaskRole`'s path (`crate::runtime::task::TaskRole`) and derive — adjust the import/compare to match.

- [ ] **Step 2: Build to verify it compiles**

Run: `cargo build -p crabka-client-streams`
Expected: success. (Behavior is covered end-to-end by Task 7's driver path and Task 6's `assemble` unit tests; `serve_iq2`'s own integration needs a live supervisor, exercised opportunistically in Task 10 if a broker-backed test exists — otherwise this compile + the assemble/driver tests are the gate.)

- [ ] **Step 3: Commit**

```bash
git -C <worktree> add crates/client-streams/src/runtime/thread.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(client-streams): supervisor serve_iq2 (per-partition IQv2 dispatch)"
```

---

## Task 9: `KafkaStreams::query()` + dedicated IQv2 channel

**Files:**
- Modify: `crates/client-streams/src/runtime/app.rs`

Depends on Task 6 + Task 8. Adds a second mpsc channel (`iq2_tx`/`iq2_rx`), a `select!` arm routing to `serve_iq2`, and the public async `query()` method. The v1 `iq_tx` channel and accessors are untouched.

- [ ] **Step 1: Add the channel + select arm**

In `crates/client-streams/src/runtime/app.rs`:

Add to imports:
```rust
use crate::runtime::iqv2::dispatch::Iq2Request;
use crate::runtime::iqv2::{Query, StateQuery, StateQueryResult};
```

Add a field to `struct KafkaStreams` (next to `iq_tx`):
```rust
    /// Channel to the supervisor for IQv2 queries (separate from the v1 `iq_tx`).
    iq2_tx: mpsc::Sender<Iq2Request>,
```

In `start()`, beside `let (iq_tx, mut iq_rx) = mpsc::channel::<IqRequest>(64);` add:
```rust
        let (iq2_tx, mut iq2_rx) = mpsc::channel::<Iq2Request>(64);
```

Add a `select!` arm inside the supervisor loop (beside the `iq_rx.recv()` arm):
```rust
                    Some(req) = iq2_rx.recv() => {
                        thread.serve_iq2(req).await;
                    }
```

Add `iq2_tx` to the returned `Self { … }`:
```rust
            iq2_tx,
```

- [ ] **Step 2: Add the `query()` method**

Add to `impl KafkaStreams` (after the v1 accessors, before `close`):

```rust
    /// Run an IQv2 query against locally assigned partitions and return one
    /// `QueryResult` per partition. Serde-free: the store supplies its own
    /// serdes. If the instance is not running, the result has no partitions.
    pub async fn query<Q: Query>(&self, req: StateQuery<Q>) -> StateQueryResult<Q::Result> {
        use crate::runtime::iqv2::dispatch::{assemble, Iq2Request};

        if self.state != KafkaStreamsState::Running {
            return StateQueryResult::new(std::collections::BTreeMap::new());
        }
        let kind = req.query.store_kind();
        let (reply, rx) = tokio::sync::oneshot::channel();
        let iq2 = Iq2Request {
            store: req.store,
            kind,
            query: req.query.lower(),
            partitions: req.partitions,
            bound: req.bound,
            require_active: req.require_active,
            reply,
        };
        if self.iq2_tx.send(iq2).await.is_err() {
            return StateQueryResult::new(std::collections::BTreeMap::new());
        }
        match rx.await {
            Ok(outcome) => assemble::<Q::Result>(outcome),
            Err(_) => StateQueryResult::new(std::collections::BTreeMap::new()),
        }
    }
```

> `req` is `StateQuery<Q>` by value: read `store_kind()` (borrow) before
> `lower()` (move), and move `store`/`partitions`/`bound`/`require_active` out
> field-by-field — Rust allows the partial moves since `req` is owned and not
> used afterward. `StateQueryResult::new` and `dispatch` items are
> `pub(crate)`-visible from `app.rs`.

- [ ] **Step 3: Build to verify it compiles**

Run: `cargo build -p crabka-client-streams`
Expected: success.

- [ ] **Step 4: Commit**

```bash
git -C <worktree> add crates/client-streams/src/runtime/app.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(client-streams): KafkaStreams::query() + dedicated IQv2 channel"
```

---

## Task 10: Behavioral goldens + CI coverage

**Files:**
- Create: `crates/client-streams/tests/iqv2_golden.rs`
- Create: `crates/client-streams/tests/testdata/iqv2/behavior.json`
- Modify: `.github/workflows/ci.yml` (add `iqv2_golden` to the crate's llvm-cov `--test` list)

Depends on Task 7 (`TopologyTestDriver::query`). The JSON is the committed source of truth; its values match Docker Streams 4.1 (`apache/kafka:4.x` / `cp-kafka`) for the same inputs — capture once with the harness convention used by `iq_golden.rs`, then assert via the driver (JVM-free at test time).

- [ ] **Step 1: Write the golden data**

Create `crates/client-streams/tests/testdata/iqv2/behavior.json`:

```json
{
  "kv": {
    "records": ["a", "a", "b", "c"],
    "key_a": 2,
    "key_z": null,
    "range_a_b_asc": [["a", 2], ["b", 1]],
    "range_all_desc": [["c", 1], ["b", 1], ["a", 2]],
    "range_lower_b": [["b", 1], ["c", 1]]
  },
  "window": {
    "size_ms": 1000,
    "records": [["a", 0], ["a", 0], ["b", 0], ["a", 1000]],
    "wkey_a_0_2000": [[0, 2], [1000, 1]],
    "wrange_all_0_0": [[["a", 0], 2], [["b", 0], 1]]
  }
}
```

> Provenance: counts a windowed/keyed aggregation of the listed records.
> `kv` uses `group_by_key().count("counts")`; `a` appears twice → 2, `b`,`c`
> once → 1. `window` uses a 1000ms tumbling count: `a` twice in window 0 → 2,
> once in window 1000 → 1; `b` once in window 0 → 1. These equal the JVM
> Streams 4.1 outputs for the same inputs; re-capture and overwrite if a JVM
> diff is found.

- [ ] **Step 2: Write the test**

Create `crates/client-streams/tests/iqv2_golden.rs`:

```rust
//! IQv2 (KIP-796/806) behavioral parity, replayed through `TopologyTestDriver`.
//! Ground truth: Docker Streams 4.1 (see testdata/iqv2/behavior.json).

use crabka_client_streams::{
    Consumed, I64Serde, KeyQuery, RangeQuery, StateQueryRequest, StreamsBuilder, StringSerde,
    TimeWindows, WindowKeyQuery, WindowRangeQuery,
};
use serde_json::Value;

fn golden() -> Value {
    let raw = include_str!("testdata/iqv2/behavior.json");
    serde_json::from_str(raw).unwrap()
}

#[tokio::test]
async fn iqv2_kv_key_and_range_parity() {
    let g = golden();
    let kv = &g["kv"];

    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"]).group_by_key().count("counts");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for v in kv["records"].as_array().unwrap() {
        let v = v.as_str().unwrap().to_string();
        d.pipe_input("in", Consumed::with(StringSerde, StringSerde), Some(v.clone()), v, 0);
    }

    // KeyQuery
    let got_a = d
        .query(StateQueryRequest::in_store("counts").with_query(KeyQuery::<String, i64>::with_key("a".into())))
        .await;
    assert_eq!(got_a.only_partition_result().unwrap().result(), Some(&Some(kv["key_a"].as_i64().unwrap())));
    let got_z = d
        .query(StateQueryRequest::in_store("counts").with_query(KeyQuery::<String, i64>::with_key("z".into())))
        .await;
    assert_eq!(got_z.only_partition_result().unwrap().result(), Some(&None));

    // RangeQuery [a,b] ascending
    let r = d
        .query(StateQueryRequest::in_store("counts").with_query(RangeQuery::<String, i64>::with_range("a".into(), "b".into())))
        .await;
    let want = pairs(&kv["range_a_b_asc"]);
    assert_eq!(r.only_partition_result().unwrap().result(), Some(&want));

    // RangeQuery all, descending
    let rd = d
        .query(StateQueryRequest::in_store("counts").with_query(RangeQuery::<String, i64>::with_no_bounds().with_descending_keys()))
        .await;
    assert_eq!(rd.only_partition_result().unwrap().result(), Some(&pairs(&kv["range_all_desc"])));

    // RangeQuery lower-bound b
    let rl = d
        .query(StateQueryRequest::in_store("counts").with_query(RangeQuery::<String, i64>::with_lower_bound("b".into())))
        .await;
    assert_eq!(rl.only_partition_result().unwrap().result(), Some(&pairs(&kv["range_lower_b"])));
}

#[tokio::test]
async fn iqv2_window_key_and_range_parity() {
    let g = golden();
    let w = &g["window"];
    let size = w["size_ms"].as_i64().unwrap();

    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by(TimeWindows::of_size_with_no_grace(size))
        .count("wcounts");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for rec in w["records"].as_array().unwrap() {
        let key = rec[0].as_str().unwrap().to_string();
        let ts = rec[1].as_i64().unwrap();
        d.pipe_input("in", Consumed::with(StringSerde, StringSerde), Some(key.clone()), key, ts);
    }

    // WindowKeyQuery: key a, [0, 2000]
    let wk = d
        .query(
            StateQueryRequest::in_store("wcounts")
                .with_query(WindowKeyQuery::<String, i64>::with_key("a".into()).from_time(0).to_time(2000)),
        )
        .await;
    let want_wk: Vec<(i64, i64)> = w["wkey_a_0_2000"].as_array().unwrap().iter()
        .map(|p| (p[0].as_i64().unwrap(), p[1].as_i64().unwrap())).collect();
    assert_eq!(wk.only_partition_result().unwrap().result(), Some(&want_wk));

    // WindowRangeQuery: all keys, starts in [0,0]
    let wr = d
        .query(
            StateQueryRequest::in_store("wcounts")
                .with_query(WindowRangeQuery::<String, i64>::with_all_keys().from_time(0).to_time(0)),
        )
        .await;
    let want_wr: Vec<((String, i64), i64)> = w["wrange_all_0_0"].as_array().unwrap().iter()
        .map(|p| ((p[0][0].as_str().unwrap().to_string(), p[0][1].as_i64().unwrap()), p[1].as_i64().unwrap()))
        .collect();
    assert_eq!(wr.only_partition_result().unwrap().result(), Some(&want_wr));
}

fn pairs(v: &Value) -> Vec<(String, i64)> {
    v.as_array().unwrap().iter()
        .map(|p| (p[0].as_str().unwrap().to_string(), p[1].as_i64().unwrap()))
        .collect()
}
```

> Adjust public import names to the crate's actual re-exports: confirm
> `Consumed`, `I64Serde`, `StringSerde`, `StreamsBuilder`, `TopologyTestDriver`,
> `TimeWindows`, and the windowing DSL method names (`windowed_by`,
> `of_size_with_no_grace`) against `crates/client-streams/tests/iq_golden.rs`
> and the windowing tests; copy their exact spelling. If `TopologyTestDriver`
> is re-exported at the crate root use that path, else
> `crabka_client_streams::TopologyTestDriver`.

- [ ] **Step 3: Run the tests**

Run: `cargo test -p crabka-client-streams --test iqv2_golden`
Expected: PASS (both tests).

- [ ] **Step 4: Add to CI coverage**

In `.github/workflows/ci.yml`, find the `crabka-client-streams` llvm-cov invocation (the per-crate-integration job that lists `--test <name>` for each integration test, e.g. `--test iq_golden`). Add `--test iqv2_golden` to that list so the new test counts toward coverage (else codecov reports 0% for the patch).

- [ ] **Step 5: Commit**

```bash
git -C <worktree> add crates/client-streams/tests/iqv2_golden.rs \
  crates/client-streams/tests/testdata/iqv2/behavior.json .github/workflows/ci.yml
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "test(client-streams): IQv2 behavioral goldens (KeyQuery/RangeQuery/Window*Query)"
```

---

## Task 11: Full-suite reconciliation

**Files:** none (verification only) unless fixes are needed.

- [ ] **Step 1: Format check**

Run: `cargo fmt --check`
Expected: clean. If not, run `cargo fmt` and re-check (CI gates on `--check`).

- [ ] **Step 2: Workspace clippy (all targets)**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean. (Clippy's per-target cache can serve stale results; if a touched file looks suspect but passes, `touch` it and re-run, and check the real `$?` — not a piped exit. Watch `doc_markdown`: backtick bare identifiers like `iq2_execute`, `Iq2Query`, `windowStart` in module/doc comments, since workspace `pedantic` is warn-as-error.)

- [ ] **Step 3: Full crate test suite**

Run: `cargo test -p crabka-client-streams`
Expected: PASS — the whole suite is the gate (type-erasure mismatches are runtime downcasts, not compile errors).

- [ ] **Step 4: Workspace build**

Run: `cargo build --workspace`
Expected: success.

- [ ] **Step 5: Update memory + final commit (if any fixes)**

Update the `project-kip1071-streams` memory: IQv2 slice **3a** done (envelope + `KeyQuery`/`RangeQuery`/`WindowKeyQuery`/`WindowRangeQuery` + `Position`/`PositionBound`/`FailureReason` + per-partition `serve_iq2` on a second channel; v1 views untouched; window results are start-only). Remaining = slice **3b** (`VersionedKeyQuery` KIP-960 + `MultiVersionedKeyQuery` KIP-968 on the same envelope). Note the implied-serde / `Box<dyn Any>` downcast contract and that `iq2_execute` is the single store hook.

```bash
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -am "chore(client-streams): reconcile IQv2 slice 3a (fmt/clippy/test green)" || echo "nothing to commit"
```

---

## Self-Review notes (carried into execution)

- **Spec coverage:** §3 dispatch (A′) = Task 1 + the `iq2_execute` impls (Tasks 2/3) + `assemble` (Task 6); §4 per-partition channel = Tasks 6/8/9 (second channel, v1 untouched — verified: `runtime/iq.rs`, `iq_view.rs`, `answer_iq` are not in any task's file set); §5 Position/PositionBound/FailureReason = Tasks 4/5/8; §6 public API = Task 4; §7 store ops (window key-range new; KV/window key supported) = Tasks 2/3; §8 module layout = Tasks 4/6; §10 testing = Tasks 7/10. Versioned ops (§1 3b rows) intentionally deferred to slice 3b.
- **Type consistency:** `Iq2Query`/`Iq2Failure`/`iq2_execute` (store/iq.rs) used identically in Tasks 2/3/7/8. `StateQuery<Q>` (finalized request) vs `StateQueryRequest` (entry ZST) vs `StateQueryRequestBuilder` — consistent across Tasks 4/7/9. `Iq2Request`/`Iq2Outcome`/`assemble` (dispatch.rs) consistent across Tasks 6/8/9. `Position::dominates` (pub(crate)) used in Task 8. Result types per query (`Option<V>`, `Vec<(K,V)>`, `Vec<(i64,V)>`, `Vec<((K,i64),V)>`) match between `query.rs` (Task 4), the store impls (Tasks 2/3), and the goldens (Task 10).
- **Known empirical adjustments (not placeholders):** DSL builder spellings in Tasks 7/10 (`group_by_key`/`count`/`windowed_by`/`of_size_with_no_grace`) and the `make_test_task` helper in Task 5 must be matched to the crate's actual surface — each task says where to copy the exact spelling from (`iq_golden.rs`, existing runtime tests). `TaskRole` comparison (Task 8) adapts to its derive.
```
