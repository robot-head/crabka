# Per-offset explicit Subscribe ack (MSG-3) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `SubscribeAck.{topic,partition,offset}` load-bearing: an ack for the record at offset `X` commits `X+1` for that partition, gated on a per-partition contiguous-ack frontier (gap-safe), via a net-new client-consumer explicit-offset commit — with no broker change.

**Architecture:** A per-`(topic,partition)` `PartitionAckState` (frontier + out-of-order `pending` + `last_committed_frontier`) lives in the gateway `ConsumeSession`. Acks (client + filtered-record auto-acks) are buffered in the stream loop and replayed through `record_ack` after the poll borrow releases; `commit_acked` commits `frontier+1` for advanced, still-owned partitions via `Consumer::commit_offsets_sync`. All frontier machinery runs only under `auto_commit == false`.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `std::collections::{BTreeSet,HashMap}`, `tokio`, Connect-RPC, the in-process `Broker::start` harness, `assert2`, `cargo +nightly fmt`, `clippy::pedantic`.

**Spec:** [`docs/superpowers/specs/2026-07-06-crabka-peroffset-subscribe-ack-design.md`](../specs/2026-07-06-crabka-peroffset-subscribe-ack-design.md).

**PREREQUISITE:** none unlanded — the broker already accepts explicit `OffsetCommit` (`offset_commit.rs:327`). Independent of MSG-1/2/4.

---

## Invariants

1. **Commit = `frontier + 1`** — Kafka next-to-consume; ack(record@X) → commit X+1.
2. **Gap-safe** — an out-of-order ack above a gap goes to `pending`, never advances the frontier past an unacked offset.
3. **Bounded** — `pending` per partition is capped at `MAX_PENDING_PER_PARTITION`; overflow fails the stream fast (no silent unbounded growth).
4. **Lazy seed** — frontier seeds from the first *delivered-and-acked* offset, never a resume offset.
5. **Explicit-mode only** — all frontier machinery gated on `auto_commit == false`; `auto_commit == true` keeps today's whole-position commit unchanged.
6. **No committed-offset regression** — `commit_acked` commits only currently-owned partitions.
7. **No broker change.** Every task ends green before its commit.

## Scope boundary

- **In scope:** `PartitionAckState`/`record_ack`/cap/`acked_offsets`/`commit_acked`; `commit_offsets_sync` + `assigned_partitions`; the stream wiring + auto/explicit gating; the proto comment.
- **Deferred:** broker-side redelivery/`delivery_count`/lock expiry (share groups); true per-partition backpressure; async per-offset commit; commit metadata.

---

## File Structure & Batching

- **`crates/grpc-gateway/src/consume.rs`** — `PartitionAckState`, `ack_tracker`, `record_ack`, `acked_offsets`, `commit_acked` (Tasks 1, 3).
- **`crates/client-consumer/src/commit.rs`** + **`consumer.rs`** — `commit_offsets_sync`, extracted `commit_topics`, `assigned_partitions` (Task 2).
- **`crates/grpc-gateway/src/streaming.rs`** — the Ack-frame + filtered-record wiring (Task 4).
- **`crates/grpc-gateway/proto/.../gateway.proto`** — the `SubscribeAck` comment (Task 5).

**Batching:** Task 1 (`consume.rs`) and Task 2 (`client-consumer`) touch disjoint files → **concurrent**. Task 3 (`consume.rs`) depends on both. Task 4 (`streaming.rs`) depends on 3. Tasks 5 (proto) is parallel-safe.

---

## Task 1 (Batch A): `PartitionAckState` + `record_ack` + pending cap

**Files:**
- Modify: `crates/grpc-gateway/src/consume.rs`

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod ack_tests {
    use super::*;
    use assert2::{assert, let_assert};

    fn st() -> PartitionAckState { PartitionAckState::default() }

    #[test]
    fn first_ack_lazily_seeds_frontier() {
        let mut s = st();
        s.record(5).unwrap();
        assert!(s.commit_value() == Some(6)); // frontier=5 -> commit 6
    }

    #[test]
    fn lazy_seed_on_gappy_start_does_not_stall() {
        // First delivered offset 100 though the notional resume was 42.
        let mut s = st();
        s.record(100).unwrap();
        assert!(s.commit_value() == Some(101)); // NOT a stall at 43
    }

    #[test]
    fn in_order_acks_advance() {
        let mut s = st();
        for o in 10..=13 { s.record(o).unwrap(); }
        assert!(s.commit_value() == Some(14));
        assert!(s.pending.is_empty());
    }

    #[test]
    fn out_of_order_ack_above_gap_does_not_advance() {
        let mut s = st();
        s.record(10).unwrap();       // frontier=10
        s.record(12).unwrap();       // gap at 11 -> pending {12}
        assert!(s.commit_value() == Some(11)); // stays at frontier+1=11
        assert!(s.pending.contains(&12));
    }

    #[test]
    fn filling_the_gap_coalesces_in_one_drain() {
        let mut s = st();
        s.record(10).unwrap();
        s.record(12).unwrap();
        s.record(13).unwrap();
        s.record(11).unwrap();       // fills gap -> drains 11,12,13
        assert!(s.commit_value() == Some(14));
        assert!(s.pending.is_empty());
    }

    #[test]
    fn below_frontier_ack_is_idempotent() {
        let mut s = st();
        s.record(10).unwrap();
        s.record(10).unwrap();
        s.record(3).unwrap();
        assert!(s.commit_value() == Some(11));
    }

    #[test]
    fn unchanged_frontier_not_recommitted() {
        let mut s = st();
        s.record(10).unwrap();
        s.last_committed_frontier = Some(10);
        assert!(s.commit_value() == None); // no advance since last commit
    }

    #[test]
    fn pending_cap_overflows() {
        let mut s = st();
        s.record(0).unwrap(); // frontier=0
        // ack a long ascending tail leaving offset 1 forever unacked
        for o in 2..(2 + MAX_PENDING_PER_PARTITION as i64) { s.record(o).unwrap(); }
        let_assert!(Err(AckOverflow) = s.record(2 + MAX_PENDING_PER_PARTITION as i64));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-grpc-gateway --lib consume::ack_tests`
Expected: FAIL — `PartitionAckState`/`record`/`AckOverflow`/`MAX_PENDING_PER_PARTITION` undefined.

- [ ] **Step 3: Implement (pure, no I/O)**

Add to `consume.rs` (imports: `std::collections::BTreeSet`):

```rust
/// Cap on buffered out-of-order acks per partition. There is no per-partition
/// delivery-pause seam (Consumer::poll advances all partitions together), so a
/// permanently-withheld low ack would otherwise grow `pending` without bound;
/// on overflow the stream fails fast (see streaming.rs).
pub(crate) const MAX_PENDING_PER_PARTITION: usize = 100_000;

/// Recording an ack would exceed the pending cap.
#[derive(Debug)]
pub(crate) struct AckOverflow;

/// Per-(topic,partition) contiguous-ack frontier. Commit value = `frontier + 1`.
#[derive(Debug, Default)]
pub(crate) struct PartitionAckState {
    /// Highest offset X with every offset below it acked; `None` until first ack.
    frontier: Option<i64>,
    /// Out-of-order acks strictly above `frontier + 1`, coalesced as gaps fill.
    pending: BTreeSet<i64>,
    /// Highest frontier already committed; skip re-committing an unchanged frontier.
    last_committed_frontier: Option<i64>,
}

impl PartitionAckState {
    fn record(&mut self, offset: i64) -> Result<(), AckOverflow> {
        match self.frontier {
            None => self.frontier = Some(offset), // lazy seed (first delivered offset)
            Some(f) if offset <= f => {}          // duplicate / reordered low ack: idempotent
            Some(f) if offset == f + 1 => { self.frontier = Some(offset); self.drain(); }
            Some(_) => {
                if !self.pending.contains(&offset) && self.pending.len() >= MAX_PENDING_PER_PARTITION {
                    return Err(AckOverflow);
                }
                self.pending.insert(offset);
            }
        }
        Ok(())
    }

    fn drain(&mut self) {
        while let Some(f) = self.frontier {
            if self.pending.remove(&(f + 1)) { self.frontier = Some(f + 1); } else { break; }
        }
    }

    /// The next-to-consume commit value, if the frontier advanced since last commit.
    fn commit_value(&self) -> Option<i64> {
        match self.frontier {
            Some(f) if self.last_committed_frontier != Some(f) => Some(f + 1),
            _ => None,
        }
    }
}
```

- [ ] **Step 4: Run to verify it passes; commit**

Run: `cargo test -p crabka-grpc-gateway --lib consume::ack_tests` → PASS.

```bash
git add crates/grpc-gateway/src/consume.rs
git commit -m "feat(gateway): contiguous-ack frontier (PartitionAckState) with pending cap"
```

---

## Task 2 (Batch A): Client-consumer explicit-offset commit + assignment accessor

**Files:**
- Modify: `crates/client-consumer/src/commit.rs`, `crates/client-consumer/src/consumer.rs`

- [ ] **Step 1: Write the failing tests**

Add a shaping test (mirror the existing `snapshot_commit_topics` test at `commit.rs`): an explicit offset map `{(t,0): 42}` with a known `positions` epoch produces an `OffsetCommitRequestTopic` with `committed_offset = 42` and the expected `committed_leader_epoch`. Add an `assigned_partitions` test asserting it reflects `self.assigned`.

- [ ] **Step 2: Run to verify it fails; implement**

In `commit.rs`, extract the send/interpret tail of `commit_sync` (`:135-190`) into `async fn commit_topics(&self, partitions: usize, topics: Vec<OffsetCommitRequestTopic>) -> Result<(), ConsumerError>`; `commit_sync` becomes `snapshot_commit_topics(...) → commit_topics(...)`. Add:

```rust
/// Commit explicit per-partition offsets (each the *next* offset to consume,
/// i.e. last-processed + 1) instead of the current fetch position. Blocks until
/// the broker acks. Used by the gateway's per-offset ack path.
///
/// # Errors
/// Broker/coordinator error, or a non-deferrable OffsetCommit error code.
pub async fn commit_offsets_sync(
    &self,
    offsets: HashMap<(String, i32), i64>,
) -> Result<(), ConsumerError> {
    if offsets.is_empty() { return Ok(()); }
    let partitions = offsets.len();
    let with_epoch = {
        let positions = self.positions.lock().await;      // KIP-320 epochs; no next_offsets read
        commit_offsets(offsets, &positions)               // reuse commit.rs:42
    };
    let topics = build_commit_topics(with_epoch, &self.topic_ids().await); // reuse offset_wire.rs:111
    self.commit_topics(partitions, topics).await
}
```

In `consumer.rs`, expose the existing `self.assigned` (`:63`):

```rust
/// The partitions currently assigned to this member (for the gateway's
/// owned-partition commit filter).
pub async fn assigned_partitions(&self) -> Vec<(String, i32)> {
    self.assigned.lock().await.clone()
}
```

(Match the exact `topic_ids`/`positions` accessors used by `commit_sync`; if `commit_sync` reads them differently, mirror that.)

- [ ] **Step 3: Run to verify it passes; commit**

Run: `cargo test -p crabka-client-consumer commit` → PASS.

```bash
git add crates/client-consumer/src/commit.rs crates/client-consumer/src/consumer.rs
git commit -m "feat(client-consumer): commit_offsets_sync + assigned_partitions accessor"
```

---

## Task 3: `acked_offsets` + `commit_acked` (ownership-filtered)

**Files:**
- Modify: `crates/grpc-gateway/src/consume.rs:95-102`

Depends on Tasks 1 + 2.

- [ ] **Step 1: Write the failing test**

Unit-test `acked_offsets` (pure): record acks producing frontiers on two partitions, assert the map is `{(t,0): f0+1, (t,1): f1+1}` for advanced partitions only; a partition whose frontier equals `last_committed_frontier` is absent.

- [ ] **Step 2: Run to verify it fails; implement**

Add the `ack_tracker` field to `ConsumeSession` (`HashMap<(String,i32), PartitionAckState>`, default empty) and:

```rust
/// Record a delivered-record ack (client Ack or filtered-record auto-ack).
/// Pure; `partition<0`/`offset<0` are tolerated (ignored).
///
/// # Errors
/// [`GatewayError`] when the per-partition pending cap is exceeded (caller
/// terminates the stream).
pub fn record_ack(&mut self, topic: &str, partition: i32, offset: i64) -> Result<(), GatewayError> {
    if partition < 0 || offset < 0 { return Ok(()); }
    self.ack_tracker
        .entry((topic.to_string(), partition))
        .or_default()
        .record(offset)
        .map_err(|_| GatewayError::too_many_unacked(topic, partition, offset))
}

fn acked_offsets(&self) -> std::collections::HashMap<(String, i32), i64> {
    self.ack_tracker
        .iter()
        .filter_map(|(k, st)| st.commit_value().map(|v| (k.clone(), v)))
        .collect()
}
```

Rename `commit` → `commit_acked` (still `async`, now `&mut self`):

```rust
/// Commit `frontier+1` for every advanced, still-owned partition. Drops
/// ack-tracker entries for partitions no longer assigned (their tail simply
/// re-delivers to the new owner — at-least-once), avoiding a committed-offset
/// regression the broker would otherwise accept.
pub async fn commit_acked(&mut self) -> Result<(), GatewayError> {
    let consumer = self.consumer.as_ref().expect("committed after close");
    let owned: std::collections::HashSet<(String, i32)> =
        consumer.assigned_partitions().await.into_iter().collect();
    self.ack_tracker.retain(|k, _| owned.contains(k));
    let map = self.acked_offsets();
    if map.is_empty() { return Ok(()); }
    consumer.commit_offsets_sync(map.clone()).await?;
    for (k, next) in &map {
        if let Some(st) = self.ack_tracker.get_mut(k) {
            st.last_committed_frontier = Some(next - 1);
        }
    }
    Ok(())
}
```

Add `GatewayError::too_many_unacked` (a `resource_exhausted`-mapped variant).

- [ ] **Step 3: Run to verify it passes; commit**

Run: `cargo test -p crabka-grpc-gateway --lib consume::` → PASS.

```bash
git add crates/grpc-gateway/src/consume.rs
git commit -m "feat(gateway): commit_acked (ownership-filtered per-offset commit)"
```

---

## Task 4: Stream wiring — bind ack, buffer filtered records, replay after select

**Files:**
- Modify: `crates/grpc-gateway/src/streaming.rs:289,298-330`
- Test: `crates/grpc-gateway/tests/streaming.rs` (extend)

Depends on Task 3.

- [ ] **Step 1: Write the failing integration test**

Boot a broker + gateway; subscribe with `auto_commit=false` and a predicate filter; deliver records including filtered ones; ack out of order (leave a gap); drop the stream; resubscribe and assert redelivery **starts at `frontier+1` and not past the gap**, and that a run of filtered offsets does **not** stall the frontier. Second test: `auto_commit=true` ignores explicit acks and does not auto-ack filtered records (whole-position commit unchanged).

- [ ] **Step 2: Run to verify it fails; implement**

In `subscribe_inner`, per loop iteration keep `let mut filtered_acks: Vec<(String,i32,i64)> = Vec::new();` and `let mut client_ack: Option<(String,i32,i64)> = None;`. Change the Ack arm (`:301`):

```rust
Some(Ok(pb::SubscribeFrame { frame: Some(pb::subscribe_frame::Frame::Ack(ack)) })) => {
    client_ack = Some((ack.topic, ack.partition, ack.offset));
    commit = true;
}
```

In the poll arm, where a predicate-filtered record hits `continue` (`:311`), first push its offset (the poll borrow permits pushing to a local Vec, not calling `record_ack`):

```rust
if !structured_json_matches(/* … */) {
    if !auto_commit { filtered_acks.push((r.topic.clone(), r.partition.0, r.offset.0)); }
    continue;
}
```

After the select resolves (where `to_emit` is drained + `commit` runs, `:322-330`), in explicit mode replay the buffered acks, then commit:

```rust
if !auto_commit {
    for (t, p, off) in filtered_acks.drain(..) {
        if let Err(e) = session.record_ack(&t, p, off) {
            yield Err(ConnectError::new(Code::ResourceExhausted, e.to_string())); return;
        }
    }
    if let Some((t, p, off)) = client_ack.take() {
        if let Err(e) = session.record_ack(&t, p, off) {
            yield Err(ConnectError::new(Code::ResourceExhausted, e.to_string())); return;
        }
    }
}
if commit {
    let res = if auto_commit { session.commit().await } else { session.commit_acked().await };
    if let Err(e) = res { yield Err(ConnectError::new_internal(e.to_string())); break; }
}
```

(Keep the record yields at `:322-324` **before** this commit block — at-least-once ordering. `session.commit()` for the auto-commit path remains the whole-position `commit_sync` wrapper; rename the old `commit` to keep it, or add a thin `commit_position` alias — do not delete the whole-position path.)

- [ ] **Step 3: Run to verify it passes; commit**

Run: `cargo test -p crabka-grpc-gateway --test streaming` → PASS.

```bash
git add crates/grpc-gateway/src/streaming.rs crates/grpc-gateway/tests/streaming.rs
git commit -m "feat(gateway): per-offset ack wiring in the Subscribe stream (gap-safe, filtered auto-ack)"
```

---

## Task 5: Proto comment — fields are load-bearing

**Files:**
- Modify: `crates/grpc-gateway/proto/crabka/gateway/v1/gateway.proto:108-115`

- [ ] **Step 1:** Replace the "advisory / per-offset commit is a follow-up" comment with: the `offset` is the record offset being acked; the gateway commits `offset+1` for `(topic,partition)` gated on a contiguous frontier; the fields are load-bearing only when `auto_commit=false` and are ignored under `auto_commit=true`. No field changes.
- [ ] **Step 2:** `cargo build -p crabka-grpc-gateway` (regenerates pb, no code change). Commit.

```bash
git add crates/grpc-gateway/proto/crabka/gateway/v1/gateway.proto
git commit -m "docs(gateway): document SubscribeAck fields as load-bearing (explicit mode)"
```

---

## Task 6: Final gate

- [ ] **Step 1:** `cargo +nightly fmt --check` — no diff.
- [ ] **Step 2:** `cargo clippy -p crabka-grpc-gateway -p crabka-client-consumer --all-targets -- -D warnings` — no warnings.
- [ ] **Step 3:** `cargo nextest run -p crabka-grpc-gateway -p crabka-client-consumer` — PASS, incl. the frontier unit tests (seed/gap/drain/cap), `commit_offsets_sync` shaping, and the end-to-end gap-safety + filtered-auto-ack + rebalance-ownership integration tests.
- [ ] **Step 4:** Commit any formatting.

---

## Self-Review

**1. Spec coverage:** commit X+1 (Task 1 `commit_value`); contiguous frontier + lazy seed + drain (Task 1); mandatory pending cap (Task 1); `commit_offsets_sync` + `assigned_partitions` (Task 2); `acked_offsets` + ownership-filtered `commit_acked` (Task 3); stream wiring + filtered auto-ack (borrow-safe replay) + fail-fast on overflow + auto/explicit gating (Task 4); proto comment (Task 5). Deferred set (broker redelivery, backpressure, async commit, metadata) untouched — Scope boundary. ✅

**2. Placeholder scan:** Tasks 1–3 are complete code against named seams (`consume.rs:95-102`, `commit.rs:42/111/135-190`, `consumer.rs:63`); Task 4 gives the exact arm rewrites at `streaming.rs:301/311/322-330` with the borrow-safe replay. The two resolved decisions (cap = fail-fast; ownership filter via `assigned_partitions`) are implemented, not left open. No `TBD`.

**3. Type consistency:** `PartitionAckState` (Task 1) is stored in `ConsumeSession.ack_tracker` and read by `acked_offsets`/`commit_acked` (Task 3); `record_ack(&mut self,…) -> Result<(),GatewayError>` (Task 3) is called from the stream replay (Task 4); `commit_offsets_sync(HashMap<(String,i32),i64>)` (Task 2) consumes exactly `acked_offsets()`'s output (Task 3); `assigned_partitions() -> Vec<(String,i32)>` (Task 2) feeds the ownership filter (Task 3).

**4. Invariant check:** commit=frontier+1 (Task 1); gap-safe (Task 1 out-of-order test + Task 4 e2e); bounded via mandatory cap → fail-fast (Task 1 + Task 4); lazy seed (Task 1 gappy-start test); explicit-mode gating (Task 4); no regression via ownership filter (Task 3 + Task 4 rebalance test); broker unchanged. Each task green before commit.

**5. Prerequisites:** none — broker `OffsetCommit` already persists explicit offsets (`offset_commit.rs:327`). Batching: Task 1 (`consume.rs`) ∥ Task 2 (`client-consumer`) → Task 3 (`consume.rs`) → Task 4 (`streaming.rs`); Task 5 (proto) parallel-safe.
