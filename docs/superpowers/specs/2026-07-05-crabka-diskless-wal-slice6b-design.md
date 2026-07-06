# Diskless WAL — Slice 6b: leaderless serving + hot-tail cache — design

**Date:** 2026-07-05
**Status:** Approved
**Type:** Subsystem design (sub-slice of Slice 6, the capstone). Serve-side foundation — it lets any WAL-group member serve a correct diskless read; the write-side leaderless flip is 6c.

## Context — where this sits

Sub-slice of Slice 6 (see the [6a spec](2026-07-05-crabka-diskless-wal-slice6a-design.md) for the 6a→6c→6b→6d decomposition). 6a made a diskless partition's WAL a 2f+1 AZ quorum, so **every WAL-group member holds the committed tail in its local WAL-replica log** and the durable watermark is quorum-committed. 6b makes that useful for **reads**: any WAL-group member — not just the write-leader — can serve a byte-exact diskless fetch, gated on a correct high watermark sourced from the quorum.

**Scope decision (write path → 6c).** "Leaderless serving" here is the **read** side. The leaderless *write* path — flipping the produce leadership gate (`produce.rs:459-476`) to accept-and-sequence-on-any-broker — belongs to **6c** (the concurrent sequencer, which owns concurrent appenders + the offset authority the gate defers to). 6b builds the *ability* to serve from any member; the client-routing that sends fetches to non-leader members (KIP-392 fetch-from-follower or the 6c leaderless Metadata advertisement) is likewise **deferred**. This keeps 6b dependent on 6a alone and unblocked.

**Prerequisites (unlanded):** Slices 1–5 + 6a. 6b consumes 6a's per-member WAL-replica log and quorum-committed watermark.

## Design Goals

- **A correct HW on every WAL-group member:** drive `ReplicaState.hw` from the quorum-committed watermark on *every* member (today only the leader installs HW machinery, `replicator_supervisor.rs:353-364`), so a non-leader member exposes the same client-facing HW/ISR surface.
- **Any member serves a byte-exact diskless read** from its local WAL-replica log (the correctness floor), with the Slice-4 cold path for trimmed offsets unchanged.
- **A hot-tail latency fast-path:** an in-memory verbatim-batch cache over the local read, returned as `RecordsPayload::Raw`.
- **Change nothing on the write/ack path** (that's 6c) — 6b is read-side only.

### Non-goals (6b)

- **No leaderless write path / produce-gate flip** (6c).
- **No client routing to non-leader members.** 6b makes a member *able* to serve; the Metadata/KIP-392 routing that sends fetches there is 6c / a follow-up.
- **No re-composed gate / Jepsen** (6d).
- **No transactional read_committed on cold diskless reads** (diskless is non-transactional per Slice 4; `LSO = HW`).
- **No sendfile for cache/network-sourced bytes** — those take the `RecordsPayload::Raw` vectored-copy drain; sendfile stays for locally-materialized segments.

## Architecture Overview

```
WAL group (6a): 2f+1 members, each with a local WAL-replica Log holding [log_start, hw)
   quorum-committed watermark  ──►  ReplicaState.hw on EVERY member   [NEW: not just leader==self]
                                          (replicator_supervisor.rs:353-364 extended for diskless)

Fetch on ANY WAL-group member (handlers/fetch.rs):
   do_read → local WAL-replica Log read, gated at hw            ← AUTHORITATIVE (correctness floor)
      │  hot-tail latency path: in-memory verbatim-batch cache for [flushed_frontier, hw)
      │     hit → RecordsPayload::Raw (no sendfile); miss → the local Log read
      ▼
   OFFSET_OUT_OF_RANGE dispatch (fetch.rs:514-518):
      try_remote_read (tiered) | try_diskless_read (Slice 4 cold objects)   ← UNCHANGED
```

## Key Design Decisions

### HW from the quorum watermark on every member

Today HW/ISR machinery is installed only where `part_record.leader == self.node_id` (`replicator_supervisor.rs:353-364`); `install_leader_change` already runs on every broker (`:351`, idempotent). For a diskless partition, 6b drives `ReplicaState.hw` from the **quorum-committed watermark** (6a's `on_watermark_advance`) on *every* WAL-group member via `recompute_hw_for_wal_durable` (Slice 1) — re-using the same `ReplicaState` client-facing surface (`replica_state.rs`), the same `await_hw_at_least` gate, and firing `hw_advance_notify` on each member. A non-leader member thus reports the identical HW a leader would, so a fetch it serves has the correct visibility bound (`compute_visibility_window`, `fetch.rs:1022-1042`, `limit_offset = hw`). *This is the read-side analog of 6a's leader-side HW re-source; it is what makes any member's fetch correct.*

### Any member serves from its local WAL-replica log

Because 6a replicates (fsync) each batch to every member's local WAL-replica `Log`, a non-leader member's `do_read` reads the committed tail `[log_start, hw)` from its *own* log — the same `read_raw`/`read_raw_desc` path (`fetch.rs`), byte-exact. So leaderless *serving* needs no new read path for the hot tail: it needs the correct HW (above) and permission for a non-leader to answer. The Slice-4 cold path (`try_diskless_read`) for trimmed offsets is unchanged. *The single-leader assumption that "only the leader has the tail" is retired for diskless — the WAL quorum put the tail on every member.*

### The hot-tail cache is a latency path, not a correctness requirement

An optional per-`(topic,partition)` in-memory ring/`BTreeMap` of verbatim v2-batch bytes keyed by base offset, populated as the member observes the WAL commit stream. A fetch landing in `[flushed_frontier, hw)` that hits the cache returns `RecordsPayload::Raw` (owned bytes, **no sendfile** — heap bytes, not a pinned inode) without a disk read; a miss falls through to the authoritative local `do_read`. Wired as a fast-path check inside `do_read` (or a pre-dispatch sibling) — the correctness floor is always the local WAL-replica read, so a cold/empty cache never loses data. *The dual-representation "hot tail in memory" bet from the roadmap, scoped as a pure optimization.*

### Read-side only — routing deferred

6b makes a WAL-group member *capable* of serving a correct diskless fetch. It does **not** change which broker the client fetches from — that is either KIP-392 fetch-from-follower (advertising the diskless followers as fetchable replicas) or the 6c leaderless Metadata advertisement (leader becomes a wire fiction decoupled from where data lands). Deferring routing keeps 6b dependent on 6a only and avoids the write-path/Metadata churn (6c). *Until routing lands, 6b's serve capability is exercised by tests + fetch-from-follower; production leaderless reads switch on with 6c.*

## Integration

- **`crates/broker/src/replicator_supervisor.rs`** — for diskless partitions, install the quorum-watermark HW source on **every** WAL-group member (extend the `leader == self.node_id` gate at `:353-364`); the watermark is fed by 6a's `WalShardEngine::on_watermark_advance`.
- **`crates/broker/src/replica_state.rs`** — reuse `recompute_hw_for_wal_durable` (Slice 1) as the advance path on non-leader members; `compute_hw`/the ISR surface unchanged.
- **`crates/broker/src/handlers/fetch.rs`** — allow a non-leader WAL-group member to answer a diskless fetch (relax the leader-only serve assumption for diskless); add the hot-tail cache fast-path in `do_read`; cold path (`try_diskless_read`, Slice 4) unchanged.
- **`crates/broker/src/diskless/hot_tail.rs`** (new) — the in-memory verbatim-batch cache + the commit-stream observer that populates it.
- **Write/ack path, offset assignment, flush** — untouched.

## Kafka / KIP compliance

- **Fetch byte-exact.** Served bytes are unmodified verbatim v2 batches (local WAL-replica read or cache), gated at the correct HW.
- **HW/ISR surface preserved.** Every member exposes the same `ReplicaState`-derived HW; clients cannot tell which member served them.
- **Fetch-from-follower path.** A diskless follower serving reads is the KIP-392 shape (rack-aware follower fetch); the routing/advertisement is deferred (6c/follow-up), but the serve mechanics conform.

## Testing

- **Non-leader serves a correct read:** in a 3-member WAL group, drive an `acks=all` produce to quorum-commit; a fetch answered by a **non-leader** member returns the same bytes and the same HW-bounded window as the leader would; a fetch beyond HW returns empty (not stale data).
- **HW on every member:** after a quorum commit, every member's `ReplicaState.hw` equals the quorum-committed watermark (not zero/empty on non-leaders, the pre-6b bug).
- **Hot-tail cache byte-exact:** a fetch in `[flushed_frontier, hw)` that hits the cache returns bytes identical to the local WAL-replica read; a cold cache falls through and still returns correct bytes (the correctness floor).
- **Cold path unaffected:** trimmed offsets still route via `try_diskless_read` (Slice 4); the hot-tail path only serves `[flushed_frontier, hw)`.
- **Ack/write path untouched:** produce/ack behavior is unchanged (that path is 6c); 6b edits are read-side only.

## Risks (carried into the plan)

- **HW coherence across members:** a member whose observed watermark lags the true quorum commit would under-report HW (safe — serves less) but must never over-report (would serve un-committed data). The advance source must be the *committed* watermark, never a local optimistic one.
- **Cache coherence:** a stale cache entry (e.g. after a truncation) must never be served; key by base offset + validate against the local log's `end_offset`/epoch, and treat the cache as advisory (miss → authoritative read).
- **Serving before routing exists:** 6b's capability isn't client-visible until 6c/KIP-392 routes fetches to non-leaders — so its value is latent until then; the tests exercise it directly.
- **read_committed / LSO:** diskless is non-transactional (`LSO = HW`); a non-leader must report `LSO = HW`, not a stale local LSO.

## Resolved decisions (from brainstorming)

- **Scope:** leaderless *serving* (reads) only; the write-path leaderless flip + routing → 6c.
- **Model:** each WAL-group member's local WAL-replica log is the log Fetch reads (every member holds the tail); serve from it + the quorum HW.
- **HW:** drive `ReplicaState.hw` from the quorum watermark on every member (extend the leader-only install).
- **Hot-tail cache:** a latency fast-path (in-memory verbatim bytes → `RecordsPayload::Raw`), correctness floor = local WAL-replica read.
- **Deferred:** leaderless writes/produce-gate flip (6c), client routing to non-leaders (6c/KIP-392), re-composed gate + Jepsen (6d), sendfile for cache/network bytes.
