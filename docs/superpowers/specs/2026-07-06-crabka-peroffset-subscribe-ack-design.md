# Per-offset explicit Subscribe ack (MSG-3) — design

**Date:** 2026-07-06
**Status:** Approved
**Type:** Subsystem design. The **lighter** ack path of the [serverless messaging cycle](2026-07-06-crabka-gateway-header-carrythrough-design.md) — makes `SubscribeAck`'s advisory `(topic, partition, offset)` load-bearing so a gateway client commits the specific offset it acked, gap-safely.

## Context — the lighter ack variant (positioned honestly)

Today the gateway `Subscribe` stream treats an `Ack` frame as "commit the consumer's whole current position for all assigned partitions" — the `SubscribeAck.{topic,partition,offset}` payload is **ignored** (a wildcard match at `streaming.rs:301`, with a proto comment calling the fields "advisory … per-offset commit is a follow-up"). MSG-3 makes those fields load-bearing: an ack for the record at offset `X` commits `X+1` for that `(topic, partition)`, gated on a **contiguous-ack frontier** so an out-of-order ack never commits past an unacked gap.

**This is deliberately the lighter of two ack models Crabka already has.** KIP-932 **share groups are fully built and JVM-validated** (`share_partition/state.rs` — a per-offset broker-side `Available→Acquired→Acknowledged→Archived` machine with `delivery_count`, lock expiry, automatic redelivery, poison-pill archiving, and Accept/Release/Reject/Gap). A genuine serverless *function-per-message* backend — one that can crash mid-invocation and needs server-managed redelivery — should usually prefer **share groups**. MSG-3's honest niche is a *transient, ordered, at-least-once* client that owns its own retry/dead-letter logic and wants the gateway to be a thin ack hook over the consumer-group-offset model. **Redelivery is the caller's problem:** an unacked record is only re-delivered by re-consuming from the committed offset on rebalance/restart; the gateway does not time out or re-push individual in-flight records. Do not sell MSG-3 as the recommended serverless queue — that is the share-group story.

## Design Goals

- **Load-bearing ack:** `ack(record @ X)` → commit `X+1` for `(topic, partition)` (Kafka's committed offset is the *next* offset to consume).
- **Gap-safe:** a per-`(topic, partition)` **contiguous-ack frontier** — commit only the offset below which *every* offset is acked; an out-of-order ack above a gap is buffered, never committed past.
- **No broker change:** the broker already accepts an explicit per-partition `OffsetCommit` and persists it verbatim (`offset_commit.rs:327-332`); MSG-3 is gateway + client-consumer plumbing.
- **Bounded memory, honestly:** a mandatory per-partition cap on buffered out-of-order acks — fail-fast when exceeded (no per-partition backpressure seam exists in v1).
- **Explicit-mode only:** the frontier machinery runs only under `auto_commit == false`; `auto_commit == true` keeps today's whole-position behavior unchanged.

## Non-goals

- **Broker-side per-message ack state / redelivery / `delivery_count` / lock expiry** — that is KIP-932 share groups, already built; MSG-3 does not replicate it.
- **True per-partition backpressure** (delivery-pause on a stalled partition) — `Consumer::poll` advances all partitions together (`poll.rs:568`); v1 fails the stream fast instead. Deferred.
- **Async per-offset commit**, **commit metadata**, and **mixing** explicit acks with `auto_commit=true` (acks are ignored under auto-commit).
- MSG-4 (scaler), MSG-2 (CloudEvents), MSG-5 (SDK).

## Architecture Overview

```
Subscribe(auto_commit = false)                        [gateway, streaming.rs]
  per stream, gateway ConsumeSession holds:
    ack_tracker: HashMap<(topic,partition), PartitionAckState>
      PartitionAckState { frontier: Option<i64>, pending: BTreeSet<i64>, last_committed_frontier: Option<i64> }

  poll arm (holds &mut session borrow):
    deliver matching records; predicate-FILTERED records → buffer (t,p,offset) into `filtered_acks`
  frames arm:
    client Ack(t,p,offset) → buffer into `client_ack`
  AFTER the select resolves (borrow released), in explicit mode:
    for each buffered ack: session.record_ack(t,p,X)          // lazy-seed, drain, gap-safe; cap-checked
    session.commit_acked()  →  { (t,p): frontier+1 for advanced, owned partitions }
                            →  consumer.commit_offsets_sync(map)   // NET-NEW explicit-offset commit
  → broker OffsetCommit persists frontier+1 verbatim (unchanged)
```

## Key Design Decisions

### Commit `X+1` (Kafka next-to-consume), confirmed against the resume path

The ack subject `X` is the raw record offset the gateway emits (`Inbound.offset = record.offset`, `streaming.rs:150`). Kafka's committed group offset is the *next* offset to consume: the consumer's `next_offsets` already holds `last+1` (`poll.rs:604`), the broker persists the committed value verbatim (`offset_commit.rs:327`), and `OffsetFetch` returns it verbatim for resume (`offset_fetch.rs:228`). So committing `X` (instead of `X+1`) would redeliver record `X` on restart. The frontier stores the highest contiguously-acked offset and commits `frontier + 1`.

### Contiguous-ack frontier in the gateway `ConsumeSession`

Per `(topic, partition)`: `frontier` (highest offset with every offset below it acked), `pending` (a `BTreeSet` of out-of-order acks above a gap), `last_committed_frontier` (skip re-committing an unchanged frontier). On `ack(X)`:
- `frontier` is `None` → **lazy seed** `frontier = X`, then drain.
- `X ≤ frontier` → idempotent (duplicate/reordered low ack).
- `X == frontier + 1` → advance, then drain.
- `X > frontier + 1` → `pending.insert(X)` — **do not advance** (the gap-safety guarantee), subject to the cap.
- **Drain:** while `pending` contains `frontier + 1`, remove it and bump `frontier`.

Commit = `frontier + 1`. Acking 1000 while 999 is unacked leaves `frontier` at 998 and commit at 999 — a crash re-delivers 999+1000 (at-least-once), never skips 999. `BTreeSet`, not a map: MSG-3 has exactly one ack semantic (accept); Release/Reject/Gap is share groups.

### Lazy frontier seed (not `resume_offset − 1`)

The frontier is seeded from the **first offset actually acked/auto-acked** for a partition — never from the resume/committed offset. On a compacted or gappy log the first *delivered* offset can be strictly greater than the committed resume offset; a `resume_offset − 1` seed would treat the never-delivered `[resume … first_delivered−1]` range as a permanent gap and stall the frontier forever. Because the gateway only ever acks offsets it actually delivered, the lazy seed is always a real delivered offset and never waits on an offset that will not arrive. (This also removes any need for a consumer position accessor.)

### Mandatory pending cap — the honest memory bound

There is **no per-partition delivery-pause seam**: the gateway polls all partitions unconditionally (`streaming.rs:307`) and `Consumer::poll` advances every partition's `next_offsets` together (`poll.rs:568`). A client that withholds one low ack while acking the tail grows `pending` to the *entire consumed tail above the gap* — `O(records since the gap)`, not `O(fetch window)`. So `MAX_PENDING_PER_PARTITION` (a fixed constant, e.g. `100_000`) is the **only real invariant**: the ack that would exceed it terminates the stream with a `resource_exhausted` error at `streaming.rs:328` ("too many outstanding un-acked records; the ack for offset N was never received"). This is fail-fast, **not** true backpressure — the earlier design's "bounded by the in-flight window" claim is withdrawn as unenforceable. In steady state with in-order acks, `pending` is empty and each ack is `O(log n)` insert + `O(1)` drain.

### Filtered-record auto-ack (borrow-safe, post-select replay)

Predicate-filtered records (`streaming.rs:311` `continue`) are delivered-then-dropped — never client-acked — so their offsets would be a **permanent gap** stalling the frontier. They must be auto-acked. But `record_ack(&mut self)` cannot run inside the poll arm, which holds the `&mut session` borrow for its whole body (`streaming.rs:292-294`). Fix, mirroring how `commit` is deferred: buffer filtered offsets into a local `filtered_acks: Vec<(String,i32,i64)>` in the poll arm, and replay them (plus the buffered client ack) through `record_ack` **after** the select resolves, at the same point `commit` runs. This yields exactly **one** `record_ack` call site and one cap-check site.

### Net-new client-consumer explicit-offset commit

Neither `commit_sync` nor `commit_async` takes an explicit offset — both read `self.next_offsets`. Add `Consumer::commit_offsets_sync(offsets: HashMap<(String,i32), i64>)` that shapes the passed offsets instead. It reuses the existing pipeline verbatim: `commit_offsets(offsets, &positions)` for KIP-320 `committed_leader_epoch` (`commit.rs:42`), `build_commit_topics` (`offset_wire.rs:111`), the `with_coordinator_refind` wrapper with a live-loaded generation (`commit.rs:159`), and `commit_response_result` for rebalance-code deferral (`commit.rs:102`). Refactor the send/interpret tail of `commit_sync` (`commit.rs:135-190`) into a shared private `commit_topics(partitions, topics)` so both methods single-source the coordinator/rebalance logic. `_sync` (not async) so the gateway can surface the error on the stream.

### Rebalance ownership filter (required)

The broker validates only `member_id`/generation on `OffsetCommit`, **not partition ownership** (`offset_commit.rs:302-310`), and persists verbatim. So a stale `ack_tracker` commit for a partition revoked-but-same-generation would be accepted and could **regress the new owner's** committed offset. Mitigation (in the gateway, not the broker): `commit_acked` filters to partitions still owned by the underlying `Consumer`, via a new `Consumer::assigned_partitions()` reading the existing `self.assigned` (`consumer.rs:63`); `ack_tracker` entries for no-longer-owned partitions are dropped. This is a required correctness step, verified by a rebalance integration test.

### Auto vs explicit — mutually exclusive

`auto_commit == true` keeps today's whole-position commit at enqueue (`streaming.rs:316`); explicit acks and filtered auto-acks are **ignored** and the `ack_tracker` is not built. `auto_commit == false` is the MSG-3 path: polling never auto-commits (already gated at `:316`), and only recorded acks advance the frontier. All frontier machinery is gated on `!auto_commit`.

## Integration

- **`gateway.proto:108-115`** — keep `SubscribeAck` fields; rewrite the "advisory" comment to load-bearing (explicit mode).
- **`grpc-gateway/src/consume.rs`** — `ack_tracker` + `record_ack` (pure, cap-aware) + `acked_offsets` + rename `commit` → `commit_acked` (ownership-filtered).
- **`grpc-gateway/src/streaming.rs:289,298-330`** — bind the ack, buffer filtered offsets, post-select replay + cap check + `commit_acked`; all gated on `!auto_commit`.
- **`client-consumer/src/commit.rs`** — `commit_offsets_sync` + the extracted `commit_topics` tail; reuse `commit_offsets`/`build_commit_topics`/`commit_response_result`.
- **`client-consumer/src/consumer.rs:63`** — `assigned_partitions()` accessor.
- **broker** — **no change** (`offset_commit.rs` already persists explicit offsets).

## Kafka / wire compliance

- **Broker wire unchanged** — the gateway sends a standard explicit `OffsetCommit`; `frontier+1` is the ordinary next-to-consume value.
- **The ownership gap is a broker limitation worked around in the gateway** — `OffsetCommit` not validating partition ownership matches Kafka's own leniency; the gateway's owned-partition filter avoids regressing a reassigned partition.

## Testing

- **Frontier unit tests** (pure, no broker): first ack lazily seeds; in-order acks advance; out-of-order ack above a gap → `pending`, no advance; filling the gap coalesces in one drain; below-frontier ack idempotent; `partition<0`/`offset<0` ignored; unseen partition inserts-and-seeds; **lazy seed on a gappy start** (first delivered offset 100 while resume is 42 → `ack(100)` commits 101, no stall); `commit_value == frontier+1`.
- **Pending cap:** withholding one low ack while acking a long tail grows `pending` to the cap, then the next out-of-order ack reports overflow; in-order acks never trip it.
- **`commit_offsets_sync` shaping:** an explicit offset map with a known positions epoch produces the expected `OffsetCommitRequestTopic` (mirrors the existing `snapshot_commit_topics` test).
- **End-to-end gap-safety + filtered auto-ack:** deliver records including predicate-filtered ones; ack out of order; kill the stream; resubscribe; assert redelivery starts at `frontier+1` and **not** past the gap, and that a run of filtered offsets does not stall the frontier.
- **Auto vs explicit:** `auto_commit=true` ignores explicit acks, does not auto-ack filtered records, still whole-position commits; `auto_commit=false` commits only contiguously-acked offsets.
- **Rebalance ownership:** a commit for a revoked partition is filtered out (no regression of the new owner's offset).

## Risks (carried into the plan)

- **Rebalance ownership** — mitigated by the owned-partition filter (`assigned_partitions()` + drop revoked entries); requires the rebalance integration test to confirm no committed-offset regression.
- **Fail-fast vs backpressure** — v1 terminates the stream on pending-cap overflow rather than pausing delivery (no per-partition pause seam); true backpressure is deferred and would need client-consumer changes.
- **No redelivery of in-flight records** — caller's responsibility; a stalled/withheld ack coarsens redelivery to "everything from the frontier." Share groups (with lock expiry + redelivery) tolerate exactly the adversarial ack patterns that make MSG-3 fail-fast — the honest reason to prefer them for a crash-prone function backend.

## Resolved decisions (from grounding)

- **Semantics:** `ack(X)` → commit `X+1`; frontier stores highest contiguously-acked, commits `frontier+1`.
- **Seed:** lazy (first acked offset); drop the gap-unsafe `resume_offset−1` variant.
- **Memory:** mandatory `MAX_PENDING_PER_PARTITION`, fail-fast on overflow.
- **Filtered records:** auto-acked via post-select replay (borrow-safe).
- **Client:** net-new `commit_offsets_sync` + extracted `commit_topics`; `_sync` only; no metadata.
- **Ownership:** `commit_acked` filters to `assigned_partitions()`; broker unchanged.
- **Modes:** frontier machinery gated on `auto_commit == false`.
- **Positioning:** lighter than share groups; caller owns redelivery.
