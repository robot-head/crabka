# Single-Shard Commit Bypass Design

Let transactions confined to one range commit against that range's own local sequence instead of the global timestamp source, so the global clock's load is proportional to cross-shard traffic rather than total writes.

**Type:** Write-path change in gres-ranges/pgexec, mechanism-agnostic over the [TimestampSource seam](2026-07-20-timestamp-source-seam-design.md) — it works identically under `LogicalTso` and `HLC`. This is the decision record's ["single-shard commits use a local per-range sequence"](../../decisions/2026-07-20-write-scalability-two-mode-timestamp-source.md) decision: the biggest genuine horizontal-write lever, mirroring the per-partition offset design the Kafka side already proves.

## Design Goals

- Remove the common case — a transaction that reads and writes one range — from the global sequencer entirely: no grant RPC, no central counter increment, no cross-node coordination per commit.
- Preserve the existing correctness story unchanged for anything that spans ranges: Percolator prewrite/commit, `commit_ts > start_ts`, first-committer-wins, and snapshot visibility `commit_ts <= read_ts`.
- Keep cross-range reads consistent without asking single-shard writers to wait on anyone: consistency is reconciled on the (rarer) cross-range read path via closed timestamps, not on the (hot) single-shard write path.

## Architecture Overview

Today every timestamp transaction — even one whose route resolves to a single range — allocates `start_ts` and `commit_ts` from the tenant-wide `TimestampSource`. The gateway already classifies transactions: the ordinary-table machine tracks `touched`/`escalated` and gives unescalated transactions a direct 1PC commit, and the sharded-table router distinguishes `RouteTarget::single` from scatter with a primary-range fast path for autocommit writes. The classification exists; only the timestamp allocation ignores it.

The change gives each range a **local sequence**: a per-range monotone allocator in the same packed `u64` timestamp domain, seeded at recovery from the range's durable horizon and advanced by the Lamport rule — every globally stamped write applied to the range folds its stamp into the sequence, so local allocations always exceed every global timestamp the range has ever seen. A single-shard transaction takes `start_ts` and `commit_ts` from its range's local sequence; the version it writes is indistinguishable in storage from a globally stamped one.

Each range also publishes a **closed timestamp**: a watermark it promises never to commit below again. The local sequence reserves above the watermark before publishing it, and the range's leader raises it continuously (a few hundred milliseconds of lag is fine). Cross-range snapshot reads at `read_ts` consult the watermark: a range may serve the read only once its closed timestamp has reached `read_ts`, waiting out the (bounded, usually zero) gap otherwise. Single-range reads never consult watermarks — the range's own sequence is the full ordering authority for its own data.

The global `TimestampSource` is then invoked exactly when its name says: to order transactions across sources — cross-range transactions, and cross-domain Kafka+SQL transactions.

## Key Design Decisions

### Local sequences live in the shared timestamp domain

A local sequence is not a second kind of timestamp; it is an allocator of ordinary timestamps that happens to be range-local. Storage encodings, visibility checks, the horizon floor, and the descriptor machinery see one `u64` domain. Two ranges may allocate the same numeric value — that is harmless for versions, because a stamp only orders data within the range that assigned it, and cross-range ordering always flows through globally allocated stamps that both sequences have folded in. What it does rule out is using a locally allocated `start_ts` as a *global* transaction identity, which drives the escalation decision below.

The alternative — a distinct per-range sequence number domain reconciled at read time (a vector-clock flavor) — was rejected: it would fork the visibility rule and the storage encoding into per-range and global variants, exactly the two-stacks outcome the seam decision exists to avoid.

### Escalation restarts the transaction on global timestamps

A transaction that begins on one range under local stamps and then touches a second range cannot keep its local `start_ts`: the timestamp-transaction identity (descriptor keys, `global_xid` aliasing) assumes `start_ts` uniquely names the transaction across all participating ranges, and local allocation cannot promise cross-range uniqueness. On escalation the gateway aborts the local-stamped attempt and transparently replays the transaction with globally allocated timestamps — the same statement-replay machinery the gateway retry path already exercises. This is a deliberate rare-path penalty: workloads that routinely escalate mid-transaction were never going to benefit from the bypass, and they pay one restart, not a correctness tax.

The rejected alternative — carrying the local `start_ts` into the multi-range protocol and disambiguating identities with a range-id qualifier — keeps the fast path's stamps but threads a compound identity through the descriptor, resolve, and recovery paths, complicating exactly the machinery whose simplicity the Stateright and crash-matrix work depends on. Restart is strictly simpler and its cost is proportional to how rarely it should happen.

First delivery can scope the bypass to the narrowest classification that already exists — autocommit single-statement writes routed to a single range — and widen to interactive single-range transactions once the restart path is proven; the design is the same at both scopes.

### Closed timestamps piggyback on existing range liveness traffic

The watermark needs a publication channel with per-range freshness on the order of the closed-timestamp lag. Range leaders already maintain periodic authority traffic (registry liveness, epoch heartbeats); the closed timestamp rides that channel as an extra field rather than introducing a new gossip or subscription mechanism, and gateways cache it per range alongside the routing table they already hold. A dedicated watermark stream was rejected as a second liveness protocol to operate and reason about; if the piggybacked cadence ever proves too coarse, upgrading the channel is an implementation change invisible to the read-path contract.

### Cross-range reads wait for closure; they never push writers

When a cross-range read at `read_ts` finds a participating range's closed timestamp below `read_ts`, it waits for the watermark to advance (or retries at a slightly older `read_ts` where the session's consistency mode allows it). The alternative — having the read push the range to close `read_ts` immediately — creates a reverse dependency from the hot write path onto reader demand, and the whole point of the bypass is that single-shard writers coordinate with no one. The expected wait is the watermark lag minus clock staleness, typically zero by the time a multi-range scatter has done its routing work.

## Integration

- **TimestampSource seam:** unchanged trait; the bypass sits above it. Under `HLC` the local sequence's Lamport fold is literally the HLC receive rule, and the closed timestamp doubles as the bounded-staleness read bound the [HLC spec](2026-07-20-hlc-distributed-mode-design.md) notes as future work.
- **Percolator machinery:** single-shard commits still write ordinary committed versions (no intents needed — single-participant atomicity is the range's WAL). Prewrite/resolve paths are untouched and remain exclusive to multi-range transactions.
- **Durable-horizon floor:** the local sequence is seeded from and observed into the same `TimestampHorizonSource` floor, so recovery fencing holds without a new mechanism.
- **Kafka side:** none directly; the [unified-clock spec](2026-07-20-kafka-unified-clock-design.md) gives partitions the same shape (local offset order + stamps for cross-domain order), which is why the two compose.

## Kafka / KIP Compliance

No wire-visible change. This spec is entirely on the SQL/ranges side.

## Testing

- Extend the Stateright timestamp model with a two-range configuration: local sequences, the Lamport fold, and closed timestamps, checking that no interleaving lets a cross-range snapshot at `read_ts` miss a single-shard commit with `commit_ts <= read_ts`.
- Escalation restart: deterministic tests that a transaction observing local stamps, then touching a second range, replays onto global stamps with no lost or doubled effects — including a crash between abort and replay.
- Watermark-lag behavior: cross-range reads block until closure and unblock without polling storms; mutation testing on the wait predicate (a flipped comparison here silently serves stale reads, so the test must witness the boundary strictly).
- The existing single-range fast-path suites (1PC commit, autocommit primary-range) must pass unchanged under the bypass.
