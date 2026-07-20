# HLC Distributed Mode Design

A Hybrid Logical Clock implementation of `TimestampSource` for HA/geo deployments — node-local timestamp allocation with no central sequencer — plus the one-time promotion protocol that moves a solo tenant onto it.

**Type:** Second implementation of the [TimestampSource seam](2026-07-20-timestamp-source-seam-design.md), realizing the decision record's ["Distributed mode — HLC"](../../decisions/2026-07-20-write-scalability-two-mode-timestamp-source.md) component and its two attendant decisions: uncertainty-window reads with restart, and live promotion seeded from the LogicalTso horizon.

## Design Goals

- No central RTT floor and no single failure domain for timestamp allocation: every gateway/engine node mints stamps from its own clock. Range 0 stops being a liveness dependency for starting transactions.
- Correctness assumes only bounded clock skew from commodity NTP — no GPS/atomic/PTP requirement. The bound (`max_offset`) is configuration, and exceeding it degrades to spurious restarts, not anomalies, for the snapshot guarantees we make.
- Solo deployments pay none of this: uncertainty machinery activates only when the tenant's mode is `Hlc`.
- Promotion from solo is monotone by construction and migrates no data.

## Architecture Overview

Each node holds one HLC: a packed `u64` (physical milliseconds high, logical counter low — the packing fixed by the seam spec) updated by the standard rules. Reading the clock returns `max(wall, last) `, bumping the logical component when wall time hasn't advanced; observing a remote stamp folds it in the same way. Stamps therefore never regress, always dominate every stamp the node has seen, and stay within the skew bound of real time as long as NTP holds.

Allocation becomes node-local: `start_ts`, `read_ts`, and single-range `commit_ts` come straight off the local HLC with zero RPCs. The entire `TsoRpc`/`BatchedTsoClient`/grant-lease stack is simply not constructed in this mode — batching exists to amortize a central authority, and there is no central authority left to amortize.

Cross-range commits take `commit_ts` at the same call site the solo path uses, but the allocation folds first: every participant's prewrite acknowledgment carries the participant's HLC reading, the coordinator observes each into its clock, and the subsequent local read is by construction greater than anything any participant has stamped — the `max(participant HLCs)` of the decision record, expressed as observe-then-allocate rather than a bespoke max operation.

What the clock no longer guarantees is that allocation order equals real-time order across nodes, and the read path pays for that in one place: the uncertainty window.

## Key Design Decisions

### Uncertainty window with read restart, in the visibility check

A read at `read_ts` from node A can encounter a version committed by node B at `commit_ts > read_ts` that nevertheless happened before the read in real time — but only within the skew bound. So a version with `read_ts < commit_ts <= read_ts + max_offset` is neither visible nor safely invisible: it is *uncertain*, and the transaction restarts at a timestamp above the offending commit. Versions beyond `read_ts + max_offset` are genuinely concurrent and correctly invisible.

The tri-state lands in the pgmvcc visibility check (`satisfies_ts` grows an uncertainty verdict against the seam's uncertainty bound), because that is the one chokepoint every read already passes through; the restart itself is driven by the gateway's existing statement-replay machinery, and the per-node observe rule caps restarts at one per offending node in practice (after restarting, the reader has observed the stamp that burned it). Under `LogicalTso` the bound equals `read_ts`, the uncertain interval is empty, and the branch is dead code — solo mode pays a comparison, not a mechanism.

Commit-wait (Spanner/TrueTime) was rejected with the decision record: it buys external consistency at the price of tight-clock infrastructure and added latency on every write, and the deployments this mode targets run commodity NTP. Closed-timestamp bounded-staleness reads — already published by the [single-shard bypass](2026-07-20-single-shard-bypass-design.md) — are the designated escape hatch for read-heavy/geo-follower paths that want to skip uncertainty entirely at the cost of slight staleness; wiring a session-level staleness mode to them is follow-up work, not part of this spec.

### Promotion seeds the HLC from the solo horizon

Moving a live tenant from `LogicalTso` to `Hlc` must guarantee no new stamp ever falls at or below an existing one. The packing already guarantees this for any node whose wall clock is sane (physical > 0 dominates every frozen-physical logical stamp), but promotion does not rely on clock sanity. The protocol is a short administrative sequence: fence the solo oracle's epoch (the existing fencing mechanism — a fenced oracle refuses all further grants), read the persisted horizon from `MAX_TS_KEY`, distribute it in the mode-flip configuration, and every node folds the horizon into its HLC (an `observe`) before serving its first distributed stamp. Fence-before-read makes the horizon final; observe-before-serve makes every successor stamp dominate it. This is deliberately the same shape as the oracle's own successor-fencing story, one level up.

Re-bootstrapping a fresh distributed cluster was rejected in the decision record (dump/reload cutover for no gain), and there is no standing demotion path — demotion would be a symmetric fence-and-seed (recover the TSO at the maximum HLC stamp ever persisted, discoverable from the durable-horizon floor) but no scenario currently demands it, so it stays a documented possibility rather than built machinery.

### `max_offset` is a correctness bound for uncertainty, enforced operationally

`max_offset` (default in the 250–500 ms range, per-tenant configuration) is the promise the uncertainty window is sized to. Nodes monitor their NTP dispersion and refuse to serve — the same behavior as a fenced oracle — when their local error estimate approaches the bound, because a node outside the bound can commit "in the past" beyond where readers look for uncertainty. This turns the failure mode from silent stale reads into visible unavailability of one node, which is the right trade for a system whose solo mode exists precisely because it never wanted this dependency.

## Integration

- **TimestampSource seam:** `Hlc` implements the same trait; `observe` and the uncertainty bound are the two members that stop being trivial. Call sites, storage encodings, and the horizon floor are untouched by mode.
- **Durable-horizon floor:** node startup folds the local store's horizon into the clock before serving (the seam spec's obligation), which subsumes the solo recovery invariant.
- **Single-shard bypass:** composes directly — the per-range local sequence's Lamport fold is the HLC receive rule, so under this mode the local sequence *is* a range-scoped HLC, and closed timestamps are packed HLC stamps like any other.
- **Range 0:** keeps GTM/coordinator duties; sheds timestamp serving. The early-TSO-activation boot path is a no-op in this mode.
- **Kafka:** brokers in HLC mode stamp with their own node-local HLC (see the [unified-clock spec](2026-07-20-kafka-unified-clock-design.md)) — the shared physical-ish clock is what lets a partition and a SQL row share an order without a central source.

## Kafka / KIP Compliance

No wire-visible change on the Kafka side from this spec; broker stamping is internal (unified-clock spec) and offset semantics are untouched.

## Testing

- Extend the Stateright timestamp model with per-node clocks and bounded skew: verify no interleaving within the bound lets a snapshot read miss a real-time-prior commit without an uncertainty restart, and that restarts terminate (the observe rule makes progress).
- Skew-bound violation tests: a node pushed past `max_offset` must fence itself rather than serve; mutation testing on that comparison (a flipped bound silently readmits the anomaly the mode exists to prevent).
- Promotion: crash-matrix tests across the fence → read-horizon → seed sequence — a crash at any step must leave either a still-fenced solo tenant or a fully promoted one, never a node serving unseeded stamps; property test that every post-promotion stamp exceeds every pre-promotion stamp.
- The trait-level property suite from the seam spec runs unchanged over `Hlc`, which is the regression net for the shared commit path.
