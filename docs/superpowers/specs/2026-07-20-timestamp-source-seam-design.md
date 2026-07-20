# Timestamp Source Seam Design

Make the transaction-ordering primitive a pluggable seam — `TimestampSource` — with the existing logical oracle as its first implementation, so solo and distributed deployments choose a clock without forking the commit path.

**Type:** Foundation refactor for the two-mode write-scaling program decided in [the 2026-07-20 decision record](../../decisions/2026-07-20-write-scalability-two-mode-timestamp-source.md). Companion specs build on this seam: [single-shard bypass](2026-07-20-single-shard-bypass-design.md), [HLC distributed mode](2026-07-20-hlc-distributed-mode-design.md), and the [Kafka unified clock](2026-07-20-kafka-unified-clock-design.md).

## Design Goals

- One commit/visibility/Percolator code path, parameterized only by an opaque, totally ordered timestamp. The fork between solo (`LogicalTso`) and distributed (`HLC`) is isolated behind a single trait chosen at provision time.
- `LogicalTso` behavior is unchanged: the range-0 oracle, its successor grace period, stride batching, epoch fencing, and the exhaustive Stateright monotonicity model all carry over untouched.
- No storage or wire format changes. MVCC version keys, tuple headers, and the visibility rule keep their current big-endian `u64` encodings in both modes.
- The seam widens only where a distributed clock genuinely needs it: observing remote stamps and exposing an uncertainty bound. Solo pays nothing for either.

## Architecture Overview

The seam already half-exists. Every engine allocates timestamps through the `crabka_pgexec::TimestampOracle` trait (`timestamp_txn.rs`) — typed allocations for read timestamps, transaction ids (start_ts), write leases, and commit timestamps, each fenced against the durable-horizon floor by the `_after` variants. Below it, the solo stack is `PgexecTsoOracle` → `BatchedTsoClient` → `TsoRpc` → `TsoOracle`, installed per tenant by the four-way match in `tenant.rs` (in-process oracle when hosting range 0, registry-forwarded client otherwise).

This design renames and extends that upper trait into `TimestampSource`. The name change is deliberate: "oracle" now describes only one implementation. The lower layers (`TsoRpc`, `BatchedTsoClient`, `TsoOracle`, `RegistryTsoRpc`) become private machinery of the `LogicalTso` implementation — they are how a centralized logical source is served efficiently, not part of the ordering contract. The `HLC` implementation (see the [HLC spec](2026-07-20-hlc-distributed-mode-design.md)) allocates node-locally and never touches that stack.

Two additions to the trait cover what a distributed clock needs and a centralized one doesn't:

- `observe(ts)` — fold a timestamp learned from another node into the local clock (the Lamport/HLC receive rule). For `LogicalTso` this is a no-op: all allocations flow through one authority, so there is nothing to learn.
- an uncertainty bound on reads — the timestamp above which a read is certain not to miss a concurrent commit. For `LogicalTso` the bound equals the read timestamp itself (empty window); for `HLC` it is `read_ts + max_offset`. The read path consumes this uniformly; only the HLC value ever triggers a restart.

The four public timestamp newtypes (`TimestampTransactionId`, `CommitTimestamp`, `ReadTimestamp`, and the oracle-side `TsoTimestamp`) keep their roles; they remain thin wrappers over the shared `u64` timestamp domain described next.

## Key Design Decisions

### One u64 timestamp domain; HLC packs into it

The load-bearing constraint is that timestamps are raw big-endian `u64`s at every durable boundary: the version-key suffix that sorts MVCC versions, the tuple header carrying `start_ts`/`commit_ts`, the `commit_ts <= read_ts` visibility check in pgmvcc, the descriptor keys, and the `MAX_TS_KEY` horizon. Rather than widening all of these to carry a compound HLC (physical, logical) pair, the HLC packs both components into one `u64` — physical milliseconds in the high bits, logical counter in the low bits — so numeric order, big-endian byte order, and HLC "happens-before" order all coincide and every existing encoding and comparison is untouched.

A packed split on the order of 42 bits of milliseconds (over a century from a Crabka epoch) and 22 bits of logical counter (about 4M causally related events per millisecond per node) is ample; the exact split is an implementation constant, not an architectural commitment.

This packing also delivers the decision record's continuum for free: a logical timestamp is exactly a packed HLC whose physical component is zero. Every solo-mode stamp therefore sorts below every distributed-mode stamp by construction, which is what makes the [seed-promotion transition](2026-07-20-hlc-distributed-mode-design.md) monotone without rewriting any persisted version.

The rejected alternative — widening the on-disk timestamp to 96 or 128 bits — would churn every key and value encoding, the horizon caches, and the range wire protocol, and would buy nothing except a wider logical counter that nothing needs. Greenfield status permits the churn; the pointlessness argues against it.

### The seam sits at `TimestampOracle`, not `TsoRpc`

`TsoRpc` (grant a lease of N timestamps) looks like a tempting, smaller seam, but it bakes in the centralized model: leases only make sense when a single authority hands out disjoint ranges. An HLC never grants leases — every node mints stamps locally. The typed allocation layer is the narrowest interface both implementations satisfy honestly, and it is already what every caller uses, so no call site changes.

### Mode is explicit tenant configuration, not inferred

`MultiRangeTenantConfig` gains an explicit timestamp-mode field (`LogicalTso` default, `Hlc` opt-in) set at provision time. Today's four-way inference (in-process vs. registry-forwarded vs. unavailable) remains, but it selects *how the chosen mode is wired for this node's hosting topology*, not *which mode the tenant runs*. Inferring mode from topology was rejected: a distributed topology running `LogicalTso` is a legitimate configuration (single-zone HA), and promotion must be an administrative act, not an emergent one.

### The durable-horizon floor stays mechanism-agnostic

The `TimestampHorizonSource` floor (the cached per-store maximum durable timestamp, kept exact by the horizon-observing committer) and the `allocate_*_after` fencing survive unchanged as an obligation on every implementation: no source may ever allocate at or below the durable horizon of the store it serves. For `LogicalTso` this is the existing recovery invariant; for `HLC` it becomes part of node startup (fold the local horizon into the clock before serving). Keeping the floor outside the trait means the recovery-safety argument does not depend on which clock is plugged in.

## Integration

- **Engines and gateway:** call sites are unchanged — they already hold an `Arc<dyn TimestampOracle>`; it becomes `Arc<dyn TimestampSource>`. Installation still fans one instance to every hosted engine.
- **Cross-range commit:** the timestamp-scatter path obtains a single `commit_ts` from the source and fans it to all participants. That call site is where the HLC implementation later computes `max` over participant observations; the seam makes it a different implementation of the same allocation, not a different commit protocol.
- **Range 0:** remains the home of `LogicalTso` and of nothing else. In HLC mode range 0 still hosts the GTM and coordinator duties but serves no timestamp grants.
- **Kafka:** the broker obtains stamps through the same trait (see the [unified-clock spec](2026-07-20-kafka-unified-clock-design.md)), which is what makes cross-domain ordering coherent in both modes.

## Kafka / KIP Compliance

No wire-visible change. The seam is internal to the SQL/ranges side; Kafka protocol behavior is untouched by this refactor.

## Testing

- The existing Stateright TSO monotonicity model continues to verify `LogicalTso` unchanged — that is the point of leaving the oracle stack intact beneath the seam.
- Trait-level property tests run the same suite over both implementations: monotonicity per allocator, `commit_ts > start_ts` enforcement, horizon fencing (`allocate_*_after` refuses stamps at or below the floor), and packed-order coherence (HLC receive rule never regresses a stamp).
- Mutation testing per the workspace conventions; the packing arithmetic and the observe/max fold are the mutation-sensitive spots and each needs a test that fails on an off-by-one bit split.
