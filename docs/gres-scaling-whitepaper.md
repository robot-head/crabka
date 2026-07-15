# Scaling Postgres on a Kafka Substrate: the Crabka Gres Architecture

**A Crabka whitepaper**

## Abstract

Crabka Gres is a pure-Rust, Postgres-compatible compute tier that stores all durable state on the Crabka substrate: per-range Kafka write-ahead-log topics and object-store checkpoints.
This paper explains how the architecture scales writes horizontally — across tenants, across ranges within a tenant, and within a single table — without per-tenant consensus, without synchronized clocks, and without changing the Kafka broker.
The core ideas are: (1) make the unit of write parallelism a *range* — a single-writer compute journaling to its own totally-ordered log, fenced by Kafka's transactional-producer epochs; (2) shard tables by rowid interval so inserts allocate locally and never coordinate; (3) derive cross-range transaction ordering from a single logical timestamp oracle with batched, stride-durable grants, rather than from physical clocks; and (4) make elasticity an artifact of the checkpoint format — splits, moves, and merges are checkpoint forks over immutable key-sorted objects, not row-copying pipelines.
We state the resulting scaling envelope and its measured ceilings honestly, including the ones that remain.

## 1. Introduction

Gres serves many small-to-large Postgres-compatible databases as a serverless product: each tenant is a topic set, a bucket prefix, and at most one live compute process per range, disposable and recoverable entirely from the substrate.
The engine is Postgres-compatible at the wire and SQL-semantics level (pgwire v3, extended query protocol, PostgreSQL-faithful MVCC), not at the page or physical-WAL level; tenants needing C extensions or byte-level fidelity belong to Crabka's separate real-Postgres tier.

The scaling question this paper answers was posed during the chapter's design review: *can it horizontally scale Postgres, and where does it break first?*
The answer is layered, because the architecture was built in deliberate slices — substrate durability ([G-2](superpowers/specs/2026-07-09-crabka-gres-g2-substrate-wal-design.md)), checkpoints ([G-3](superpowers/specs/2026-07-09-crabka-gres-g3-checkpoints-design.md)), multi-range tenants ([G-7](superpowers/specs/2026-07-09-crabka-gres-g7-multirange-design.md)), sharded tables ([G-8](superpowers/specs/2026-07-09-crabka-gres-g8-sharded-tables-design.md)), and distributed maturity ([G-9](superpowers/specs/2026-07-09-crabka-gres-g9-distributed-maturity-design.md)) — each of which removes a specific, named ceiling while preserving the correctness corpus of everything beneath it.
Section 2 describes the substrate primitives every layer stands on; sections 3–6 walk the scaling axes from tenant count down to single-table commit rate; section 7 covers elasticity; section 8 states the envelope and what breaks first.

## 2. The substrate primitives

### 2.1 A range is a single writer over a totally-ordered log

The unit of both durability and write parallelism is the **range**: a single-writer compute process whose every durable mutation flows through a `Committer` seam into one single-partition Kafka topic (`__gres_wal.<tenant>.r<N>`), with a local working store (a pure-Rust LSM) holding the full materialized state for reads.
Reads never leave the compute; writes become group-committed batches, one Kafka transaction per commit group, acknowledged to SQL clients only after the broker confirms the transaction committed and the batch is applied locally.
The substrate — not the engine — owns replication and quorum durability, which is why a range needs no raft group of its own: the donor engine's consensus layer was deliberately dropped, and Kafka's replicated log stands in for it.

### 2.2 Fencing without a lease service

Two computes for one range must never interleave writes.
Rather than a lease service, each range's writer produces with a fixed transactional id; a successor's `InitProducerId` bumps the producer epoch and the broker fences the predecessor, which receives hard produce errors and self-terminates.
The recovery ordering is fence-first and load-bearing: fence, produce a barrier record, restore the newest checkpoint, replay the log tail to the barrier, then serve.
Fencing *before* reading the log's end guarantees the replayed state contains every append any predecessor ever acknowledged; the model corpus carries a dedicated action (`ZombieAppendAfterEndRead`) whose counterexamples are exactly what the reversed order admits.
This one primitive — epoch fencing on a totally-ordered log — is reused everywhere the architecture needs "exactly one authority": range writers, the split orchestrator, and (section 6) the timestamp oracle's liveness gate.

### 2.3 Checkpoints bound recovery and enable forks

A background checkpointer snapshots the working store at a point equal to a durable log prefix, uploads immutable **key-sorted part objects** under the tenant's bucket prefix, writes the manifest last (so a torn upload is invisible), and truncates the WAL topic up to the covered offset.
Spin-up cost is therefore bounded by snapshot download plus tail replay, and the log never grows without bound.
The key-sorted part format turns out to be the load-bearing gift for elasticity: because parts are sorted by key, two successor ranges can each restore a *filtered view* of the same immutable objects — which is what makes splits cheap (section 7).

## 3. Axis one: tenant count

The first scaling axis is trivial by construction: tenants are independent.
Each is a topic set, a bucket prefix, and computes; the fleet grows by adding tenants, and one fleet cell (one PgDog front door plus one aggregated configuration) is sized for roughly 10³ tenants, beyond which the horizontal unit is more cells.
The named fleet ceilings are operational, not architectural: the O(N) configuration render pipeline, Kubernetes object fan-out, and broker-side per-topic overhead for many small topics — the last mitigated by parking idle tenants' WAL topics behind a final checkpoint so suspended tenants stop costing the brokers anything.

## 4. Axis two: ranges within a tenant (table-granular scale-out)

G-7 gives one tenant database multiple ranges, each the exact single-writer construction of section 2, with tables assigned to ranges by a range map.
Aggregate write throughput grows roughly linearly with ranges for range-local transactions, because each range's commit path — its group-committed produce stream — is independent.

Cross-range transactions are handled by ported, model-checked two-phase-commit machinery: a designated system range (**range 0**) holds the catalog, the global commit log (clog), and global transaction-id allocation; participant writes carry `Prepared` markers deferring visibility to range 0's write-once decisions; recovery re-derives in-doubt state from durable markers.
Two details matter for scaling honesty.
First, every range compute maintains a local replica of range 0 by tailing its topic at `READ_COMMITTED`, so catalog and clog reads are always local.
Second, freshness for consistent snapshots comes from a **log-derived barrier**: fetch range 0's broker-log end offset and wait until the local tail has applied through it.
The offset must come from the partition leader's log, never from the range-0 SQL writer — a fenced-but-unaware zombie writer would answer with a too-low offset — which is the substrate's version of the classic deposed-leader ReadIndex hole, closed structurally rather than probabilistically.

## 5. Axis three: sharding a single table

G-7's remaining wall is that one table lives on one range.
G-8 shards a table across ranges by **rowid interval**: boundaries are `(table_id, rowid)` points in a versioned range map, and each of a sharded table's ranges allocates rowids from its own interval using its existing per-range sequence machinery.
The consequence is the property that makes ingest scale: an INSERT is routed by placement policy to one range, allocates its rowid locally, and **never coordinates** — ingest, storage, and scan bandwidth grow linearly with ranges.

Consistent cross-range reads refuse to invent a second clock: sharded tables become *global-visibility tables*, meaning every write is stamped on the one shared timeline the 2PC machinery already provides, so any range can evaluate any sharded-table tuple's visibility against a caller's global snapshot without access to foreign range-local state.
Query execution changes at exactly one seam: the table scan fans out per covering range (`RangeScanner`), remote ranges evaluate visibility under the caller's snapshot and stream back visible rows in rowid order, and concatenation across interval-ordered ranges reconstructs the ordered scan the executor above expects — joins, aggregates, and sorts run unchanged on the gateway.
Filter and projection pushdown, partial aggregation, and per-range top-K ride the same seam as equivalence-preserving rewrites (G-9b); there is deliberately no cost model, only correctness-preserving plan transformations and tunable thresholds.
Hash sharding (G-9c) is the same machinery under a bucket-prefix key encoding — a hash-sharded table is an interval-sharded table whose intervals the system distributes — so splits, moves, models, and the balancer apply unchanged.

This design has one honest, stated ceiling: under G-8, every sharded-table commit is one decision record through range 0's single writer, batched in its group commit — order 10⁴ commits per second per tenant.
The ceiling is structural (the global snapshot needs one coherent capture source), it is measured in the scaling gates rather than discovered in production, and removing it is the next section.

## 6. Removing the commit ceiling: timestamp transactions without synchronized clocks

G-9a replaces the sharded-table timeline with Percolator-class timestamp transactions.
Range 0 stops deciding commits and becomes a **timestamp oracle (TSO)** only; commit decisions move to the data ranges themselves, so decision throughput scales with ranges and the last per-table ceiling falls.

The mechanism deserves precision, because "timestamp" often implies synchronized physical clocks, and this design uses none.

**Timestamps are logical grants from a single oracle.**
Every `read_ts` and `commit_ts` for a tenant's sharded tables is a monotone integer handed out by range 0's writer.
Ordering is a property of one counter, not an assumption about clock skew; the decentralized/hybrid-logical-clock alternative was considered and rejected because the centralized oracle *extends* the proven fencing stack instead of re-founding correctness on clock-error bounds.
Notably, leasing out timestamp *blocks* (the way the G-8 design leases global-xid blocks) was rejected too: a client holding a leased block could stamp a "later" timestamp before another session's earlier grant, breaking cross-session read-your-writes; one oracle serving all grants is load-bearing.

**Batching plus stride-ahead durability make one counter fast enough.**
Grants are served from memory in batches (order 10⁵–10⁶ timestamps per second per tenant); the oracle never persists individual grants but durably advances a `max_ts` watermark in large strides through its ordinary WAL.
After a crash or fence, the successor resumes past the last durable stride — timestamps may be skipped but never reused, and monotonicity holds across generations because the stride is on the log and recovery is fence-first.

**Fencing, not clock freshness, closes the failure case.**
The one hazard that resembles a clock problem — a deposed-but-alive oracle serving stale-yet-monotone timestamps from memory, silently violating read-your-writes — is closed by epoch liveness: the oracle amortizes a heartbeat (a no-op produce on its transactional session) into each grant batch and stops granting the instant a heartbeat returns fenced.
The staleness window is the heartbeat interval, a stated configuration bound, and the model corpus carries a live-zombie action verifying that the invariant "no granted read timestamp precedes a commit acknowledged before the grant" fails without the gate and holds with it.

**Commits become range-local.**
A transaction's writes land as durable **intents** (which are the locks — they replay, so recovery re-derives less) in each participant range's own WAL; the primary range (first write) holds the single write-once commit record at `commit_ts`; secondaries resolve asynchronously and lazily.
Visibility is a comparison — a version is visible iff `commit_ts ≤ read_ts` — with intents resolved through their primary (bounded wait, then push-abort through the existing settle machinery).
A timestamp is its own consistency proof, so the range-0 read barrier disappears from sharded-table reads entirely, deleting the hottest read-path cost along with the commit ceiling.
The isolation bar is unchanged and externally validated: snapshot isolation with first-committer-wins, single-key linearizable, checked with Elle under writer kills and oracle fences.

## 7. Elasticity: splits, moves, merges, and auto-sharding

Scaling that requires downtime or bulk copies is not horizontal scaling in practice, so the growth operations are built from the checkpoint format.

A **split** of a range at rowid `b` is a checkpoint fork: force a checkpoint; commit the next range-map version through range 0 (map reads are barrier-gated, so readers converge without new machinery); both successors restore *filtered views of the same immutable key-sorted checkpoint objects* plus a filtered replay of the predecessor's log tail; the predecessor's topic is parked with a generation bump and each side opens a fresh topic with fresh epochs.
Writes pause only from the checkpoint's covered offset until the sides open — a bounded window dominated by tail length, kept short by pre-split checkpointing and measured with an asserted ceiling in the split system tests.
In-doubt prepared markers split by key interval with the rows they govern, and the split model — exhaustive over split × 2PC × recovery × fence interleavings, the hairiest model in the chapter — is the gate: no lost or double-honored decision, never neither-side-serving and never both-for-one-key.
A **move** is the degenerate split with one empty side targeting a different compute; a **merge** is the inverse fork (checkpoint both adjacent ranges, commit the unifying map version, restore the union, park both predecessors).

Placement is closed-loop: a goal-based balancer evaluates per-range metrics (store size, commit rate, scan bytes) against goals — size ceilings and floors, load skew, co-location groups, index placement, anti-affinity — and emits bounded split/move/merge/convert plans executed serially through the one audited orchestrator, under rate limits, cooldowns, and a dry-run mode.

Finally, scaling is not gated on vendor DDL.
`SHARDED` is a hint, not a requirement: when an unsharded table crosses policy thresholds, the balancer auto-converts it via an online conversion fork (a split-bounded pause; a checkpoint freeze pass rewrites committed history to timestamp versions; the catalog flag flips atomically with the map-version commit, gated on no in-doubt cross-range commit straddling the conversion).
A plain, unadorned `CREATE TABLE` from a stock Postgres application therefore scales without the application ever learning Crabka syntax, and unconverted tables remain byte-identical to the single-range baseline.

## 8. The envelope, stated honestly

After G-9, per sharded table: ingest, storage, scan bandwidth, and commit rate all scale roughly linearly with ranges.
Per-statement write latency is two group-commit cycles typical (prewrite on each touched range plus the commit record on the primary) — a few-milliseconds floor set by durable produce round trips, amortized by group commit, and improved by substrate durability tiers without engine changes.
Point reads on hash-sharded or globally-indexed keys are single-range.

What remains, and what breaks first:

- **Per range:** the executor's scan envelope between index waves (the engine is a full-scanning tree-walker until the index breadth cycle lands — for one growing tenant, the read path breaks first) and the group-commit latency floor.
- **Per tenant:** the TSO RPC path — a measured observation point at 10⁵–10⁶ grants/second, not a projected ceiling — and range 0's residual role in DDL and layout changes, both far above the decision traffic that used to bind.
  The TSO is also a liveness dependency: sharded-table transactions stall while range 0's writer recovers, bounded by the same fence-and-prologue story as any range.
- **Per fleet:** the configuration/reload pipeline around 10³ tenants per cell, then broker per-topic overhead around 10⁴ topics — which ranges multiply, making the named broker workstreams (follower-fetch multiplexing, per-topic metadata indexing) bind sooner in range-heavy deployments.
- **Gateway memory:** scatter-gather materializes working sets at the gateway, the same envelope as the single-node engine; pushdown narrows it but a general distributed exchange operator is explicitly out of scope.

Kafka-as-log is the last thing to break on every axis.

## 9. Related designs, briefly

The lineage is deliberate rather than novel-for-novelty's-sake.
The disposable-compute-over-shared-storage posture follows Neon's disaggregation, with a Kafka log replacing the safekeeper tier.
Timestamp transactions are Percolator-class (as in TiDB): a central logical oracle, durable intents as locks, primary-record commit — chosen over Spanner's TrueTime because a per-tenant database needs no geo-scale clock infrastructure, and over HLC schemes because a fenced single oracle gives exact ordering where hybrid clocks give bounded uncertainty.
What is distinctive is the substrate reuse: Kafka's transactional-producer epochs serve as the fencing primitive for every single-writer role in the system, and the checkpoint object format doubles as the resharding mechanism, so the distributed database inherits its safety properties from machinery the broker already proves.

## 10. Conclusion

Gres scales writes horizontally by composing one primitive — a fenced single writer over a totally-ordered substrate log — at three granularities: the tenant, the range, and (through interval sharding and timestamp transactions) the single table.
Coordination is engineered out of the hot path rather than optimized within it: inserts allocate locally, commits decide on their primary range, reads carry their own consistency proof in a timestamp, and the only remaining central party hands out batched integers.
Every ceiling the architecture still has is named, measured in CI gates, and owned by a scheduled workstream — which is the difference between a scaling story and a scaling promise.

## References

- [Chapter design: a pure-Rust Postgres compute engine on the Crabka substrate](superpowers/specs/2026-07-09-crabka-gres-chapter-design.md) — scaling model and ceilings, architecture overview.
- [G-2: Substrate WAL](superpowers/specs/2026-07-09-crabka-gres-g2-substrate-wal-design.md) — the committer seam, group commit, fencing.
- [G-3: Checkpoints](superpowers/specs/2026-07-09-crabka-gres-g3-checkpoints-design.md) — manifest-last snapshots, truncation, the recovery model.
- [G-7: Multi-range tenants](superpowers/specs/2026-07-09-crabka-gres-g7-multirange-design.md) — table-granular scale-out, cross-range 2PC, the log-derived barrier.
- [G-8: Sharded tables](superpowers/specs/2026-07-09-crabka-gres-g8-sharded-tables-design.md) — rowid-interval sharding, scatter-gather, checkpoint-fork splits.
- [G-9: Distributed maturity](superpowers/specs/2026-07-09-crabka-gres-g9-distributed-maturity-design.md) — timestamp transactions, pushdown, hash sharding, indexes, the balancer, auto-sharding.
