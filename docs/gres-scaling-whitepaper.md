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

The scaling question this paper answers is the one every distributed SQL system must: *can it horizontally scale Postgres, and where does it break first?*
The answer rests on one primitive composed at three granularities.
Section 2 describes that primitive and the substrate machinery around it; sections 3–5 walk the scaling axes from tenant count down to single-table throughput; section 6 explains the transaction protocol that makes single-table commit rate scale without synchronized clocks; section 7 covers elasticity; section 8 states the envelope and what breaks first.

## 2. The substrate primitives

### 2.1 A range is a single writer over a totally-ordered log

The unit of both durability and write parallelism is the **range**: a single-writer compute process whose every durable mutation flows through a `Committer` seam into one single-partition Kafka topic (`__gres_wal.<tenant>.r<N>`), with a local working store (a pure-Rust LSM) holding the full materialized state for reads.
Reads never leave the compute; writes become group-committed batches, one Kafka transaction per commit group, acknowledged to SQL clients only after the broker confirms the transaction committed and the batch is applied locally.
The substrate — not the engine — owns replication and quorum durability, which is why a range needs no raft group of its own: Kafka's replicated log stands in for per-tenant consensus.

### 2.2 Fencing without a lease service

Two computes for one range must never interleave writes.
Rather than a lease service, each range's writer produces with a fixed transactional id; a successor's `InitProducerId` bumps the producer epoch and the broker fences the predecessor, which receives hard produce errors and self-terminates.
The recovery ordering is fence-first and load-bearing: fence, produce a barrier record, restore the newest checkpoint, replay the log tail to the barrier, then serve.
Fencing *before* reading the log's end guarantees the replayed state contains every append any predecessor ever acknowledged; the model-checking corpus carries a dedicated action (`ZombieAppendAfterEndRead`) whose counterexamples are exactly what the reversed order admits.
This one primitive — epoch fencing on a totally-ordered log — is reused everywhere the architecture needs "exactly one authority": range writers, the split orchestrator, and (section 6) the timestamp oracle's liveness gate.

### 2.3 Checkpoints bound recovery and enable forks

A background checkpointer snapshots the working store at a point equal to a durable log prefix, uploads immutable **key-sorted part objects** under the tenant's bucket prefix, writes the manifest last (so a torn upload is invisible), and truncates the WAL topic up to the covered offset.
Spin-up cost is therefore bounded by snapshot download plus tail replay, and the log never grows without bound.
The key-sorted part format is also the load-bearing enabler for elasticity: because parts are sorted by key, two successor ranges can each restore a *filtered view* of the same immutable objects — which is what makes splits cheap (section 7).

## 3. Axis one: tenant count

The first scaling axis is trivial by construction: tenants are independent.
Each is a topic set, a bucket prefix, and computes; the fleet grows by adding tenants, and one fleet cell (one PgDog front door plus one aggregated configuration) is sized for roughly 10³ tenants, beyond which the horizontal unit is more cells.
The named fleet ceilings are operational, not architectural: the O(N) configuration render pipeline, Kubernetes object fan-out, and broker-side per-topic overhead for many small topics — the last mitigated by parking idle tenants' WAL topics behind a final checkpoint so suspended tenants stop costing the brokers anything.

## 4. Axis two: ranges within a tenant

A tenant database spans multiple ranges, each the exact single-writer construction of section 2, with tables assigned to ranges by a versioned range map.
Aggregate write throughput grows roughly linearly with ranges, because each range's commit path — its group-committed produce stream — is independent.

A designated system range (**range 0**) anchors the pieces that must be singular: the catalog, the range map, DDL, and the timestamp oracle of section 6.
Every range compute maintains a local replica of range 0 by tailing its topic at `READ_COMMITTED`, so catalog reads are always local.
Where a statement needs a fresh view of range-0 state (catalog or layout changes, and transactions over unsharded tables), freshness comes from a **log-derived barrier**: fetch range 0's broker-log end offset from the partition leader and wait until the local tail has applied through it.
The offset must come from the log, never from the range-0 SQL writer — a fenced-but-unaware zombie writer would answer with a too-low offset — which is the substrate's version of the classic deposed-leader ReadIndex hole, closed structurally rather than probabilistically.
Every range compute also runs a gateway, so any compute answers any statement, executing locally or forwarding to the owning range; a per-tenant service fronts them all behind the pooler.

## 5. Axis three: sharding a single table

Tables assigned whole to ranges scale a *database*; scaling a *table* requires spreading its rows.
Gres shards a table across ranges by **rowid interval**: boundaries are `(table_id, rowid)` points in the versioned range map, and each of a sharded table's ranges allocates rowids from its own interval using its existing per-range sequence machinery.
The consequence is the property that makes ingest scale: an INSERT is routed by placement policy to one range, allocates its rowid locally, and **never coordinates** — ingest, storage, and scan bandwidth grow linearly with ranges.
Hash sharding is the same machinery under a bucket-prefix key encoding — `(table_id, bucket, rowid)`, buckets fixed at creation — so a hash-sharded table is an interval-sharded table whose intervals the system distributes; equality predicates on the hash column route point statements to a single range, and tables sharing a hash spec form co-location groups whose corresponding intervals are placed on the same computes.

Query execution changes at exactly one seam: the table scan fans out per covering range (`RangeScanner`), remote ranges evaluate MVCC visibility under the caller's snapshot and stream back visible rows in rowid order, and concatenation across interval-ordered ranges reconstructs the ordered scan the executor above expects — joins, aggregates, and sorts run unchanged on the gateway.
A light planner pass keeps the fan-out cheap without a cost model: predicate and projection pushdown, per-range partial aggregation, per-range top-K merged at the gateway, and a three-way join strategy (co-partitioned when both sides share a hash spec and co-location group, broadcast when one side is small, gather as the always-correct fallback) — every rewrite equivalence-preserving and property-tested against the unpushed plan.
Secondary indexes narrow the scatter: local per-range indexes are maintained atomically in the owning range's batches, and global indexes — themselves sharded by indexed key, giving single-range point lookups on secondary keys — are maintained as one more write in the same distributed transaction, routine under the protocol of the next section.

What makes cross-range reads and writes *correct* — snapshot-isolated, first-committer-wins, with no statement-shape restrictions — is the transaction protocol those scans and writes run under.

## 6. Transactions: a timestamp oracle instead of synchronized clocks

Sharded-table transactions are Percolator-class timestamp transactions.
The design deserves precision, because "timestamp" often implies synchronized physical clocks, and this design uses none.

**Timestamps are logical grants from a single oracle.**
Every `read_ts` and `commit_ts` for a tenant's sharded tables is a monotone integer handed out by range 0's writer, acting as the tenant's **timestamp oracle (TSO)**.
Ordering is a property of one counter, not an assumption about clock skew; a decentralized or hybrid-logical-clock scheme was rejected because the centralized oracle extends the proven fencing stack instead of re-founding correctness on clock-error bounds.
Leasing out timestamp *blocks* to gateways was rejected too: a client holding a leased block could stamp a "later" timestamp before another session's earlier grant, breaking cross-session read-your-writes; one oracle serving all grants is load-bearing.

**Batching plus stride-ahead durability make one counter fast enough.**
Grants are served from memory in batches (order 10⁵–10⁶ timestamps per second per tenant); the oracle never persists individual grants but durably advances a `max_ts` watermark in large strides through its ordinary WAL.
After a crash or fence, the successor resumes past the last durable stride — timestamps may be skipped but never reused, and monotonicity holds across generations because the stride is on the log and recovery is fence-first.
The grant path is engineered like a data path, not a control path: gateways coalesce concurrent requests through a conveyor batcher (at most one grant RPC in flight, and everything that queues behind it drains into the next single RPC, so batch size self-tunes to upstream latency), and the oracle serves within-stride grants lock-free through a compare-exchange reservation, with only stride persists and liveness-certificate renewals serializing on a mutex.
Grant and batch-fill counters ride both halves, so the per-tenant observation point of section 8 is measured rather than estimated.

**Fencing, not clock freshness, closes the failure case.**
The one hazard that resembles a clock problem — a deposed-but-alive oracle serving stale-yet-monotone timestamps from memory, silently violating read-your-writes — is closed by epoch liveness: the oracle amortizes a heartbeat (a no-op produce on its transactional session) into each grant batch and stops granting the instant a heartbeat returns fenced.
The staleness window is the heartbeat interval, a stated configuration bound, and the model corpus carries a live-zombie action verifying that the invariant "no granted read timestamp precedes a commit acknowledged before the grant" fails without the gate and holds with it.
The liveness gate closes the zombie's half of the hazard; a successor grace period closes the other half: a recovering oracle refuses its first grant until the predecessor's largest possible certificate has lapsed, so arbitrarily fast failover cannot acknowledge a commit while a deposed oracle may still be serving from memory.
The model corpus represents the certificate windows explicitly — the safe configuration passes exhaustively at 11.4 million states, and removing the grace rule reproduces the freshness counterexample — and gateways converge on a successor without client involvement: a fenced oracle answers as a deposed leader, and grant RPCs re-resolve the registry and retry once, which is safe because an unclaimed grant only burns timestamps.

**Commits are range-local, so commit rate scales with ranges.**
A transaction's writes land as durable **intents** (which are the locks — they replay, so recovery re-derives them from the log rather than reconstructing in-memory state) in each participant range's own WAL; the primary range (first write) holds the single write-once commit record at `commit_ts`; secondaries resolve asynchronously and lazily.
Visibility is a comparison — a version is visible iff `commit_ts ≤ read_ts` — with intents resolved through their primary (bounded wait, then push-abort through the settle machinery that also handles crashed participants).
A timestamp is its own consistency proof, so sharded-table reads need no range-0 barrier at all: the only central party on the hot path hands out batched integers.
The isolation bar is snapshot isolation with first-committer-wins (write-write conflicts detected at prewrite via intents), single-key linearizable, validated externally with Elle under writer kills and oracle fences.

## 7. Elasticity: splits, moves, merges, and auto-sharding

Scaling that requires downtime or bulk copies is not horizontal scaling in practice, so the growth operations are built from the checkpoint format.

A **split** of a range at rowid `b` is a checkpoint fork: force a checkpoint; commit the next range-map version through range 0 (map reads are barrier-gated, so readers converge without new machinery); both successors restore *filtered views of the same immutable key-sorted checkpoint objects* plus a filtered replay of the predecessor's log tail; the predecessor's topic is parked with a generation bump and each side opens a fresh topic with fresh epochs.
Writes pause only from the checkpoint's covered offset until the sides open — a bounded window dominated by tail length, kept short by pre-split checkpointing and measured with an asserted ceiling in the split system tests.
In-doubt intents split by key interval with the rows they govern, and the split model — exhaustive over split × commit × recovery × fence interleavings — is the correctness gate: no lost or double-honored decision, never neither-side-serving and never both-for-one-key.
A **move** is the degenerate split with one empty side targeting a different compute; a **merge** is the inverse fork (checkpoint both adjacent ranges, commit the unifying map version, restore the union, park both predecessors).

Placement is closed-loop: a goal-based balancer evaluates per-range metrics (store size, commit rate, scan bytes) against goals — size ceilings and floors, load skew, co-location groups, index placement, anti-affinity — and emits bounded split/move/merge/convert plans executed serially through the one audited orchestrator, under rate limits, cooldowns, and a dry-run mode.

Finally, scaling is not gated on vendor DDL.
`SHARDED` is a hint, not a requirement: when an unsharded table crosses policy thresholds, the balancer auto-converts it via an online conversion fork (a split-bounded pause; a checkpoint freeze pass rewrites committed history to timestamp versions; the catalog flag flips atomically with the map-version commit, gated on no in-doubt cross-range commit straddling the conversion).
A plain, unadorned `CREATE TABLE` from a stock Postgres application therefore scales without the application ever learning Crabka syntax, and unconverted tables remain byte-identical to the single-range baseline.

## 8. The envelope, stated honestly

Per sharded table: ingest, storage, scan bandwidth, and commit rate all scale roughly linearly with ranges.
Per-statement write latency is two group-commit cycles typical (prewrite on each touched range plus the commit record on the primary) — a few-milliseconds floor set by durable produce round trips, amortized by group commit, and improved by substrate durability tiers without engine changes.
Point reads on hash-sharded or globally-indexed keys are single-range.

What remains, and what breaks first:

- **Per range:** the executor's scan envelope on unindexed access paths (for one growing tenant with no useful index, the read path breaks first) and the group-commit latency floor.
- **Per tenant:** the TSO RPC path — a measured observation point at 10⁵–10⁶ grants/second, not a projected ceiling — and range 0's residual role in DDL and layout changes, both far above ordinary commit traffic.
  The TSO is also a liveness dependency: sharded-table transactions stall while range 0's writer recovers — bounded by range 0's own fence and replay rather than its host's full recovery, because a restarting multirange host binds its range transport before recovering and activates the oracle the moment range 0 is replayed, while SQL serving stays gated behind the full prologue.
- **Per fleet:** the configuration/reload pipeline around 10³ tenants per cell, then broker per-topic overhead around 10⁴ topics — which ranges multiply, making the named broker workstreams (follower-fetch multiplexing, per-topic metadata indexing) bind sooner in range-heavy deployments.
- **Gateway memory:** scatter-gather materializes working sets at the gateway, the same envelope as the single-node engine; pushdown narrows it but a general distributed exchange operator is explicitly out of scope.

Kafka-as-log is the last thing to break on every axis.

## 9. Related designs, briefly

The lineage is deliberate rather than novel-for-novelty's-sake.
The disposable-compute-over-shared-storage posture follows Neon's disaggregation, with a Kafka log replacing the safekeeper tier.
The transaction protocol is Percolator-class (as in TiDB): a central logical oracle, durable intents as locks, primary-record commit — chosen over Spanner's TrueTime because a per-tenant database needs no geo-scale clock infrastructure, and over HLC schemes because a fenced single oracle gives exact ordering where hybrid clocks give bounded uncertainty.
What is distinctive is the substrate reuse: Kafka's transactional-producer epochs serve as the fencing primitive for every single-writer role in the system, and the checkpoint object format doubles as the resharding mechanism, so the distributed database inherits its safety properties from machinery the broker already proves.

## 10. Conclusion

Gres scales writes horizontally by composing one primitive — a fenced single writer over a totally-ordered substrate log — at three granularities: the tenant, the range, and (through interval sharding and timestamp transactions) the single table.
Coordination is engineered out of the hot path rather than optimized within it: inserts allocate locally, commits decide on their primary range, reads carry their own consistency proof in a timestamp, and the only remaining central party hands out batched integers.
Every ceiling the architecture still has is named, measured in CI gates, and owned by a scheduled workstream — which is the difference between a scaling story and a scaling promise.

## References

The design documents behind this paper, for readers who want the decision-by-decision rationale and the alternatives considered:

- [Chapter design: a pure-Rust Postgres compute engine on the Crabka substrate](superpowers/specs/2026-07-09-crabka-gres-chapter-design.md) — scaling model and ceilings, architecture overview.
- [Substrate WAL design](superpowers/specs/2026-07-09-crabka-gres-g2-substrate-wal-design.md) — the committer seam, group commit, fencing.
- [Checkpoints design](superpowers/specs/2026-07-09-crabka-gres-g3-checkpoints-design.md) — manifest-last snapshots, truncation, the recovery model.
- [Multi-range tenants design](superpowers/specs/2026-07-09-crabka-gres-g7-multirange-design.md) — ranges, routing, the log-derived barrier.
- [Sharded tables design](superpowers/specs/2026-07-09-crabka-gres-g8-sharded-tables-design.md) — rowid-interval sharding, scatter-gather, checkpoint-fork splits.
- [Distributed maturity design](superpowers/specs/2026-07-09-crabka-gres-g9-distributed-maturity-design.md) — timestamp transactions, pushdown, hash sharding, indexes, the balancer, auto-sharding.
- [Early TSO activation design](superpowers/specs/2026-07-15-gres-early-tso-activation-design.md) — grant availability during host startup and failover.
