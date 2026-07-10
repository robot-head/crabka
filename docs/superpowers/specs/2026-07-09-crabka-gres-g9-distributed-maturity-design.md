# Gres G-9: Distributed maturity — design

**Date:** 2026-07-09
**Status:** Approved
**Type:** Slice-family design. Resolves the frontier G-8 deliberately left open: **G-9a** timestamp transactions (unbounded single-table commit rate — this supersedes and closes the "G-8c research" line), **G-9b** distributed query optimization, **G-9c** hash sharding, **G-9d** distributed secondary indexes with placement, **G-9e** the auto-rebalancer. Two decisions were user-resolved in this cycle: the Percolator-class TSO direction over HLC, and ts-visibility **superseding** the g-timeline for sharded tables (greenfield-clean, no dual stack).

## Context — what this stands on

G-8a's honest ceiling is structural: every sharded-table commit is one decision record through range 0's single writer, because the g-snapshot (`xmin/xmax/xip` over g-space) needs one coherent capture source. The rest of the machinery this family needs already exists and is verified: per-range totally-ordered durable WALs with epoch fencing and fence-first recovery (G-2/G-7), durable-or-rederivable in-doubt disciplines with eight ported Stateright models (G-7), interval sharding with versioned barrier-gated maps and checkpoint-fork splits (G-8), block leasing through range 0 (G-8a), key-sorted checkpoint parts enabling filtered restore (G-3), a goal-based rebalancer precedent in-repo (`crabka-rebalancer`, Cruise-Control-style), and a scan seam (`RangeScanner`) whose RPC already carries an optional predicate. G-9 is the family that spends those assets.

## Design Goals

- **No per-table ceilings left:** a sharded table's ingest, storage, scan bandwidth, *and commit rate* all scale with ranges; the only remaining envelope numbers are per-range and per-statement, stated as such.
- **One mechanism per concern:** ts-visibility is *the* sharded-table transaction mechanism (supersession, not coexistence); hash sharding is interval sharding under a bucket prefix, not a second layout engine; the balancer drives the one split/move mechanism G-8b built.
- **Optimization without an optimizer:** pushdown and join strategy come from a light planner seam with cheap stats — correctness-preserving rewrites, never a cost-model gamble.
- **The correctness corpus grows, never resets:** every G-7/G-8 model and suite either still applies or is extended; new semantics (intents, TSO monotonicity, merges) get their own models in the same discipline.

## Non-goals

- **Shuffle/exchange distributed execution (DAG engines)** — broadcast and co-partitioned joins only; a general exchange operator is a different program.
- **Serializable isolation (SSI)** — the bar remains SI + first-committer-wins, single-key linearizable, Elle-validated; SSI is a future breadth question for the engine, not a G-9 deliverable.
- **Online bucket-count resharding** — bucket counts are fixed at table creation in G-9c (splits move bucket *boundaries*; changing `BUCKETS n` is a rebuild, named for later).
- **Cross-tenant balancing** — G-9e balances ranges within a tenant across its computes; fleet-level placement stays the operator's existing concern.

## Architecture Overview

```
G-9a — timestamp transactions (sharded tables)
  range 0 = TSO only: monotone ts batches served from memory over the transport;
    durability = stride-ahead advance of max_ts in range-0's WAL (crash ⇒ resume
    past the stride; monotonicity preserved; grants batched ⇒ ~10⁵–10⁶ ts/s)
  write: intents (locks) durable in each participant range's own WAL
         primary range chosen per txn (first write); COMMIT =
         one commit record @ commit_ts on the PRIMARY range (+ async secondaries)
  read:  read_ts from the TSO; version visible iff commit_ts ≤ read_ts;
         an intent with start_ts ≤ read_ts resolves via its primary
         (committed@ts / aborted / pending → bounded wait, then push-abort
          through the existing silence/settle machinery)
  ⇒ decision throughput scales with ranges; the Range0Barrier disappears from
    sharded-table reads (a timestamp is its own consistency proof)

G-9b — light planner seam over the RangeScanner:
  predicate + projection pushdown; partial aggregates; per-range top-K merge;
  joins: broadcast-small-side | co-partitioned (same hash spec) | gather (fallback)

G-9c — hash sharding = interval sharding under a bucket prefix:
  key = (table, bucket=hash(col) mod n, rowid); a range owns bucket intervals;
  equality on the hash column routes to one bucket; co-location groups align
  tables sharing a spec (feeds 9b co-partitioned joins)

G-9d — indexes: LOCAL per-range first (entries in the owning range's batches,
  atomic for free; gated on the G-6 index cycle) → GLOBAL (entries sharded by
  indexed key; maintenance is a cross-range ts-txn — routine after 9a);
  placement constraints co/anti-locate index ranges vs table ranges

G-9e — gres-balancer: goals (size ceiling, load skew, co-location, anti-affinity)
  over per-range metrics → split/move/MERGE plans → G-8b orchestrator,
  dry-run + rate limits + cooldowns; merge = inverse checkpoint-fork
```

## Key Design Decisions

### G-9a: the oracle grants time; ranges own their decisions

The Percolator-class shape was chosen over HLC/decentralized because it *extends* the proven stack instead of re-founding it: range 0 remains the single monotone authority it already is — but for **timestamps, not decisions**. A timestamp grant needs no per-grant durability (the oracle durably advances `max_ts` in large strides through its ordinary WAL and serves batches from memory; after a crash or fence the successor resumes past the stride — grants stay monotone across generations because the stride is on the log and recovery is fence-first), so batching removes the throughput ceiling without breaking the real-time ordering that leased *blocks* would have broken (one oracle serving all grants preserves cross-session read-your-writes; that hazard is named in the spec because G-8a's block-leasing intuition does NOT transfer to timestamps). **Monotonicity is not enough — the oracle must also prove it is still the oracle** *(panel amendment C6: a fenced-but-alive deposed oracle serving from memory would hand out stale-yet-monotone `read_ts`, silently breaking read-your-writes with the barrier gone)*: grant-serving is gated on **epoch liveness** — the oracle amortizes a fenced heartbeat (a no-op produce on its transactional session, every grant batch or every configured interval, whichever first) and stops granting the instant a heartbeat fences; the staleness window is the heartbeat interval, stated as a config with a default well under any human-visible bound, and the model corpus gains a live-zombie freshness action (a deposed oracle keeps serving — the invariant "no granted read_ts precedes a commit acknowledged before the grant" must fail without the liveness gate and hold with it). Commits become range-local: a transaction's writes land as **durable intents** in each participant range's WAL (an intent is the lock — strictly stronger than the donor's in-memory locks, and the fence/settle disciplines carry over with less rederivation, since intents replay); the **primary range** (first write) holds the single write-once commit record at `commit_ts`; secondaries are resolved asynchronously and lazily by readers. Visibility is a comparison, not a set: `commit_ts ≤ read_ts`, with intent resolution through the primary (bounded wait, then push-abort riding the existing silence-sweeper/settle machinery and its models). The `Range0Barrier` vanishes from sharded-table reads entirely — a timestamp carries its own consistency — which also deletes G-7's hottest read-path cost for these tables. Isolation: SI + first-committer-wins (write-write conflicts detected at prewrite via intents), single-key linearizable, the same externally validated bar. **Supersession:** sharded tables move to ts-visibility wholesale (the G-8a g-timeline path for them is removed, greenfield-clean); the g-timeline remains exactly where G-7 put it — cross-range transactions over *unsharded* tables — and unifying that remaining island onto ts-txns is a named simplification follow-up, not a G-9 gate.

### G-9b: rewrites, stats-cheap, planner-light

A logical pre-pass over statements touching sharded tables decides, per scan: pushed predicates (row-local, deterministic — the RPC's existing field, made systematic), pushed projections, partial aggregation (per-range partials for COUNT/SUM/MIN/MAX/AVG-parts and GROUP BY partials merged at the gateway), and per-range top-K for ORDER BY+LIMIT (K-way merge on interval-ordered streams). Join strategy is a three-way choice with cheap inputs — estimated cardinalities from sequence counters and checkpoint stats: **co-partitioned** (both sides share a hash spec and co-location group ⇒ join executes per-range remotely), **broadcast** (one side under a size threshold ⇒ ship it to each range), else **gather** (G-8a behavior, always correct). Every rewrite is equivalence-preserving and property-tested against the unpushed plan; there is no cost model to be wrong, only thresholds to be tuned.

### G-9c: buckets are a key prefix, so everything already works

`SHARDED BY HASH (col) BUCKETS n` (n a power of two, fixed at creation) encodes as `(table_id, bucket, rowid)` — a bucket is just the leading component of the interval key space, so **G-8b's map, splits, moves, models, filtered restore, and the balancer all apply unchanged**; a "hash-sharded table" is an interval-sharded table whose intervals the system, not the rowid sequence, distributes. The router/planner extracts equality predicates on the hash column to route point statements to one bucket's range; range predicates on the hash column degrade to scatter (documented). Tables declaring the same spec join a **co-location group**: placement keeps corresponding bucket intervals on the same computes, which is what makes G-9b's co-partitioned join a local operation.

### G-9d: local indexes are free; global indexes are ts-transactions

Local (per-range) secondary indexes — entries keyed within the owning range, maintained in the same atomic batch as the row — add zero new distributed semantics and land as soon as the G-6 index cycle gives the engine index machinery at all; queries on a non-shard key become per-range index scans instead of per-range table scans (scatter narrows *within* ranges first). Global indexes — the index itself sharded by indexed key, giving true single-range point lookups on secondary keys — make every base-table write a cross-range write, which is precisely the routine case after G-9a (the index entry is one more intent in the same ts-txn; the primary-range commit covers both). Placement awareness is a constraint vocabulary for G-9e, not new machinery: co-locate (a global index range near the table ranges it most references) or anti-locate (spread hot index ranges), declared per index with defaults from observed access correlation.

### Auto-sharding: conversion is a policy outcome, not a DDL act

*(Added after the compatibility review, user-resolved: a NORMAL PostgreSQL application — plain `CREATE TABLE`, no vendor syntax — must scale unbounded without ever learning Crabka DDL.)* `SHARDED` is demoted from gate to **hint**: it pre-shards at creation for tenants that know their shape, but the system converts unsharded tables **automatically** when they cross policy thresholds. The mechanic composes three things this family already builds: when the balancer's conversion goal fires (size/commit-rate thresholds on an unsharded table's range share), the table undergoes an **online conversion fork** — a brief write pause (the split pause, same bound), a checkpoint whose freeze pass rewrites the table's committed history from xid-versions to ts-versions (frozen tuples are visible-to-all, so they carry a synthetic `commit_ts` below every future `read_ts`; aborted/in-flight-xmin versions are dropped, not converted — only *committed-below-horizon* tuples freeze, exactly the G-3 rewrite rules), the catalog flag flips in the same map-version commit **only after the settle-complete precondition holds** — no in-doubt `Prepared` marker may straddle the conversion, the same gate splits use, so the pause both drains local transactions and refuses to start while a cross-range commit on the table is in-doubt *(precondition added after the PR panel review — the original drain argument covered local txns only)*, and the table is thenceforth an ordinary ts-table that splits like any other. Conversion is one-way (as `SET SHARDED` already was), idempotent, and crash-anywhere safe by the same argument as splits (map-version commit is the atomic instant; either the old unsharded range serves or the conversion completes deterministically from checkpoint + tail). The G-8 pin "unsharded tables pay nothing, byte-identical" survives with one amendment to its wording: *until converted* — and conversion is observable, logged, rate-limited, and disableable per tenant/table (the policy knob for users who want the explicit-DDL world). A dedicated conversion model joins the corpus: conversion racing writes, racing 2PC on the same table, racing a fence — no acked write lost, no read ever mixing visibility classes mid-statement.

### G-9e: goals in, plans out, one executor

`crabka-gres-balancer` follows the in-repo rebalancer's shape: per-range metrics already emitted (store size and checkpoint stats from G-3, commit rate from the writer, scan bytes from the scanner) aggregate into the registry; a goal list evaluates them — size ceiling (split), size floor (merge), load-skew bound (move), **conversion thresholds (auto-shard an unsharded table, per the decision above)**, co-location-group integrity (9c), index placement constraints (9d), compute anti-affinity — and emits a bounded plan of split/move/merge/convert operations executed serially through the G-8b orchestrator under rate limits, cooldowns, and a dry-run mode that publishes the plan without acting (the rebalancer's operational idiom). **Merge** is designed here as the inverse checkpoint-fork: checkpoint both adjacent ranges, commit the map version that unifies the interval, restore the union (two filtered ingests into one store), park both predecessor topics, open the successor — the same crash-anywhere obligations as split, added to the split model rather than modeled separately.

## The envelope after G-9 (chapter-bound)

Per sharded table: ingest, storage, scan, and commit rate ~linear in ranges; per-statement write latency = one prewrite group cycle on each touched range + one commit-record cycle on the primary (two group cycles typical); TSO throughput ~10⁵–10⁶ grants/s per tenant (batched), with grant latency one east-west RPC amortized across a batch. Point reads on hash keys or globally-indexed keys: single-range. What remains per-range: the executor's scan envelope between indexes (G-6 cycle) and the group-commit latency floor. What remains per-tenant: the TSO RPC path (a scaling *observation point*, not a projected ceiling — measured in the gates) and range-0's role in DDL/layout, both far above the decision traffic that used to bind.

## Integration

- **`crates/gres-ranges`:** TSO service (oracle task on the range-0 writer + client batching), intent/commit-record codecs and the primary-resolution protocol on the existing transport, the planner seam, bucket routing, co-location groups, merge orchestration.
- **`crates/pgexec`:** ts-visibility evaluation for sharded tables (a second, simpler visibility path: comparison + intent hook), prewrite/commit txn shape at the session seams G-7/G-8 already use, local-index maintenance hooks (with the G-6 cycle), planner pre-pass entry.
- **`crates/pgmvcc`:** ts-stamped version encoding for sharded tables (intent flag, start_ts/commit_ts), coexisting with xid encoding for unsharded tables (two version classes, one per table class — mirrors the supersession decision).
- **`crates/pgparser`/`pgcatalog`:** `SHARDED BY HASH (col) BUCKETS n`, index placement clauses, spec/co-location metadata.
- **New `crates/gres-balancer`** (`publish = false`): goals, plan, dry-run, executor client; operator/CLI surfaces (`crabka gres balance --dry-run`, balancer CR knobs).
- **Chapter docs:** the G-8 spec's "G-8c research" section is closed by reference to this design; the scaling model's ceiling paragraph is superseded (amended alongside this spec).

## Kafka / wire compliance

Nothing new on the Kafka wire in any sub-slice: TSO grants, intent resolution, scans, and balancer plans all ride the tenant-internal transport; durability remains ordinary transactional produce per range; splits/moves/merges use existing topic create/park machinery.

## Testing

- **G-9a models (the family's core obligation):** extend the ported corpus with — TSO monotonicity across stride-crash/fence (grants never regress; the *teeth* variant without stride-ahead must counterexample); intent lifecycle (no committed read ever observes a missing-primary state; push-abort races decision write-once); primary-crash recovery (secondaries resolve deterministically); first-committer-wins at prewrite. Plus the visibility-equivalence property suite re-targeted: sharded ts-tables vs a single-range control, equal under every read_ts.
- **G-9a system gates:** bank + Elle on sharded ts-tables under writer kills and TSO fences; the scaling demo's commit-rate curve — the G-8 ceiling measurement must visibly *un-flatten* (linear commit scaling to N ranges), published per PR.
- **G-9b:** per-rewrite equivalence property tests (pushed vs unpushed plans identical results); join-strategy selection golden tests; corpus-through-sharding stays at baseline with the planner on.
- **G-9c:** bucket routing (equality→one range) pinned; co-location integrity under splits/moves; hash-spec tables through the whole G-8 crash/nemesis suite.
- **G-9d:** local-index maintenance atomicity (index and row never diverge across kill/replay); global-index txns through bank/Elle; lookup-path selection tests.
- **G-9e:** goal-evaluation unit tests (each goal, each violation → expected plan); merge added to the split model; balancer end-to-end under synthetic skew with dry-run parity (the plan predicted equals the plan executed); no-flapping property (cooldowns respected under oscillating load).

## Risks

- **Two version/visibility classes in the engine** (xid for unsharded, ts for sharded) — the deliberate cost of supersession-without-migrating-G-7; contained by class-per-table-kind with no mixing, and by the named follow-up to unify the last g-island onto ts-txns.
- **TSO as a liveness (not throughput) dependency** — sharded-table txns stall if range 0's writer is down; bounded by the same recovery story as any range (fence + prologue), measured in the cold-path suites; grant batching keeps the hot path off it for all but one RPC per batch.
- **Intent garbage from abandoned transactions** — lazily resolved by readers and swept by the silence machinery; the G-3 garbage horizon extends to prune resolved intents (an explicit G-9a task, not an afterthought).
- **Planner-seam scope creep** — the seam's contract is rewrites-with-equivalence-proofs only; anything needing a cost model is out by construction and reviews enforce it.
- **Balancer-induced churn** — dry-run-first rollout, rate limits, cooldowns, and the no-flapping property test; the balancer can only ever call the same audited orchestrator operators an operator could call by hand.

## Resolved decisions

- G-9a: Percolator-class (user-resolved) — range-0 TSO with batched monotone grants + stride-ahead durability; decisions as write-once commit records on primary ranges; durable intents as locks; visibility = `commit_ts ≤ read_ts` + primary resolution; SI + FCW bar unchanged; **supersedes** the g-timeline for sharded tables (user-resolved); Range0Barrier removed from sharded reads.
- G-9b: light planner seam; predicate/projection/partial-agg/top-K pushdown; co-partitioned > broadcast > gather; stats from sequences + checkpoint metadata; no cost model.
- G-9c: hash = bucket-prefix interval sharding; fixed power-of-two buckets; equality routing; co-location groups.
- G-9d: local indexes first (with the G-6 cycle), global indexes on ts-txns after G-9a; placement as balancer constraints.
- G-9e: goal-based balancer over G-8b's orchestrator; merge = inverse checkpoint-fork, folded into the split model; dry-run, rate limits, cooldowns.
- Auto-sharding (user-resolved): `SHARDED` demoted to a hint; the balancer's conversion goal auto-converts unsharded tables past thresholds via the online conversion fork (freeze-rewrite to ts-versions inside a checkpoint fork; one-way; disableable; its own model) — plain `CREATE TABLE` scales without vendor DDL.
- Ordering: G-8a→9a; 9b after 8a (parallel to 9a); 9c after 8b; 9d after 9a + the G-6 index cycle; 9e after 8b, enriched by 9c/9d.
