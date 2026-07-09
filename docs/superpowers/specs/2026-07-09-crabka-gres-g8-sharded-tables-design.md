# Gres G-8: Sharded tables — design

**Date:** 2026-07-09
**Status:** Approved
**Type:** Slice design, and the chapter's first substantially net-new distributed-systems design: the donor never built this (its "D4" comments are the only trace). Splits one **table** across ranges — rowid-level boundaries, cross-range statement execution, and online splits — lifting the last per-table ceiling G-7 leaves standing. Structured **G-8a** (global-visibility tables + scatter-gather execution) → **G-8b** (online splits and moves) → **G-8c** (named research follow-on: unbounded commit rate). Depends on all of G-7.

## Context — why this is genuinely new, and what it stands on

G-7's inherited wall is visibility, not routing. The donor rejects cross-range statements (`0A000`) because its MVCC snapshots are **per-range**: tuples carry range-local xids decided in that range's clog, and a reader on range A has no way to order range B's local commits against its own snapshot — there is no shared timeline. The donor's cross-range *transactions* dodge this precisely because their tuples carry `Prepared(Li→g)` markers deferring visibility to **range 0's global clog** under a **global snapshot** (`durable_global_snapshot`: g-space xmin/xmax/xip read from range-0 state behind the barrier). That machinery — global xids, the write-once global clog, gsnap capture, `effective_global_xid` fencing, first-committer-wins — is proven, ported in G-7, and is exactly the shared timeline a sharded table needs. What G-8 adds is the decision to make sharded tables live **entirely** on that timeline, plus the execution and split machinery around it. The substrate contributes one unplanned gift: G-3 checkpoints are key-sorted part objects with manifests, which makes **split-by-checkpoint-fork** (both sides restore filtered views of the same immutable objects) the natural split mechanism — no row-copying pipeline required.

## Design Goals

- **One table, N ranges:** storage, scan bandwidth, and ingest scale linearly with ranges for a sharded table; queries against it are correct (snapshot-isolated, first-committer-wins) without statement-shape restrictions.
- **Splits are online and boring:** splitting or moving a range is a bounded-pause operation built from checkpoint machinery that already exists, safe against every 2PC/recovery interleaving (model-checked).
- **Unsharded tables pay nothing:** the local-xid fast path, the conformance baseline, and every G-2…G-7 property are untouched for tables that never opt in.
- **The commit-rate ceiling is stated, not discovered:** G-8a's design has a named aggregate-commit ceiling (range-0 decision throughput, batched); the design to remove it is scoped as research, not promised.

## Non-goals

- **Unbounded single-table commit rate** — G-8c research (below); G-8a ships the batched-decision ceiling honestly.
- **Distributed query optimization** — v1 execution is scatter-gather-then-local (correctness first); filter/partial-aggregate pushdown is a named optimization track, not a gate.
- **Hash sharding, secondary-index-aware placement** — interval sharding on rowid only (the key space is already interval-ordered); revisit alongside the index breadth cycle.
- **Auto-rebalancing policy** — G-8b ships the split/move *mechanism* plus size/load triggers with conservative defaults; placement optimization is operational tuning.

## Architecture Overview

```
CREATE TABLE t (…) SHARDED            — or ALTER TABLE t SET SHARDED (one-way)
  ⇒ t is a global-visibility table: every write to t is g-stamped

boundaries: (table_id, rowid) points — RangeMap v2; sharded t spans ranges
  r1=[t,0..b1) r2=[t,b1..b2) …; each range allocates rowids from its own
  interval (insert placement round-robins/least-loaded across t's ranges)

write path (any statement writing t):
  txn escalates to global on first sharded-table write (leased g-blocks make
  allocation local-cheap); row batches carry Prepared(Li→g) exactly as G-7
  cross-range txns do; COMMIT = one batched decision append on range 0

read path (any statement reading t):
  gsnap capture behind the Range0Barrier (G-7 machinery, unchanged)
  scan_live(t) → RangeScanner seam:
    local ranges: local scan under gsnap
    remote ranges: ScanRange RPC — the OWNING range evaluates visibility
      under the caller's gsnap and returns visible rows (rowid-ordered);
      concatenation across interval-ordered ranges = the ordered scan
  gathered rows feed the existing local executor (joins/aggs/sorts unchanged)
  ⇒ 0A000 is lifted for statements whose every table is global-visibility

split (G-8b): checkpoint r → write RangeMap v(n+1) via range 0 (barrier-gated
  readers) → both sides restore FILTERED views of r's checkpoint objects +
  filtered tail replay → fence r's topic (park, generation-bump) → sides open
  fresh topics; in-doubt Prepared markers are inherited by key-interval side
```

## Key Design Decisions

### Sharded tables are global-visibility tables — the one timeline that already exists

The crux is cross-range consistent reads, and the design refuses to invent a second clock. Every write to a sharded table is stamped with a global xid and decided in range 0's global clog — the G-7 cross-range-transaction representation applied uniformly, so a remote range can evaluate any sharded-table tuple's visibility against the caller's gsnap with no access to foreign local-clog state (local `Li` appears only under a `Prepared(Li→g)` it can deref, or frozen). Two costs follow and are engineered down rather than hidden. *Allocation:* per-txn range-0 round trips for g would be absurd, so gateways lease **g-blocks** (the GTM hands out `[g, g+K)` per lease as one range-0 append; SP23's lift-only reseed extends to "past every leased block", and unused block tails are abandoned — g-space is sparse and that is fine, the models never assume density). *Decisions:* every sharded-table txn's commit is one small range-0 clog record; range 0's writer **batches decisions in its ordinary group commit**, so the ceiling is decisions-per-batch × group rate — order tens of thousands of commits/s per tenant, stated in the envelope below. The rejected alternative — range-homed decisions for single-range txns — removes that ceiling but breaks gsnap capture (in-progress g's would be scattered across every range's state with no coherent snapshot source); it is exactly the G-8c research problem, not a v1 compromise to half-take.

### Scatter-gather at the scan seam; the executor above it does not change

The engine's one storage read path for rows is the table scan; G-8a introduces a `RangeScanner` seam exactly there. For a sharded table, the scan fans out per covering range: local ranges scan locally; remote ranges serve a `ScanRange { table, interval, gsnap, filter? }` RPC on the ported transport, where the **owning** range evaluates MVCC visibility under the caller's gsnap and streams back visible rows in rowid order — concatenation across interval-ordered ranges reconstructs the ordered scan the executor expects, and everything above (joins, aggregates, sorts, the materializing tree-walk) runs unchanged on the gateway. This preserves the engine's semantics wholesale at the cost of gathering working sets to the gateway — the same memory envelope the single-node engine already has, so sharding v1 buys **write throughput, storage, and scan bandwidth**, not reduced per-query memory; filter pushdown rides the RPC's optional predicate from day one (evaluated remotely when present), and partial-aggregate pushdown is the named optimization track. With gsnap-scoped visibility available everywhere, the router's `0A000` wall drops for any statement whose tables are all global-visibility (mixed statements — sharded joined with unsharded-remote — keep the wall until the unsharded table is co-ranged or converted).

### Rowid intervals make placement free and inserts local

Boundaries are `(table_id, rowid)` points (RangeMap v2 — the donor's descriptor format explicitly reserved room). Each of a sharded table's ranges allocates rowids **from its own interval** via its existing per-range sequence machinery, so an INSERT is routed by placement policy (round-robin/least-loaded across the table's ranges), lands on one range, allocates locally, and never coordinates — ingest scales linearly. UPDATE/DELETE route by predicate through the scan (scatter identifies victims on their owning ranges; writes execute there; a statement touching multiple ranges is simply a global txn, which every sharded-table txn already is). Interval sharding also keeps split semantics trivial: a split point is a rowid, and every key belongs to exactly one side.

### Splits are checkpoint forks (G-8b)

A split of range r at rowid b: force a checkpoint of r (G-3, with its garbage horizon); commit RangeMap v(n+1) through range 0 — the map blob becomes **versioned and mutable**, and because every map read already sits behind the Range0Barrier on fresh-snapshot statements, readers converge without new machinery (the donor's own comment demanded exactly this upgrade); the two successor ranges **restore filtered views of the same immutable checkpoint objects** (part files are key-sorted — each side ingests only its interval) plus filtered replay of r's tail; r's topic is parked (generation bump, the G-5 mechanism) and each side opens a fresh topic with fresh epochs. Writes to r pause from the checkpoint's covered offset until the sides open — a bounded window dominated by tail length, kept small by pre-split checkpointing. In-doubt `Prepared` markers split by key interval with the rows they govern; each side's bring-up prologue re-derives locks and runs settle-complete independently, and the G-8b Stateright model exhaustively checks split racing 2PC (a decision landing mid-split must be honored by both sides), split racing recovery, and cascading split-then-fence — the hairiest model in the chapter, named as such. A **move** is the degenerate split (one empty side) targeting a different compute.

### G-8c — the honest research line

Unbounded single-table *commit* rate requires decisions that do not serialize through one range's group commit: distributed decision logs with coherent snapshots (per-range decision homes + a snapshot protocol), or a timestamp-ordering redesign (HLC/Percolator-class) replacing xid snapshots outright. Both are engine-architecture changes, not slices; G-8c exists as a named research chapter with its trigger condition (tenants sustaining near the batched-decision ceiling), so the roadmap neither promises it nor forgets it. *(Resolved: the research line closed as **[G-9a](2026-07-09-crabka-gres-g9-distributed-maturity-design.md)** — the Percolator-class direction, with range 0 as a batched monotone timestamp oracle and decisions as durable intents/commit records on primary ranges, superseding this spec's g-timeline for sharded tables once it lands.)*

## The envelope (stated, chapter-bound)

A sharded table on N ranges: ingest, storage, and scan bandwidth ~linear in N; per-statement write latency unchanged (one group cycle on the owning range + the commit decision cycle); aggregate commit rate bounded by range-0's batched decision throughput (order 10⁴/s per tenant — decisions are ~decade-byte records amortized across group commits); gateway memory per query bounded by the gathered working set (unchanged from the engine's single-node behavior). Reads of hot single rows scale with ranges only insofar as placement spreads them — there are still no indexes until that breadth cycle lands, and a full-scan-per-point-read costs a scatter per statement; the G-6 breadth order (constraints → **indexes** → windows) is unchanged and indexes matter *more* here, noted explicitly.

## Integration

- **`crates/pgexec`:** the `RangeScanner` seam at `scan_live`; global-visibility table flag in table metadata (catalog schema grows one field); write-path escalation for sharded tables; gsnap plumbed into scans.
- **`crates/gres-ranges`:** RangeMap v2 (`(table_id, rowid)` boundaries, versioned blob + barrier-gated reload), `ScanRange` RPC on the existing transport, g-block leasing in the coordinator, the split orchestrator, the new models.
- **`crates/gres-substrate` / `gres-control` / operator / CLI:** filtered restore (interval-scoped ingestion from checkpoint parts), split/park choreography, layout mutations in the registry + CRD (`ALTER TABLE … SET SHARDED`, `crabka gres split|move`), split triggers (size via checkpoint stats, load via commit-rate metrics; conservative defaults, manual override first).
- **`crates/pgparser`:** `SHARDED` in CREATE/ALTER TABLE.

## Kafka / wire compliance

Nothing new on the Kafka wire: more topics (per split), stock produce/fetch/ListOffsets, parking via the G-5 mechanism. All new protocol surface (ScanRange, split orchestration) rides the tenant-internal transport.

## Testing

- **Visibility equivalence (the G-8a core gate):** property tests — for random histories over a sharded table, gsnap-scoped scatter-gather returns exactly what a single-range table with the same history returns; conformance corpus green against a database whose corpus tables are `SHARDED` across 2 ranges (this is the corpus-through-sharding gate G-7 could not have — global visibility makes it meaningful), with the unsharded baseline untouched.
- **The ported 2PC suites re-run with sharded tables** (every txn global): bank conservation, Elle list-append on a sharded table across writer kills.
- **Split model (G-8b gate):** exhaustive over split × 2PC × recovery × fence interleavings — no lost or double-honored decision, no in-doubt marker stranded on the wrong side, both sides' folds equal the pre-split fold partitioned by interval.
- **Split system tests:** split under live load (bounded write-pause measured and asserted), kill-anywhere during split (either the old range serves or both sides do — never neither, never both-for-one-key), move-under-nemesis.
- **The scaling demo extended:** single sharded table, N ranges — ingest ~N×, and the commit-rate ceiling measured and published against its stated envelope.

## Risks

- **This is the chapter's deepest water** — mitigated by strict staging (G-8a has no map mutation; G-8b has no new visibility semantics), by reusing proven G-7 invariants for everything durable, and by the corpus-through-sharding gate catching semantic drift the models abstract away.
- **Range-0 decision ceiling** — stated in the envelope, measured in the demo, G-8c triggered by evidence.
- **Gateway gather memory on huge scans** — the engine's existing behavior, but sharding invites bigger tables; the pushdown track and the (index) breadth cycle are the levers, named not promised.
- **Split write-pause under hot load** — bounded by tail length; pre-split checkpoint + threshold triggers keep tails short; the pause is measured in the split system tests with an asserted ceiling.
- **g-space sparsity from abandoned lease blocks** — harmless to correctness (models assume no density), but monotone-growth of g values is bounded-checked (u64 headroom vs realistic lease burn is astronomically safe; asserted once in a test to make the arithmetic reviewable).

## Resolved decisions

- Visibility: sharded tables are global-visibility tables on the existing g-timeline; no second clock; gsnap + Range0Barrier scope every read; 0A000 lifted for all-global-visibility statements.
- Allocation/decisions: leased g-blocks; batched range-0 decisions; the ceiling stated (~10⁴ commits/s/tenant) with G-8c as the evidence-triggered research follow-on.
- Execution: `RangeScanner` scatter-gather with owning-range visibility evaluation and optional filter pushdown; the executor above the seam unchanged.
- Sharding: rowid-interval boundaries (RangeMap v2, versioned, barrier-gated); per-interval rowid allocation; insert placement round-robin/least-loaded.
- Splits: checkpoint forks with filtered restore + parked predecessor topics; moves as degenerate splits; the split model is the gate.
- Staging: G-8a (visibility + execution) → G-8b (splits/moves) → G-8c (research).
