# Chapter Gres G-9: Distributed Maturity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the last per-table ceilings and operate the result: timestamp transactions (commit rate ~linear in ranges), pushdown execution, hash sharding, distributed indexes, and a goal-based auto-rebalancer.

**Architecture:** Per the [G-9 design](../specs/2026-07-09-crabka-gres-g9-distributed-maturity-design.md): range 0 becomes a batched monotone timestamp oracle (stride-ahead durable); sharded tables move wholesale to ts-visibility with durable intents and primary-range commit records (superseding their g-timeline path); a light planner seam adds equivalence-preserving pushdown and join strategies; hash sharding is a bucket key-prefix over the existing interval machinery; local then global indexes; a gres-balancer drives split/move/merge through the G-8b orchestrator.

**Tech Stack:** everything G-7/G-8 built, `crabka-rebalancer` as the goal-framework precedent, stateright, the scaling-demo pipeline.

## Global Constraints

- **Prerequisites:** G-8a for 9a/9b; G-8b for 9c/9e; G-9a plus the G-6 index breadth cycle for 9d's global half. This is the furthest plan from the tree: **every task re-verifies the seams it names at execution time; the landed code wins over this text and divergences are recorded in commit messages.**
- **The two invariants every task defends:** TSO grants are monotone across every crash and fence (stride-ahead is load-bearing; its absence must counterexample in the model), and no read ever observes an intent without resolving it through the primary (visibility is `commit_ts ≤ read_ts` — never a guess).
- **Supersession is clean:** when 9a lands, the G-8a g-timeline path for sharded tables is *deleted* (greenfield rule — no dual stack, no flag); unsharded tables and G-7 cross-range txns are untouched.
- **Every rewrite proves equivalence:** 9b/9c/9d query-path changes carry property tests against the unoptimized plan; the corpus-through-sharding gate runs continuously.
- Lints/format/commit/test conventions as in the G-2 plan; ported/extended model and suite names keep their lineage.

---

## Part 9a — Timestamp transactions (serial batches; the family's critical path)

### Task 1: The TSO — oracle task, stride durability, batched client

**Files:** Create `crates/gres-ranges/src/tso/{oracle,client}.rs`; extend `transport/protocol.rs` (`TsoRpc::Grant { count } → Granted { first_ts, count }`); the oracle runs on the range-0 writer's compute, persisting `max_ts` strides (`/0/meta/max_ts`, a new counter key with max-merge classification) through range 0's `SubstrateCommitter`.

Steps: TDD the stride logic (grants served from memory below the durable stride; crossing it blocks on one append; crash/fence recovery resumes past the stride — an integration test kills the oracle mid-stride and asserts the successor's first grant exceeds every prior grant); TDD client batching (one in-flight Grant amortizing concurrent requests, the group-commit idiom); Stateright model `tso_monotonicity_model.rs` with the stride-ahead teeth (no-stride variant must produce a regressed-grant counterexample across crash). Commit `feat(gres): the timestamp oracle`.

### Task 2: ts-versions, intents, and the prewrite/commit protocol

**Files:** Modify `crates/pgmvcc` (ts-stamped version encoding for sharded tables: intent flag, `start_ts`, `commit_ts`; visibility fn `satisfies_ts(read_ts, …)`), `crates/pgexec` (sharded-table write path: prewrite intents via the owning ranges' Committers, primary selection = first write, commit = write-once commit record @ commit_ts on the primary + async secondary resolution; first-committer-wins conflict check at prewrite), `crates/gres-ranges` (primary-resolution RPC `ResolveTxn { primary_range, start_ts } → Committed{ts}|Aborted|Pending`; reader-side bounded wait + push-abort through the silence machinery).

Steps: strict TDD, smallest slices — version codec round-trips; single-range ts-txn end-to-end (prewrite→commit→read at ts); conflict (two prewrites, first committer wins, loser aborts with 40001); cross-range ts-txn with primary crash before/after the commit record (secondaries resolve correctly both ways); reader push-abort of an abandoned txn. Extend the intent-lifecycle and primary-crash Stateright models per the spec. **Delete the G-8a g-timeline path for sharded tables in the same task** (supersession — router escalation, gsnap capture for sharded reads, and the sharded-read Range0Barrier call sites go; unsharded paths pinned byte-identical by existing tests). Commit `feat(gres): timestamp transactions for sharded tables`.

### Task 3: 9a proof — equivalence, bank/Elle, the un-flattened ceiling

**Files:** Re-target the sharded visibility-equivalence property suite to read_ts sweeps; re-run bank + Elle on sharded ts-tables under writer kills **and TSO fences**; extend the garbage horizon to resolved intents (G-3 checkpointer prunes intents whose txn is terminal below the horizon — with its own visibility-equivalence assertion); extend `scripts/gres-range-scaling.sh` — the commit-rate curve that plateaued at the G-8 decision ceiling must now scale ~linearly with ranges, published per PR with the old curve kept for contrast.

Commit `test(gres): ts-transaction gates — equivalence, Elle, linear commit scaling`.

---

## Part 9b — Distributed query optimization (parallel to Part 9a after G-8a; merge after Task 2 lands)

### Task 4: The planner seam + pushdown set

**Files:** Create `crates/pgexec/src/plan_dist.rs` (the pre-pass: per-scan pushdown decisions; join strategy selection), extend the `ScanRange` RPC (projection list, partial-agg spec, top-K), `crates/gres-ranges` remote execution of pushed fragments.

Steps: one rewrite at a time, each TDD'd with an equivalence property test (pushed ≡ unpushed over random data/predicates): predicates → projections → partial aggregates (COUNT/SUM/MIN/MAX/AVG-parts; GROUP BY partials with gateway merge) → ORDER BY+LIMIT top-K (K-way merge over interval-ordered streams). Join strategies behind a threshold config: broadcast-small-side, then co-partitioned (activated when 9c lands — the selection test gains that arm then), gather as fallback; golden tests pin selection. Stats inputs = sequence counters + checkpoint metadata via a `Stats` trait (fakeable). Corpus-through-sharding must stay at baseline with the planner enabled. Commit per rewrite; final `feat(gres): distributed pushdown execution`.

---

## Part 9c — Hash sharding (after G-8b)

### Task 5: Bucket-prefix sharding + co-location groups

**Files:** Modify `crates/pgparser` (`SHARDED BY HASH (col) BUCKETS n`), `crates/pgcatalog` (hash spec on the table record), `crates/pgkv`/`crates/pgmvcc` key encoding (bucket component for hash-sharded tables), `crates/gres-ranges` (router/planner equality-predicate → bucket routing; co-location groups in the layout; placement keeps corresponding bucket intervals together), `crates/gres-control` (spec + group in the registry/CRD).

Steps: TDD the encoding (order-preserving, bucket = leading component; verified against the interval machinery — a hash-sharded table passes the existing split/move/crash suites unchanged, which is the design's whole point and the task's main gate); bucket routing (equality → one range; range predicates → scatter, pinned); co-location integrity under splits/moves (property: group members' corresponding intervals always co-placed after any balancer-free sequence of operations); wire 9b's co-partitioned join arm + its selection and equivalence tests. Commit `feat(gres): hash sharding as bucket intervals`.

---

## Part 9d — Distributed indexes (local: with the G-6 index cycle; global: after 9a)

### Task 6: Local per-range indexes (lands inside/alongside the G-6 index breadth cycle)

**Files:** Per the G-6 index cycle's own design (which owns the index machinery); this task contributes the distributed dimension: index entries live in the owning range's key space and ride the row's atomic batch; per-range index scans slot under the `RangeScanner` (scatter narrows within ranges).

Steps: atomicity property (index and row never diverge across kill/replay — the G-2 disposability suite extended with an indexed table); planner picks index scans per range (equivalence + selection tests). Commit `feat(gres): range-local secondary indexes`.

### Task 7: Global indexes on ts-transactions

**Files:** `crates/pgexec` (index maintenance as additional intents in the same ts-txn; lookup path: index range point-read → base fetch), `crates/pgcatalog`/`pgparser` (GLOBAL index declaration + placement clause), `crates/gres-ranges` (index ranges in the layout; placement constraints for 9e).

Steps: TDD maintenance atomicity through the ts-txn (bank-style invariant: index-derived reads always equal base-table reads, under nemesis); lookup-path selection; placement constraint records. Commit `feat(gres): global secondary indexes`.

---

## Part 9e — The balancer (after G-8b; constraints enriched by 9c/9d)

### Task 8: Merge = inverse checkpoint-fork

**Files:** Extend `crates/gres-ranges/src/split.rs` (merge orchestration: checkpoint both adjacent ranges → map version unifying the interval → union restore (two filtered ingests) → park both predecessors → successor prologue), the split Stateright model (merge actions + the same crash-anywhere obligations), kill-at-every-step and merge-under-load suites.

Commit `feat(gres): range merges`.

### Task 8b: Online auto-shard conversion

**Files:** Extend `crates/gres-ranges/src/split.rs` (the conversion fork: pause → checkpoint-with-freeze-rewrite → catalog flag + map version in one range-0 commit → ts-table successors → resume), `crates/gres-substrate` (the freeze pass gains an xid→ts rewrite mode: frozen tuples emitted as ts-versions with a synthetic `commit_ts` below the conversion's read floor), `crates/pgexec`/`pgcatalog` (the flag flip path shared with `SET SHARDED`, which becomes a manual trigger of the same operation), the conversion Stateright model (racing writes / 2PC / fence — no acked loss, no mixed-visibility statement), a conversion equivalence test (converted table answers every query exactly as its unconverted control did), and the `ALTER TABLE … SET SHARDED` docs re-worded as "request now what policy would do later".

Commit `feat(gres): online auto-shard conversion`.

### Task 9: `crabka-gres-balancer`

**Files:** Create `crates/gres-balancer/` (internal-crate manifest; **verify the goal-framework shape against `crates/rebalancer` at execution time and mirror its idioms** — goals, plan, dry-run reporting); metrics aggregation into the registry (store size + checkpoint stats + commit rate + scan bytes — all already emitted, wired to records); goals: size ceiling/floor, load skew, **auto-shard conversion thresholds (Task 8b's operation; per-tenant/table disable knob)**, co-location integrity (9c), index placement (9d), compute anti-affinity; executor client calling the G-8b/Task-8/8b orchestrator under rate limits + cooldowns; CLI (`crabka gres balance [--dry-run]`) + operator knobs.

Steps: per-goal unit tests (violation → expected plan, whole-struct compares); dry-run parity (predicted plan == executed plan on a static fleet); the no-flapping property (oscillating load within hysteresis produces no plan); end-to-end under synthetic skew (balancer converges to goal satisfaction; every operation audited in the registry). Commit `feat(gres): goal-based auto-rebalancer`.

## Completion checklist (maps to the G-9 gates)

- TSO monotonicity model (with teeth) + intent/primary-crash models green; grants monotone across kill/fence in system tests (Tasks 1–2).
- Sharded ts-tables: visibility equivalence, bank, Elle green; the commit-rate scaling curve un-flattened and published (Task 3); the g-timeline sharded path deleted (Task 2).
- Every pushdown rewrite equivalence-proven; corpus-through-sharding at baseline with the planner on (Task 4).
- Hash-sharded tables pass the entire G-8 crash/nemesis corpus unchanged; equality routing + co-location integrity pinned (Task 5).
- Index/base divergence impossible under kill/replay (local) and nemesis (global) (Tasks 6–7).
- Merge in the split model; balancer dry-run parity + no-flapping + convergence under skew (Tasks 8–9).
