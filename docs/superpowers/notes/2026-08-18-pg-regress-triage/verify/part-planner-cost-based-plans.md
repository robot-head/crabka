# Verification: part-planner-cost-based-plans

Stance: refute. Result: root cause CONFIRMED, fix locations exist but two are
under-scoped, attribution within 2%, dependencies incomplete.

## Evidence read

- diffs: partition_join, partition_prune, partition_aggregate, inherit, indexing
- explain.rs: plan_statement (L77), plan_select (L162), plan_from (L225),
  scan_node (L298), render_text_node (L871). `plan_statement(&Statement)` takes
  the AST only. Every multi-item FROM and every TableExpr::Join prints
  `Nested Loop` + one `Join Filter`. Every table prints `Seq Scan on <name>`.
  ANALYZE: inner nodes print `actual rows=0.00 loops=1` (comment: "Gres has no
  per-node instrumentation"). `Statement::ExecuteStatement` falls to
  `utility_node_type` -> "Result".
- session.rs L5188-5206: `explain()` runs describe_statement then
  `crate::explain::plan_statement(statement)`; no catalog/plan input.
- join.rs L345 join_relations_impl, L550 JoinIndex::build: whole-Relation
  materializing join with an equi-key hash index (so the executor already
  hash-probes, only the plan text says Nested Loop). No merge join, memoize.
- relstats.rs: stores only reltuples + relhassubclass. No pg_statistic
  (n_distinct/MCV/histogram); PG_COMPAT_MATRIX L169 "Statistics themselves are
  never collected".
- exec.rs L13548 append_from_item: for comma joins the WHERE is pushed as the
  join constraint only when the WHOLE predicate resolves on the accumulated
  pair; a 3-way comma join therefore cross-products the first two tables and
  trips scanner::memory_budget_exceeded (53200) -- seen in partition_join
  ("blocking query exceeded the memory budget" on the plt1/plt2/plt1_e and
  pht1/pht2/pht1_e N-way joins).

## Recount (whole-block rule, blocks.py / costsig.py)

| file | total | PLAN blocks | GRES_ERR | of PLAN, cost-signal | analyst |
|---|---:|---:|---:|---:|---:|
| partition_join | 4199 | 3638 | 561 (expr partition keys cascade) | 3638 (all statements carry Hash/Merge/Index/Memoize) | 3613 |
| partition_aggregate | 1201 | 1102 | 99 (pagg_tab_m expr keys) | 1102 | 1076 |
| partition_prune | 4035 | 2866 | 1088 | ~500 strong (Index/Bitmap/Merge Append/Index Only); 435 weak (actual-rows/InitPlan/Subplans Removed/EXPLAIN EXECUTE only); 1962 pure Append expansion+prune | 519 |
| inherit | 2157 | 891 | 430 | 541 | 491 |
| indexing | 703 | 36 | 130 | 36 | 36 |

Revised total: 3638+1102+500+541+36 = 5817 (analyst 5735, +1.4%).

## Oracle facts

Confirmed in oracle .out: `(actual rows=N.NN loops=1)` two decimals, `Subplans
Removed: N` (41 in partition_prune), `(never executed)` (115), `Workers
Launched` (14), Gather/Parallel Seq Scan, Partial/Finalize, Merge Append,
Memoize (6 in partition_join), Materialize, One-Time Filter, `Hash Cond:`,
`Merge Cond:`, `Index Cond:`, `Recheck Cond:`. All GUC SETs
(enable_partitionwise_join, enable_hashjoin, parallel_setup_cost ...) are
accepted by Gres today (no "unrecognized configuration parameter").

## Corrections / hidden prerequisites

1. relstats.rs is not where statistics live; STATS is a new subsystem
   (ANALYZE sampler, pg_statistic storage, selectivity functions, pg_stats view).
2. Index READ path does not exist (only index maintenance on writes); Index
   Scan / Index Only Scan / Bitmap Heap+Index Scan need executor nodes.
3. Per-node instrumentation: executor materializes whole Relations; per-node
   `actual rows`/`loops`/`(never executed)` needs a node-based (or instrumented)
   executor. Bigger than join.rs.
4. Parallel plans: Gather/Workers Launched need a worker model (or faithful
   emulation) -- partition_prune 13 Gather, partition_aggregate 14 Gather.
5. EXPLAIN EXECUTE renders "Result" (explain.rs utility_node_type) -- non-cost;
   needed by every runtime-pruning block in partition_prune (~435 lines) which
   also need `Subplans Removed` (executor runtime pruning) and `$1` generic
   params.
6. Deparse of implicit constant coercions in Filter (`'100'::double
   precision`, `'a1$'::text`) is missing (inherit tuplesest / permtest blocks);
   same blocks, separate root; unblocked plans would still differ.
7. Fail-longer: after expression partition keys land, the N-way comma-join
   SELECTs in partition_join still fail with 53200 unless append_from_item
   pushes per-conjunct WHERE terms (or the planner orders the join).
8. Filter placement: PG puts single-relation quals on the scan (`Filter: (b =
   0)`); Gres prints them in `Join Filter` although the executor already
   pre-applies them (push_local_where). Deterministic, not cost-based, but
   inside the same blocks.
