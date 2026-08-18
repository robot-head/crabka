# Verification: part-explain-append-prune

Verifier stance: refute. Result: root cause CONFIRMED, fix locations CONFIRMED with
one upstream correction, attribution within ~10%, dependencies incomplete.

## Method

`split_stmts.py` splits each diff into per-statement change blocks (whole-block
rule; file headers excluded). Totals match `grep -c '^[-+]'` minus 2:
partition_prune 4035, inherit 2157, partition_join 4199, partition_aggregate 1201.
Per-block dumps: `<test>.blocks.txt`; per-block JSON: `<test>.stmts.json`.

## 1. Root cause

partition_prune: first hunk is line 29, `explain (costs off) select * from lp;`
after every `create table ... partition of lp` succeeded. Expected `Append` +
six `Seq Scan on lp_<x> lp_n`; Gres `Seq Scan on lp`. Not a cascade. The
next ~200 EXPLAINs fail the same way. Confirmed.

Cascades inside the file that are NOT this root:
- `create table mc3p ... partition by range (a, abs(b), c)` -> Gres
  `ERROR: expression partition keys are not supported` (also iboolpart
  `((not a))`, coll_pruning_multi `substr(a,1) collate ...`, second mc3p,
  and inherit's mcrparted). 589 lines in partition_prune. Producer =
  part-expression-partition-keys; once fixed those statements need THIS root
  ("fail longer").
- `select explain_parallel_append(...)` / `select explain_analyze(...)`:
  Gres `set-returning function ... is only supported in FROM position`
  (plpgsql SETOF in select list). 371 lines. Once fixed they need
  Parallel Append / Gather / Bitmap Heap Scan / Materialize -> PLANNER.
- hp / hp_prefix_test use `part_test_int4_ops` / `part_test_text_ops`
  (custom hash opclasses from test_setup.sql). Gres parses `opclass` on
  `PartitionKeyElem` (pgparser/src/ast.rs:2402) but partition/hash.rs never
  consults it, so rows route differently (block "select tableoid::regclass,*
  from hp": expected hp1/hp2/hp3/hp2, Gres hp0/hp1/hp1/hp3). 36 lines are
  pure routing; 230 more EXPLAIN lines need this root AND the custom hash.

## 2. Fix locations (all exist)

- crates/pgexec/src/explain.rs: `plan_statement(statement: &Statement)`
  (line 77) takes ONLY the AST — no `Kv`, no scope. `scan_node` (298) prints
  `Seq Scan on <name>`; `Statement::Update` (92-101) prints `Update on
  <table>` + one scan, no per-leaf `Update on leaf parent_n` lines;
  `Expr::StringLiteral => '..'::text` hardcoded (627); `Expr::ScalarSubquery
  => "(SubPlan)"` (812); `render_text_node` (871-903) instruments only the
  root (`rows = if root { actual_rows } else { 0 }`). Correction: the plan
  builder must gain catalog + scope access (`eval::infer_type(expr, scope)`
  at eval.rs:4154 exists for typed constants), so the signature/entry point
  changes, not just the internals.
- crates/pgexec/src/session.rs `explain()` 5176-5231: calls
  `describe_statement` then `plan_statement(statement)`; `Statement::
  ExecuteStatement` falls to `utility_node_type` -> "Result". `execute_sql`
  (4913) shows the prepared statement + params are available on
  `self.prepared`. Confirmed.
- crates/pgexec/src/partition.rs: `partitions_of` (479) sorts by NAME;
  `leaves_of` (537) name order via `descendants`; `contains`/`compare_range_
  tuple`/`route` (798-893) are the reusable bound comparators. PostgreSQL
  order is PartitionDesc order (list: first appearance in sorted datums,
  null-only partition after, default last; range: lower bound; hash:
  modulus/remainder). Confirmed the scan-order dependency.
- crates/pgexec/src/exec.rs `partitioned_scan` (17100): appends every leaf
  from `leaves_of`, no pruning, no per-leaf filter, no per-leaf counters.
  Confirmed.

## 3. Attribution (partition_prune, whole-block rule)

| category | lines |
|---|---|
| APPEND_PRUNE (this root, incl. typed deparse, EXECUTE, runtime prune) | 1962 |
| APPEND_PRUNE + custom hash opclass (hp, hp_prefix_test) | 230 |
| EXPR_KEYS cascade | 589 |
| PLANNER (index/bitmap scans, Merge Append, MixedAggregate, asptab) | 521 |
| SRF-in-select-list -> then PLANNER parallel/bitmap | 371 |
| MERGE ... USING x JOIN y parse error | 103 |
| inherits() redeclared-column "specified more than once" (inh_lp) | 61 |
| PREPARE of UPDATE on auto-updatable view | 57 |
| hash partitioning on array unsupported | 39 |
| UNION ALL subquery pull-up + One-Time Filter on constant arm | 39 |
| custom hash opclass routing rows | 36 |
| `===` user operator parse | 14 |
| partition scan order (result rows) | 6 |
| partitioned index check misfire (`create index on part_abc(a)` then sub-partition) | 4 |
| ATTACH PARTITION column-order mismatch not rejected | 3 |

This root: 1962 (+230 with the hash dependency) vs analyst 2351 -> 7-17% low.
inherit: list_parted 61 + range_list_parted 118 + inhpar Update/Append 44 =
223 clean; the `where false -> Result / One-Time Filter: false` blocks (26,
28, 40 = 37 lines) are const-folding of a false qual, not pruning; block 19
needs an Index Scan (PLANNER). partition_join 25: `a = 1 AND a = 2`
EquivalenceClass contradiction -> dummy rel, not partprune. partition_
aggregate: block 9 (`c = 'x'`, 13) is all-pruned; block 7 (`1 = 2`, 8) is
const-fold; MixedAggregate (18) is a cost-based grouping-sets choice.
Revised total for THIS root: ~2192 + 223 + 13 = ~2430 (analyst 2690).

## 4. Dependencies missed

- Per-node EXPLAIN ANALYZE instrumentation (actual rows/loops per child,
  `(never executed)`, `Rows Removed by Filter`). Today only the root row
  count is measured (explain.rs 887-891); Nested Loop children print 0.00.
- Typed / const-folded deparse is cross-cutting: `'a'::bpchar`, `= ANY
  ('{..}'::int[])`, `(b)::text`, `'Tue Feb 01 00:00:00 2000 PST'::timestamp
  with time zone`, `LOCALTIMESTAMP`, `(InitPlan 1).col1`, `InitPlan 1 ->
  Result` sub-nodes, EC-derived equality clauses printed AFTER other quals
  (`(a <= 1) AND (c >= 0) AND (b = 1)`), flat AND lists. Shared with every
  EXPLAIN file; should be its own root that this one depends on.
- GUCs: `plan_cache_mode = force_generic_plan` (line 23 of the file) so
  EXPLAIN EXECUTE prints `$n`; `enable_partition_pruning = off` must keep
  Append but stop pruning; `constraint_exclusion = 'partition'` must
  exclude inheritance children by CHECK (inh_lp) -> needs
  relation_excluded_by_constraints / predicate_refuted_by.
- Custom hash opclass dispatch to SQL functions (part_hashint4_noop,
  part_hashtext_length) for hp/hp_prefix_test (266 lines) and hash_part/
  insert/alter_table.
- Prepared-statement `$n` binding for EXPLAIN (ANALYZE) EXECUTE: run with
  values, print with `$n`.
- Executor must actually prune / append per leaf (per-leaf actual rows).

## 5. Oracle facts checked

- Survivor-only alias numbering: `lp_bc lp_1, lp_default lp_2` (static);
  `ab_a2_b1 ab_1` after `Subplans Removed: 6` (run-time). Confirmed.
- Single survivor elides Append: `Seq Scan on lp_ad lp`. Confirmed.
- All-pruned static -> `Result / One-Time Filter: false` (pp_arrpart
  a='{1, 2}'); all-pruned at run time -> `Append / Subplans Removed: 2`
  with no children (execute q1 (0,0)). Analyst stated only the first.
- InitPlan-driven pruning -> `(never executed)` (listp a = (select null::int)).
- UPDATE: `Update on pp_arrpart / Update on pp_arrpart1 pp_arrpart_1 / ->
  Seq Scan on pp_arrpart1 pp_arrpart_1`. VERBOSE `Seq Scan on public.part_p1
  p_1 / Output: p_1.x, p_1.b`. Confirmed.
- "quals ordered by cost (IS NULL before =)": the visible rule is also
  "EquivalenceClass equalities last" (rp_prefix_test2 shows `(a <= 1) AND
  (c >= 0) AND (b = 1)` with equal costs). Incomplete, not wrong.
