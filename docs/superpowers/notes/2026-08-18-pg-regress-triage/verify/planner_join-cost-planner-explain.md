# Verification: planner_join-cost-planner-explain

Verdict: root cause CONFIRMED, fix locations CONFIRMED (incomplete), attribution REASONABLE
(my count 10,900-12,800 vs 11,000 claimed), dependencies INCOMPLETE.

Scripts and per-statement JSON: analysis/verify/pjcost/ (align.py, cls.py, plancls.py, h.py).
Method: re-aligned oracle .out and Gres .out statement by statement against the PG 18.4 .sql
(/tmp/tmp.C8pPTxiDnA/postgresql-18.4/src/test/regress/sql/), classified each differing statement,
then split EXPLAIN mismatches by whether either side contains a node that only a cost planner
emits (Hash/Merge Join, Materialize, Memoize, Index/Bitmap/Tid scans, Gather, Incremental Sort,
Sort Method, cost=, actual rows ...). Statement-level costs undercount hunk-level by ~4 % (join:
7,665 vs 7,988).

## 1. Root cause
- join first failing statement L64 `SELECT * FROM J1_TBL t1 (a,b,c), J2_TBL t2 (d,e)`: same rows,
  J2 outer in PG, J1 outer in Gres. Not a cascade. join_relations_impl (join.rs:345) loops
  `for l in &left.rows` with a JoinIndex over the right side: FROM-order nested loop. Confirmed.
- explain.rs plan_table_expr (268-296) prints `Nested Loop` for every TableExpr::Join and drops
  the ON constraint and the join kind; plan_from (225-251) chains `Nested Loop` in FROM order;
  every table is `Seq Scan`. Confirmed.
- session.rs explain (5175-5217): `actual_rows = rows.len()` on the root result only. Confirmed.
- Cascade producers (join_hash: `update pg_class` -> "relation pg_catalog.pg_class does not
  exist" inside a transaction; memoize/explain/select_parallel: plpgsql SRF in the select list,
  routine.rs:2121/2609 "only supported in FROM position"; select_parallel also
  pg_stat_force_next_flush/pg_stat_database). Confirmed as the analyst says.
- The evidence quotes check out (join hunk 0, tidscan hunk 7 [same hunk also holds the
  WHERE CURRENT OF parser error], select hunk 12, incremental_sort hunk 0). memoize hunk 0 is a
  diff against the SRF ERROR, i.e. a producer, not the planner (analyst acknowledges).

## 2. Per-file classification (statement basis)
file             PLAN_COST PLAN_NOCOST ROWORDER  ERR/ERRTEXT/ROWS(other)
join                3398      1243      2054      970
join_hash             90         0         0      683
equivclass           314        10         0       27
subselect            906       378        59      308
memoize               46         0         0      244
incremental_sort     470         0         0      211
select_parallel       72         0         0      992
write_parallel        39         0         0       13
predicate            146        75         0        0
explain                0         0         0      661
tidscan               64         0         0      101
select_distinct      168         0         8        0
select_distinct_on    62        23         0        2
limit                 38        53         0       95
select                96        10        38       27
tuplesort             86        18         0      342
union                526        25         4      200
TOTAL               6521      1835      2163     4876   (=15,395; hunk basis 16,311)

Planner-only-matchable now visible: 6,521 + 2,163 = 8,684 (~9,050 hunk basis).
Cascade-hidden lines that will need the planner once the producer is fixed: join_hash ~600,
select_parallel ~900, memoize ~240, explain ~165 (only the parallel Gather Merge JSON block
541-696 and the generic_plan Bitmap plan; the other ~500 explain lines are EXPLAIN option/format
renderer: BUFFERS/WAL/MEMORY/SERIALIZE/SETTINGS sections, track_io_timing GUC, SRF filter
functions) -> ~1,900.
Revised: ~10,900 excluding PLAN_NOCOST (join removal, self-join elimination, subquery pull-up,
Result/One-Time Filter shapes: the sibling structural-transforms root), ~12,800 including it.
Analyst 11,000 is inside +-30 % either way. `explain` should not be listed as a planner file at
693 lines: ~165 of it are planner.

## 3. Things the analyst missed
a) int4_tbl poison. select_into.sql `INSERT INTO int4_tbl SELECT 1 INTO f;` (and
   `CREATE VIEW foo AS SELECT 1 INTO int4_tbl;`) succeed in Gres: pgparser parser.rs
   opt_select_into (12427) records the INTO target in parser state and only query_statement
   (12407) consumes it, so a nested SELECT INTO is silently ignored and the INSERT inserts row 1.
   Every later test sees int4_tbl with 6 rows: join L475 (41 lines: 36 rows where PG has 0 -- the
   Gres answer is right for the poisoned table), L1241/1242 full join, L1709, L3184, L3323, ...;
   also subselect, union, join_hash, polymorphism, with (~120-200 lines total, all counted as
   "wrong result"). Fix: parser must raise 0A000 "SELECT ... INTO is not allowed here" /
   "views must not contain SELECT INTO". Cross-file producer, small (S).
b) The executor is a whole-relation blocking evaluator (Relation{rows: Vec}, spawn_blocking
   worker in session.rs ~7958, scanner.rs:1139 memory budget). Per-node actual rows/loops,
   Rows Removed by Filter, Memoize Hits/Misses, Sort Method need a node-tree (iterator) executor
   with instrumentation, not just "join.rs runs the chosen method". This is bigger than the fix
   list suggests: exec.rs (39k lines) query evaluation must be re-hosted on plan nodes.
c) Un-analyzed relations: PG's plan for J1_TBL/J2_TBL/int4_tbl/int8_tbl depends on
   estimate_rel_size (relpages from heap size, tuple density from width). Gres has no pages
   (relstats.rs stores only reltuples/relhassubclass; relpages is a constant in exec.rs:20131).
   The STATS dependency must include page-geometry emulation or the row orders and join orders in
   join hunks 0-38 cannot match.
d) Writable pg_class (join_hash `update pg_class set reltuples/relpages`) and the planner honouring
   those manual values; `alter table ... set (parallel_workers)`.
e) Partial indexes are refused (exec.rs:2514 index_key_columns; DESC/NULLS FIRST keys too), so
   select's `Bitmap Index Scan on onek2_u2_prtl` cannot exist until create_index's partial index
   DDL works: INDEX-ACCESS-PATH must include partial-index storage + predicate implication.
f) SRF in select list (routine.rs) is the producer for memoize/explain/select_parallel; it is
   not in the dependency list.
g) plan_dist.rs already has a `Stats`/`PlannerConfig`/`JoinStrategy` (Broadcast/CoPartitioned/
   Gather) distributed pre-pass wired via scanner.rs with_join_planner (1187): "no planner exists"
   is slightly overstated; it is a distribution planner, not a PG cost planner, and a new
   crates/pgexec/src/planner/ must coexist with it.
h) Parallel plans: select_parallel prints `Workers Launched: 4` under EXPLAIN ANALYZE, so plan
   text alone is not enough there.

## 4. Fail-longer
After the planner lands, PLAN blocks still carry: typed deparse (`'B'::name`, `$N`), Join Filter
vs Filter placement, VERBOSE Output: lines, join removal / SJE / subquery pull-up (1,835 lines),
memory-budget errors (~9 join statements, tidscan ctid self-join), parser gaps in join
(`(a JOIN b) AS x(cols)`, `USING (i) AS x`, `ROW(x.*)`, WHERE CURRENT OF, `JOIN ... ON` after
LATERAL), information_schema types, `pg_stats` view. These are sibling roots; the analyst's note
already says they must land first or together.

## 5. Oracle facts
All spellings verified in self-check-serial results: `Workers Planned: N`, `Hits: 980  Misses: 20
Evictions: Zero  Overflows: 0  Memory Usage: NkB` (memoize, masked by explain_memoize),
`Sort Method: top-N heapsort  Memory: xxx`, `Index Searches: N`, `Heap Fetches: N`,
`Disabled: true`, `Hash Cond: (t1.ctid = t2.ctid)`, `Presorted Key: tenk1.four`.
