# Verification: planner_join-explain-utility-formats-options

Verdict: root cause PARTLY confirmed, fix location incomplete, attribution number
close but the framing of the largest file is wrong (it is a cascade), and the
dependency list misses two producer defects and several hidden prerequisites.

## 1. Root cause, file by file

### explain (693 lines) — CASCADE, not this root today
- Every `select explain_filter('explain ...')` in the file fails with
  `ERROR:  set-returning function explain_filter(text) is only supported in FROM position`
  (crates/pgexec/src/routine.rs:2121 `validate_plpgsql_scalar`, and :2609 for
  SQL-language SRFs). `explain_filter` is `returns setof text language plpgsql`
  called in the target list. That is producer defect #1 (SRF-in-select-list).
- The one call in FROM position (`from explain_filter(...) ln where ln ~ ...`)
  fails with `invalid regular expression: unrecognized escape sequence`: the
  body does `regexp_replace(ln, '-?\m\d+\M', 'N', 'g')`. Producer defect #2:
  crates/pgexec/src/regexp_fn.rs:472 `compile_pattern` hands the PG ARE
  pattern to the `regex` crate (1.13.1) unchanged; no translation of
  `\m \M \y \Y` (regex 1.10+ has `\b{start}` / `\b{end}` so the translation is
  small).
- Other non-EXPLAIN roots inside the file: `track_io_timing` /
  `compute_query_id` GUCs unknown (4 lines; GUC table in
  crates/pgexec/src/session.rs ~976-1360, `plan_cache_mode` and `jit` exist,
  these two do not); `create function pg_temp.mysin` -> `schema "pg_temp" does
  not exist` (1 + 8 lines); `jsonb #- text[]` operator missing (lexer/eval:
  `operator does not exist: jsonb # text`, 167-line block which is then a
  parallel Gather Merge plan = planner).
- After the two producers are fixed, "fails longer" classification of the 693:
  * ~482 land on THIS root (timings, Planning/Execution Time, JSON/YAML/XML
    field sets 51+58+43+63+20+53, Serialization 36, Memory 96, Settings 12,
    Query Identifier 24, ANALYZE+GENERIC_PLAN error 3, WindowAgg/Window:/
    Storage blocks 35+36 which additionally need a WindowAgg node the
    syntactic renderer does not have).
  * 12 would simply pass (plain `explain select` and `explain (buffers,
    format text)`: Gres `(cost=0.00..0.00 rows=0 width=0)` filters to N).
  * 186 planner-only (generic_plan Bitmap Heap Scan 9, gen_part Append 10,
    parallel JSON 167 which is also `#-`).
  * 4 GUC, 9 pg_temp.

### select_into (93 lines)
- 32 lines directly this root: EXPLAIN ANALYZE CTAS WITH DATA (9), WITH NO
  DATA (9), IF NOT EXISTS existing (7 + 7).
- 24 lines are `CREATE TABLE ... AS EXECUTE` = parser gap
  (crates/pgparser/src/parser.rs:7032 `create_table_as` calls `query_expr()`,
  AST `CreateTableAs.query: Box<QueryExpr>`), not EXPLAIN. After the parser
  fix, 14 of them fail longer on this root.
- Rest: ALTER DEFAULT PRIVILEGES parser (2 + cascade 6), SQL function whose
  final statement is CTAS (17), SELECT INTO placement errors (11).
- Hidden prerequisites exposed here: `ProjectSet` node for SRF-in-tlist
  (explain.rs has none), per-node actual rows (`render_text_node` prints 0
  for every non-root node by design), `(never executed)` for WITH NO DATA,
  0-row result for IF NOT EXISTS on an existing table.

### write_parallel (56 lines)
- 42 lines: EXPLAIN over CTAS / SELECT INTO / CREATE MATERIALIZED VIEW print
  `Result`. Dispatch is the first-visible defect, but the expected plans are
  Finalize HashAggregate / Gather / Partial HashAggregate / Parallel Seq Scan:
  0 of 42 can match without a parallel-aware planner. Co-requisite, not
  fixable by this root.
- 14 lines: `create table ... as execute prep_stmt` parser gap (+ abort
  cascade).

### subselect (37 lines of 1767)
- 4 `EXPLAIN (COSTS OFF) EXECUTE test(...)` blocks (9+7+9+12). After dispatch:
  VALUES-to-array rewrite with folded params, `One-Time Filter: false`
  constant folding, Hash Semi Join under force_generic_plan. 0 of 37 match
  without a planner.

### tidrangescan (9 lines of 94)
- 1 `EXPLAIN DECLARE c SCROLL CURSOR` block. After dispatch needs Tid Range
  Scan (planner scan choice) and `'(1,0)'::tid` literal typing. 0 of 9 match.

### tuplesort (20 lines of 502)
- 2 `EXPLAIN (COSTS OFF) DECLARE ... ORDER BY` blocks (10 each). After
  dispatch, `Sort / Sort Key / -> Seq Scan` IS what the syntactic renderer
  prints. Fully fixable by dispatch: 20 lines.

### select_parallel (20 lines of 1148)
- Both EXPLAIN EXECUTE blocks are inside a `current transaction is aborted`
  cascade whose producer is `select sp_test_func() order by 1` (SQL-language
  SRF in select list, routine.rs:2609). After that, parallel plans = planner.
  0 attributable.

## 2. Fix location
- crates/pgexec/src/explain.rs `plan_statement` (77), `utility_node_type`
  (114), `render_text_node` (871), `render_json/json_node` (907/921),
  `render_yaml/yaml_node` (972/978), `render_xml/xml_node` (1000/1011): exist,
  correctly named. Missing from the claim: `plan_select` (162) needs
  ProjectSet and WindowAgg nodes.
- crates/pgexec/src/session.rs `explain` is at 5176-5231 (claim says
  5169-5215; close). Dispatch to `self.prepared` (see `execute_sql` 4913),
  `declare_cursor` 4589, `run_create_table_as` 8277,
  `run_create_materialized_view` 8431.
- MISSED: crates/pgparser/src/parser.rs `explain_stmt` (6059-6130) and
  crates/pgparser/src/ast.rs `ExplainOptions` (1805): the parser accepts
  buffers/wal/timing/summary/settings/generic_plan/memory/serialize but
  DISCARDS them (`let _ = self.explain_option_flag()?`). The AST has only
  analyze/verbose/costs/format. Every option-driven output line and the
  ANALYZE+GENERIC_PLAN rejection need the AST to carry the options first.
- MISSED: crates/pgparser/src/parser.rs `create_table_as` (7001) for
  `CREATE TABLE ... AS EXECUTE` (separate parser root, 38 lines across
  select_into and write_parallel).

## 3. Attribution
Analyst: 690. Mine:
- first-visible-failure, whole-block: 140 (select_into 32, write_parallel 42,
  subselect 37, tidrangescan 9, tuplesort 20). Of these only 52 are matchable
  without a planner.
- fail-longer share of explain.out once SRF-in-select-list + `\m` regex are
  fixed: ~482.
- inclusive total 622 (analyst 690 is within 30%); planner-free total 534.

## 4. Dependencies / hidden prerequisites
- SRF in select list (plpgsql and SQL language) — routine.rs; gates all of
  explain.out and select_parallel.
- PG ARE escapes `\m \M \y \Y` in regexp_fn.rs `compile_pattern`.
- Parser: ExplainOptions fields; CTAS AS EXECUTE.
- GUCs `track_io_timing`, `compute_query_id`.
- plpgsql `CONTEXT:  PL/pgSQL function explain_filter(text) line 5 at FOR over
  EXECUTE statement` line — no CONTEXT emission exists in plpgsql.rs.
- ProjectSet / WindowAgg / Function Scan-with-alias nodes; per-node actual
  rows; `(never executed)`; Sort Method / Storage instrumentation lines;
  VERBOSE `Output:` lines and `public.`-qualified relation names (not in the
  renderer today).
- Planner for write_parallel (parallel), subselect (VtA + folding + Hash Semi
  Join), tidrangescan (Tid Range Scan), explain generic_plan/gen_part/parallel
  JSON.

## 5. Oracle facts
All stated behaviours check out against the oracle text in the diffs:
`ProjectSet (never executed)`, NOTICE + `(0 rows)`, JSON/YAML/XML key sets and
order, `Serialization: time=N.N ms  output=NkB  format=text|binary` (no
`time=` when TIMING OFF), `Memory: used=NkB  allocated=NkB`,
`Query Identifier: N`, `Window: w AS (PARTITION BY tenk1.ten)`,
`Storage: Memory|Disk  Maximum Storage: NkB`, GENERIC_PLAN error text with
CONTEXT. One nuance the claim omits: JSON `Actual Rows` is `N.N` (float) in
PG18 and `Disabled` sits after the cost/actual fields, before the buffer
fields.
