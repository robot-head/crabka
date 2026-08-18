# Verification: agg_window-user-function-scans

Verdict: root cause CONFIRMED (with a sharper diagnosis), fix location PARTLY WRONG,
attribution within tolerance (my count ~950 vs 930), dependencies INCOMPLETE.

## 1. Root cause (evidence from diffs + source)

First failing statement in rangefuncs (diff line 7) is
`select * from rngfunct(1) with ordinality as z(a,b,ord);` -> Gres 0A000
"WITH ORDINALITY over a user-defined function is not supported". Not a cascade;
`CREATE FUNCTION rngfunct` succeeded. gstest_data in groupingsets: CREATE succeeded
(gres .out line 24-30 identical), the 7 uses fail with "a column definition list is
required for functions returning record: gstest_data(integer)". Not a cascade.

What actually exists today (source read):
- exec.rs build_table_expr 17745-17779 (read path): a single-call user routine goes
  (a) to routine::eval_plpgsql_table_function (plpgsql only), else (b) refuses
  ORDINALITY (17765), refuses coldeflist (routine.rs 2928), and otherwise INLINES the
  SQL body by AST substitution (routine.rs expand_table_function 2817 ->
  substitute_in_query 2854). Column names come from routine.rs table_function_columns
  2941: RoutineResult::Type => vec![routine.name]  <-- this is why composite-returning
  functions print `rngfunc_sql | nextval`, `getrngfunc4 | rngfuncsubid | ...`,
  `rngfunct | f2` and why `rngfunct.rngfuncid`, `t.a` (rngfunc1), `i` (rngfunc_sql in
  LEFT JOIN ON) are "column does not exist". Explicit `RETURNS setof record` with OUT
  params is RoutineResult::Type{record} (routine.rs 716-719), so both
  table_function_columns (SQL) and plpgsql_table_function_schema 3020-3031 (plpgsql)
  ignore the OUT params and demand a coldeflist -> gstest_data, rngfunc1 defects.
  Non-SETOF composite (getrngfunc4) returns every row (no LIMIT 1 in
  expand_table_function) -> 2 rows instead of 1.
- exec.rs build_table_expr_schema_with_ctes 19576-19605 (describe path): knows plpgsql
  (plpgsql_table_function_schema) but has NO SQL-function branch; falls to
  srf::from_item_schema -> srf::plan -> "function X does not exist" + HINT. This path is
  what CREATE VIEW, EXPLAIN (session.rs 5176 explain -> describe_statement), and
  correlated-subquery planning (exec.rs resolve_select_subqueries 15546
  build_from_schema_described, error propagated when the WHERE holds a subquery) run.
  So the "does not exist" in views / EXPLAIN / `IN (select ... from rngfunct(...))` is
  the DESCRIBE twin, not build_table_expr. Analyst's "inlines only at top level" is
  imprecise: the read path handles any depth; the schema path handles none.
- ROWS FROM(udf, udf): routine::expands_as_table 2911 requires exactly one call, so
  multi-call items go to srf::from_item -> plan_all -> "does not exist" (both paths).
- Join operand: append_from_item 13548 / lateral_join 13745 call build_table_expr, so
  join operands already reach the same code. join.rs has nothing to change; the
  `LEFT JOIN rngfunc_sql(11,13) ON (r+i)<100` failure is the composite-naming defect.
- plpgsql non-SETOF in FROM (getrngfunc8 RETURNS int): plpgsql.rs
  execute_table_function 402-421 sets set_results=Some for every table request, and the
  Return arm at plpgsql.rs 1756-1760 errors when set_results.is_some() && value.is_some()
  -> "RETURN cannot have a parameter in a set-returning function". Fix there.
- plpgsql SETOF <composite> (rngfunc_mat returns setof rngfunc_rescan_t):
  plpgsql_table_function_schema is_record_type() treats a named composite as record and
  demands a coldeflist. Same function, needs composite attribute expansion.

## 2. Fix locations
Confirmed: exec.rs build_table_expr Function arms; routine.rs expands_as_table /
table_function_expansion / table_function_columns / plpgsql_table_function_schema /
eval_plpgsql_table_function / expand_table_function; srf.rs from_item / plan_all /
user_function_relation / qualify_columns.
Missing: exec.rs build_table_expr_schema_with_ctes 19576-19605 (+ lateral_schema_item
19326, from_column_names 14634 which swallows errors); plpgsql.rs
execute_table_function + Return arm; routine.rs Routine construction (716) or consumers
must treat RETURNS [setof] record + OUT params as OUT-defined shape.
Wrong: join.rs — no join-operand builder lives there.

## 3. Attribution (whole-block rule, my segmentation script)
rangefuncs total 1896. In scope of this root (UDF in FROM: ordinality, view, ROWS FROM,
subquery, join, coldeflist, composite/OUT naming, plpgsql-in-FROM shape, non-SETOF LIMIT):
 rngfunct top ordinality 20; vw_ord 23; rows-from + vw_ord 31; implicit-lateral naming
 10; lateral+ordinality 8; subselect 31; rngfunct.rngfuncid 7; getrngfunc1..7 254;
 getrngfunc8 (plpgsql) 24; rows-from mix + vw_rngfunc 46 (also needs %ROWTYPE);
 rescan rngfunc_sql/rngfunc_mat 303; rngfunc1 whole-row 12; array_to_set coldeflist 40;
 testrngfunc record coldeflist + "-- fail" text 21; testrngfunc composite results 32;
 coldeflist error cases 14; rngfuncbar OUT-row 12  => ~888.
groupingsets: 14+17+10+4+17 = 62 result blocks + 28 EXPLAIN blocks (planner-only once
gstest_data works) => 62 (+28 planner).
Direct total ~950 (analyst 930: within 30%).
NOT this root (in rangefuncs): LATERAL derived-table outer-ref binding 207
(`?column?`/"column r1 does not exist" with generate_series only); dup/OUT-mode parser
~101; insert_tt DML-body SQL functions + select-list SRF ~145; rngfuncr/rngfuncb scalar
record 20; testrngfunc scalar composite/select-list SRF ~69; EXPLAIN plans ~86 (planner);
users temp-table rowtype cascade ~115 (routine.rs resolve_type only checks public);
getrngfunc9 %ROWTYPE cascade 43 (pgparser plpgsql.rs supports %TYPE only); extractq2
whole-row-var args 60; unnest composite arrays 27; coalesce coldeflist 24; viewdef
unnest rendering 38; row_to_json(s.*) 9; FULL JOIN order 6; CREATE OR REPLACE checks 6.
Fail-longer once this root lands: getrngfunc9 (43) and users (115) blocks need this
root after their producers; gstest_data EXPLAINs (28) need planner; array_to_set /
testrngfunc EXPLAIN VERBOSE (22+63) need planner + inlining decision + Function Call
rendering; coldeflist "during inlining" CONTEXT needs PG's inline_set_returning_function
rule.

## 4. Dependencies missed
- agg_window-sql-function-executor: correct and load-bearing. SQL functions in FROM are
  AST substitution today: DML-RETURNING bodies refused ("final statement must be a
  query"), repeated volatile args refused (array_to_set STRICT case), params cannot
  reach nested queries/set-ops, whole-row params fail (extractq2 "missing FROM-clause
  entry for table t"). A real FunctionScan needs bound-parameter execution.
- Error position plumbing: PG's coldeflist errors carry LINE/caret at the function
  name; Gres has none for these.
- PG inlining rule (rule-based, not cost-based) for CONTEXT "during inlining" vs
  "statement 1", and for EXPLAIN Function Scan vs inlined plan.
- Not needed: planner (except EXPLAIN blocks), storage format, wire protocol.
- Small parser dependency: none for the FROM syntax itself (all shapes parse).

## 5. Oracle facts
Correct: 'a column definition list is required for functions returning "record"' with
LINE/caret (diff 1875-1877); 'return type mismatch ...' DETAIL 'Final statement returns
integer instead of point at column 1.' CONTEXT 'SQL function "array_to_set" statement 1'
(1890-1892). Incomplete: the non-STRICT repeat says CONTEXT 'SQL function "array_to_set"
during inlining' (1937-1939). Also: 'a column definition list is redundant for a function
returning a named composite type' / '... for a function with OUT parameters' / '... is
only allowed for functions returning "record"' (2225-2237).
