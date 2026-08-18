# agg_window cluster triage (2026-08-17, main CI artifact)

Files: aggregates 1760, groupingsets 1919, window 2434, with 1755, create_aggregate 95,
polymorphism 832, rangefuncs 1896, functional_deps 70, tsrf 499, rowtypes 753 = 12,013 lines.
Whole-block attribution per hunk is in `attrib.py`; totals in `tally.json`.

## Totals by root (lines)

PLANNER 1577 | R8 sort-tie-order 1198 | R7 result-column-naming 1057 | R18 user-function-scans 930 |
R13 recursive-cte-shape+SEARCH/CYCLE 883 | R19 sql-function-executor 705 | R23 explain-window/projectset 631 |
R6 srf-projectset 508 | R15 table-rowtypes 468 | R20 named/default/VARIADIC call args 458 | R12 window-frame 409 |
R14 arrays-of-any-type 346 | R1 ordered-set aggs 283 | R4 outer-level aggs/refs 254 | R17 generic-operator lexing 238 |
R2 builtin support functions 219 | R30 RULES (non-goal) 174 | R32a DML-CTE forward refs 161 | R5 agg overloads 154 |
R34 self-ref FK 152 | R39 parser misc 140 | R29 missing catalogs 138 | R26 error positions 97 | R16 row comparison 97 |
R10 functional dependency 94 | R3 useragg definition fidelity 79 | R31 viewdef 76 | R9 grouped output order 73 |
R24 memory budget 64 | R21 plpgsql composite field assign 60 | R3b moving aggregates 60 | R40 bytea oid in views 51 |
R32b upsert/merge 48 | R11 grouping-sets semantics 47 | R35 pg_temp functions 28 | R33 dead subquery columns 20 |
R25 int4_tbl contamination 14 | R43 USING column identity 7 | R28 drop notices 6 | R27 index DDL 5 | R22 BEGIN WORK 4

## Per-file first failing statement / cascade

- aggregates: `SELECT any_value(v) FROM (VALUES (1),(2),(3)) AS v (v);` — not a cascade; local cascades:
  create_aggregate failures (builtin sfuncs, aggtype[]) poison newavg/aggfns/agg_view1 blocks; `begin work` parse
  failure runs a whole block outside a transaction; blocking-query memory budget aborts a txn block.
- groupingsets: `select a, b, grouping(a,b), sum(v), count(*), max(v) from gstest1 group by rollup (a,b);` (row order).
- window: `SELECT depname, empno, salary, sum(salary) OVER (PARTITION BY depname) FROM empsalary ORDER BY depname, salary;`
  (tie order 11/10). Not a cascade.
- with: `CREATE RECURSIVE VIEW nums (n) AS ...` (parse). Cascades: `department`/`tree` self-referential FK
  CREATE TABLE fails (152 lines); forward-referencing DML CTE leaves `y` different for the rest of the file (161);
  CREATE RULE (non-goal) 174.
- create_aggregate: `CREATE AGGREGATE newavg (sfunc = int4_avg_accum ...)` — builtin support fn not resolvable.
- polymorphism: `select polyf(point(3,4))` CONTEXT lines; not a cascade.
- rangefuncs: `select * from rngfunct(1) with ordinality as z(a,b,ord);` — not a cascade, but the file is dominated
  by the FROM-function path.
- functional_deps: `SELECT id, keywords, title, body, created FROM articles GROUP BY id;` — not a cascade.
- tsrf: `SELECT generate_series(1, generate_series(1, 3));` — not a cascade.
- rowtypes: `select (1.1,2.2)::complex, ...` (column name `row`), then `row('Joe','Blow')::fullname` (table rowtype).

## Roots (fix locations verified in source)

R1 ordered-set / hypothetical-set aggregates. Parser refuses `WITHIN GROUP` (parser.rs:14976 "ordered-set aggregates
are not supported"; the SELECT-side `within` is a plain syntax error). Needs: parser (`agg(direct) WITHIN GROUP
(ORDER BY ...) [FILTER]`), agg.rs (percentile_cont/disc scalar+array, mode, rank/dense_rank/percent_rank/cume_dist
hypothetical, direct-arg grouping rules, type unification errors, collation propagation), useragg.rs (CREATE AGGREGATE
`(args ORDER BY args)`, `hypothetical`, finalfunc_extra), viewdef deparse, \da argument spelling. Size L.

R2 builtin support functions as SQL-callable/aggregate-support routines: useragg.rs `lookup` uses `routines_named`
(user routines only) so `int4pl`, `int8inc`, `int8inc_any`, `int4_avg_accum`, `int8_avg`, `float8pl/mi`,
`float8_accum`, `float8_regr_accum`, `float8_combine`, `float8_regr_combine`, `numeric_avg_accum/serialize/
deserialize/combine`, `numeric_add`, `int4_sum`, `int4larger`, `booland_statefunc`, `boolor_statefunc`,
`array_larger`, `array_append` (as sfunc), `ordered_set_transition(_multi)`, `window_nth_value` are unknown.
builtin_procs.rs holds pg_proc rows but func.rs has no callable entries. Fix: useragg.rs lookup + func.rs. Size L.

R3 user-aggregate definition fidelity: validation messages (run-time type coercion, serial/deserial pairing,
`parallel` value, strictness match, inverse return type, unrecognized-attribute WARNINGs, stype-before-sfunc check
order), COMMENT ON AGGREGATE (exec.rs comment_ops only knows relations/columns), pg_aggregate columns,
transition-state sharing (NOTICE counts), DROP FUNCTION CASCADE dropping dependent aggregate, polymorphic
consistency in `accepts` (myaggp12a must fail), variadic aggregates. Fix useragg.rs build/lookup/accepts,
exec.rs comment_ops. Size M.

R3b moving-aggregate execution (msfunc/minvfunc/mstype/minitcond) in window frames: useragg.rs records them as
`unimplemented`; window.rs recomputes each frame with sfunc. Size M.

R4 outer-level aggregates and outer references: aggregates whose args reference only outer levels (in scalar
subqueries, HAVING EXISTS, FILTER, ARRAY(...)), nested-aggregate detection with positions, GROUPING() at outer
level, correlated subqueries as grouping-set keys, correlated subqueries in window-function args
(`lead(ten,(SELECT ... s.unique2 ...))`). agg.rs func_in_scalar_context_error/validate_grouped, scope.rs,
subquery.rs, grouping.rs, window.rs. Size L.

R5 aggregate overload coverage: agg.rs `AggFunc` is a closed enum with hand-typed args: any_value missing;
string_agg(varchar,...) with DISTINCT fails; string_agg(bytea, unknown literal '') and NULL delimiter wrong;
bit_and/or/xor(bit); avg/sum(interval); count() and `f() OVER ()` messages. Size M.

R6 SRF ProjectSet semantics: srf.rs:1467/1587/1709 refuse SRFs with GROUP BY/aggregates, nested SRF args,
DISTINCT ON; SRF in GROUP BY, window PARTITION BY SRF, error contexts (CASE/COALESCE/LIMIT/UPDATE/window args/FROM
nesting), user-defined SRFs in select list. Size L.

R7 result column naming (exec.rs named_expr_inner ~24916): scalar subquery -> inner name, ARRAY(...) -> "array",
CASE -> "case", ROW(...) -> "row", outer/lateral column refs keep their name (currently "?column?"), function-scan
alias/composite naming (`getrngfunc1(1) AS t1` -> t1; setof composite -> composite columns). Size S; 1057 lines.

R8 sort tie-order fidelity: window.rs `execute` emits rows in input order after evaluating calls; PostgreSQL emits
the last WindowAgg's sort order and its in-memory qsort (unstable) fixes tie order; ORDER BY sorts (exec.rs
24179-24216 sort_by) are stable in Gres. Port pg_qsort (src/port/qsort.c) as the row sorter and emit window rows
in window-sort order. Size M; 1198 lines in window.

R9 grouped output order without ORDER BY: aggregates with DISTINCT/ORDER BY force sorted GroupAggregate;
single-chain ROLLUP emits sorted with subtotals after each group. agg.rs/grouping.rs emission. Size M.

R10 functional dependency in GROUP BY (agg.rs validate_grouped:1161 checks only structural match; must accept
columns of a table whose PK columns are all grouped), CREATE VIEW must run the check, view->constraint dependency
so DROP CONSTRAINT RESTRICT fails with the "other objects depend on it" text. Size M.

R11 grouping-sets semantics (grouping.rs): equal grouping columns kept separate by position; hash/sort
capability check ("could not implement GROUP BY" DETAIL) ; misc. Size M.

R12 window frames/spec: infinite-interval RANGE offsets, timetz in_range, negative-offset check order,
`unbounded` param-name precedence and `unbounded(1)`/`unbounded.x` in frame bounds (parser.rs frame_bound:2138),
named WINDOW over GROUP BY resolution ("column two does not exist"), LANGUAGE internal WINDOW functions with named
args, error text/positions for window contexts. window.rs resolved_frame:1354, parser.rs. Size M.

R13 recursive CTE shape and SEARCH/CYCLE (cte.rs split_recursive_terms:294 refuses nested WITH; SEARCH/CYCLE
refused at :338/:524; eager recursion overflows on infinite CTE + LIMIT; recursive-reference validation messages;
forward references; CREATE RECURSIVE VIEW; type-mismatch message spelling). Depends on R14 for SEARCH/CYCLE path
columns. Size L.

R14 arrays of any element type: pgtypes datum.rs `ElemType` closed enum (no record/composite/point/user types).
Blocks aggtype[], ARRAY[ROW(..)], array_agg(record), SEARCH/CYCLE. Size XL (cross-cluster).

R15 table rowtypes as types (temp table `fullname`, `users`, `int8_tbl`, `compos`, `tt2`), whole-row args to
functions, `f.last`<->`last(f)`, `$1.id`. usertype.rs/routine.rs resolve_type. Size L.

R16 row comparison semantics: ROW = / <> with NULLs (Gres returns NULL), opfamily operators, ANY/ALL row
subqueries, dissimilar-type messages, IS NULL on composite. eval.rs/rowexpr.rs. Size M.

R17 generic operator lexing: lexer.rs `punctuation` is a fixed table; `~<~ ~<=~ *= *< |@|` fail. Size M.

R18 user-defined function scans: exec.rs build_table_expr:17740 (`expands_as_table` inlines SQL functions;
ordinality refused; other contexts fall to srf::from_item and report "does not exist"), routine.rs
table_function_expansion:2924/eval_plpgsql_table_function:3052, srf.rs from_item:1185/user_function_relation:1357.
Needs one FunctionScan relation builder (SQL+plpgsql, subquery/view/join/ROWS FROM/ORDINALITY/coldeflist, OUT-param
setof record, RETURNS TABLE column names, whole-row). Size L.

R19 SQL-function executor: routine.rs inlines only (module doc); needs value-bound execution (volatile args once),
DML RETURNING final statement, SETOF/record in select list, `returns <rowtype>`, CREATE OR REPLACE checks + HINTs,
inlining CONTEXT/QUERY lines, polymorphic resolution completeness (anycompatible*, unknown-literal errors). Size XL.

R20 named/default/VARIADIC call arguments: parser.rs positional_from_named:3010 resolves only make_interval;
`variadic array[...]` and mode-after-name declarations unparsed. Size M.

R21 plpgsql composite field assignment on unassigned record var (plpgsql.rs:1392). Size S.
R22 `BEGIN WORK` (parser.rs begin():6628). Size S.
R23 EXPLAIN WindowAgg/Window:/Run Condition/ProjectSet/grouping keys (explain.rs plan_select:162 has none). Size M;
~50% of these lines also need cost decisions.
R24 blocking-query memory budget (scanner.rs BLOCKING_QUERY_MEMORY 16MiB; harness 20MiB) too small for 4x tenk1
UNION ALL and tenk1 UNION tenk2. Size S.
R25 int4_tbl contamination from select_into (`INSERT INTO int4_tbl SELECT 1 INTO f` must error 42601): parser.rs
opt_select_into/finish_query_statement. Size S; affects aggregates/with/polymorphism.
R26 error LINE/caret/HINT/DETAIL/CONTEXT lines. Size M (cross-cluster).
R27 index DDL (DESC keys, partial indexes, record_image_ops opclass) cross-cluster.
R28 DROP cascade NOTICE lines (inheritance children), temp-view NOTICE.
R29 catalogs: UPDATE pg_class, pg_depend, pg_statistic, pg_stats, information_schema._pg_expandarray.
R30 CREATE RULE (matrix non-goal).
R31 viewdef deparse: `i(i)` function alias, literal typing (`'@ 1 day'::interval`), GROUPING uppercase,
`select *` in CTE, USING op NULLS LAST.
R32a DML CTE forward references (cte.rs evaluation_order); R32b ON CONFLICT multi-col SET subselect, EXCLUDED
typing, MERGE correlated target.
R33 unreferenced subquery output columns must not be evaluated (t_sub scalar subquery error).
R34 self-referential FK in CREATE TABLE. R35 CREATE FUNCTION pg_temp.f. R39 parser misc: `s.*`/`t.*` inside
expressions (ROW(v.a,s1.*), row_to_json(s.*), merge_source_cte.*::text), `((subquery)) q`, `$1.id`,
`name OUT type` arg mode order. R40 exec.rs column_type_from_oid:24973 lacks BYTEA (view with bytea column
fails "unknown query field type oid 17"). R43 USING-join merged column identity in GROUP BY validation.

## Planner-only estimates
aggregates 706, groupingsets 685, window ~300 (of 595 explain lines), with 125, rowtypes 61, rangefuncs 20,
tsrf 0 (ProjectSet/One-Time Filter deterministic but needs VERBOSE Output), others 0.
