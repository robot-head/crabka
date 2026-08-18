# Verification: agg_window-sql-function-executor (rangefuncs, polymorphism)

Verdict: root partly confirmed, fix location incomplete, attribution too high, dependencies missing.

## Method
- Split each diff into per-statement change blocks (scripts stmts.py / attrib_*.py in this dir).
- rangefuncs = 1896 changed lines, polymorphism = 832 (both match file_stats.json).

## rangefuncs attribution (whole-block rule)
| cat | lines | root |
|---|---|---|
| B | 492 | SQL routine in FROM is unknown to the schema/describe path (CREATE VIEW, IN-subselect scope, EXPLAIN) and to ROWS FROM(...) with >1 call: exec.rs `build_table_expr_schema_with_ctes` (~19585) only handles plpgsql via `plpgsql_table_function_schema`, else falls to `srf::from_item_schema` -> "function X does not exist"; routine.rs `expands_as_table` (2911) demands a single call. NOT the claim's root. |
| A | ~381 | claim root (see breakdown) |
| I | 207 | LATERAL derived table `SELECT r1, *` scope bug (column "r1" does not exist / ?column?) |
| C | 172 | WITH ORDINALITY over user routine refused (exec.rs 17765) |
| E | 163 | FROM-position column model: `RETURNS composite` columns named after the function (`table_function_columns` 2941 returns vec![routine.name] for RoutineResult::Type), scalar alias `AS t1` not applied, non-SETOF composite returns all rows, `t.a`/whole-row of function alias |
| F | 104 | plpgsql: `%ROWTYPE` parse, RETURN-in-setof misclassification, plpgsql RETURNS setof composite -> "column definition list is required" |
| H | 100 | user SRF in select list refused (inline_scalar_call 2609) |
| D | 88 | column definition list on user routine refused (table_function_expansion 2925) + coldeflist redundancy errors |
| S | 60 | table-rowtype parameter / whole-row var argument (`extractq2(int8_tbl)`, `substitute` 2377 does not resolve `t.q2` param field access) |
| L | 27 | arrays of composite type unsupported |
| N | 26 | viewdef rendering of unnest/ROWS FROM |
| R | 25 | coalesce(...) AS c(...) coldeflist over non-SRF expr |
| J | 16 (+~130 hidden behind B/A) | EXPLAIN VERBOSE plans (Function Scan / inlined plan / const-folded Result) |
| G | 13 | parser: `s.*` as func arg, `f2 out anyelement` param order (3 lines direct, gates 95 A-lines), CREATE RULE |
| Q | 6 | FULL JOIN row order |

A breakdown (rangefuncs): OR REPLACE/OUT-type checks 6; rngfuncr/rngfuncb record-in-scalar 20; dup block 95 (gated by parser G: `name mode type` order); insert_tt block 115 (29 more lines are H; 76 of the 115 are `select * from tt` cascades that need H too); testrngfunc record/composite in scalar 18; users temp-table rowtype 115 (only 28 direct; 87 fail longer on E/C/B/K once RETURNS resolves); rngfuncbar OUT split 12.

## polymorphism attribution
| cat | lines | root |
|---|---|---|
| M | 260 | named arguments (`:=` parse error; `=>` -> parser.rs 3025 "does not support named arguments here") |
| V | 203 | VARIADIC: `a variadic int[]` (name-mode order, parser), `VARIADIC` in call args (parser), variadic resolution |
| E | 108 | scalar/OUT function in FROM with alias: `x` resolves to record `(12)` instead of the column; OUT column names in FROM |
| A | 66 | inlining CONTEXT/QUERY (6), polyf anycompatiblerange+anycompatible coercion (14: `implicitly_coercible` lacks int2->int4, float4->float8), polyf(null)/unknown-literal errors (20+12), coerce args to resolved common type (14: `array[$1] || $2` int[]||numeric[]) |
| A' | 44 | definition checks in build_routine: duplicate param names, OUT default, rename HINT, and their cascades |
| P | 56 | polymorphic aggregates |
| O | 41 | error rendering (LINE/HINT/DETAIL) |
| L | 35 | arrays of point/record |
| K | 28 | pg_statistic/pg_stats/array_in/anyrange_in |
| H | 12 | `(dfunc(...)).*` RETURNS TABLE in select list |

## Totals for the claim
Core A ≈ 381 + 110 = ~490 (analyst 705). Strict (exclude parser-gated dup block and fail-longer users cascade) ≈ 310. Liberal (add SRF-in-select-list H 112) ≈ 600.

## Fix location check
- routine.rs symbols exist at the named lines (inline_scalar 1825, final_query 2211, callable 2272, bound_args 2347, substitute 2377, check_replaceable 875, build_routine 677, resolve_call 1128, polymorphic_* 1233-1354). Confirmed.
- Missing: `implicitly_coercible` (1535) is the actual polyf defect; `resolve_type` (468) is public-only -> `RETURNS users` (temp table) fails; the runtime value path already exists for plpgsql (`eval_plpgsql_scalar_with` 1990 -> `ScalarFunctionRequest` -> session.rs `drive_scalar_worker` 8027 -> plpgsql.rs `execute_scalar_function` 269 which substitutes values and calls `session.run_one`) - the SQL-language executor must plug in there (routine.rs gate + session.rs dispatch + new SQL-body executor), and the FROM path at exec.rs 17751 (`eval_plpgsql_table_function` analog).
- error.rs is the wrong layer for QUERY:/CONTEXT: pgwire `DiagnosticFields` (crates/pgwire/src/error.rs:77) has no internal_query/internal_position; needs wire fields q/p and body-relative position tracking.
- OUT-param result type checks belong in build_routine (CREATE time), not check_replaceable; the OR REPLACE failure is because RoutineResult::Unspecified != Type when RETURNS is spelled.
- Parser prerequisite: parser.rs `routine_arg` (~14375) accepts `[mode] [name] type` but not `name mode type` (`f2 out anyelement`, `a variadic int[]`).

## Oracle facts
All stated facts check out against self-check-serial/results/rangefuncs.out and polymorphism.out. One description error: the failing polyf calls use anycompatiblerange/anycompatiblemultirange + anycompatible, not anyrange/anymultirange + anyelement.
