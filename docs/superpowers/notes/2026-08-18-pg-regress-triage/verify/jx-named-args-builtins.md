# Verification: jx-named-args-builtins

Verdict: root cause CONFIRMED, attribution CONFIRMED (593 = 571 jsonb_jsonpath + 22 jsonb, my recount with the whole-block rule), fix location PARTLY WRONG (see below), dependencies INCOMPLETE (mechanism is corpus-wide, not json-only), oracle facts CONFIRMED.

## 1. Root cause

- `crates/pgparser/src/parser.rs:1919-1950` (`func_call`): labels are recognised by `peek_named_argument_label` (line 2991) and immediately lowered to positional by `positional_from_named` (line 3012). The AST (`crates/pgparser/src/ast.rs`) has NO named-argument node; `FuncArgs::Exprs(Vec<Expr>)` only.
- `positional_from_named` has a hard-coded table with ONE entry, `make_interval`; every other name returns 42883 "function {name} does not support named arguments here". Confirmed verbatim at parser.rs:3018-3028.
- All 88 error lines in jsonb_jsonpath.diff (60 query, 16 match, 6 exists, 4 query_first, 2 query_array) and 4 in jsonb.diff (jsonb_set_lax) come from this line. Each is a self-contained SELECT; there is no producer statement that poisons later ones. So "cascade: true" is not right in the strict sense (nothing upstream fails); the whole-block cost is just the lost result rows.
- Executor already handles `vars` (index 2) and `silent` (index 3) positionally: `crates/pgexec/src/json_fn.rs:874-899` (`path_args`), `srf.rs:321/755` (`Srf::JsonbPathQuery` -> `jsonb_path_query_rows`), `json_fn.rs:956` (`eval_set_lax` reads index 3/4 with defaults). So the gap really is name->position resolution, not evaluation.

## 2. Fix location

Correct:
- `crates/pgparser/src/parser.rs` `func_call` / `positional_from_named` / `peek_named_argument_label` — yes, exists, this is where the refusal is decided today.
- `crates/pgexec/src/builtin_procs_*.tsv.zst` — column 19 (argument_names) is `-` for jsonb_path_exists/query/query_array/query_first/match(_tz) and jsonb_set_lax; it IS populated for make_interval, random(6339-6341), parse_ident, pg_clear_relation_stats. Column 13 (default_count) is 0 for EVERY row and there is no proargdefaults column at all: the fixture mirrors pg_proc.dat, i.e. pre-`system_functions.sql`. In PostgreSQL 18.4 the names+defaults for the jsonb_path_*/jsonb_set_lax family come from `src/backend/catalog/system_functions.sql` (82 CREATE OR REPLACE FUNCTION, 52 DEFAULT lines). So "read names from the catalog table" needs the fixture regenerated from a post-initdb pg_proc (proargnames, pronargdefaults, deparsed proargdefaults) or a hand table for those 82 functions. Hidden prerequisite.
- `crates/pgexec/src/routine.rs:3544` decoder — exists (`decode_builtin_pg_proc_rows`), destructures `argument_names` already.

Wrong / imprecise:
- `crates/pgexec/src/func.rs "builtin function dispatch"`: jsonb_path_* are NOT dispatched from func.rs. They go through `json_fn::json_func` (json_fn.rs:208-215) and `srf::classify` (srf.rs:321), reached from the guard chain in `eval.rs:369-426`. make_interval is in format_fn.rs. A per-family fix would touch json_fn.rs + srf.rs (+ format_fn.rs); the sane fix is ONE resolution point that turns (name, positional, named) into positional before that guard chain (bind/rewrite pass), backed by (a) `routine.rs` `resolve_call`/`bound_args` (line 2345, already knows `RoutineParam.name` and `.default`) for user routines and (b) a builtin (name -> param names + default exprs) table.
- Missing location: `crates/pgparser/src/ast.rs` `FuncCall`/`FuncArgs` must carry the labels (the analyst says "touches parser AST" in the rationale but does not list ast.rs), and `crates/pgexec/src/viewdef.rs` (FuncCall deparse, ~line 527) if a view/rule ever stores named notation (PG deparses `name => value`). Not exercised in these two files.

## 3. Attribution recount (whole-block rule)

Script: analysis/verify/count_named.py. jsonb_jsonpath.diff: 386 change blocks, 88 contain the named-arg error, 571 lines (incl. the 149-line lt/le/eq/ge/gt matrix at diff line 3090+). jsonb.diff: 4 blocks, 22 lines. Total 593 — identical to the analyst. 3 blocks (6 lines) are interleaved with the `.bigint()/.boolean()/.integer()` "1.23" defect (jx-jsonpath-eval-misc) because of diff alignment; +-5 lines noise.

## 4. Dependencies / fail-longer

- No planner, storage, wire dependency. Parser AST change required (analyst says so).
- Mechanism is shared corpus-wide: polymorphism.diff 25 (user function `dfunc` in named+mixed notation with overloads, and PG error text `function dfunc(x => integer, b => integer, c => integer) does not exist`), random.diff 1 (`random(min=>10,max=>100)` inside CREATE DOMAIN), name.diff 1 (`parse_ident(..., strict => false)`), stats_import.diff 4 (`pg_clear_relation_stats(schemaname=>,relname=>)`), window 1, fast_default 1. A make_interval-style table extension would fix jsonb/jsonb_jsonpath only; the plan should own the generic mechanism once (user routines by RoutineParam names; overload selection by names; PG-format signature in the 42883 message).
- Fail longer after unblocking, in jsonb_jsonpath: 61 named statements have a positional twin in the file; 54 twins already match, 7 differ (`+$` unary message says `-`; `"inf"/"-inf"` for .double()/.decimal()/.number() give the "invalid for type" text instead of "NaN or Infinity is not allowed"). Also `select jsonb_path_query('[1,"2",3]', '+$', silent => true)` expects 1 row (`1`) — PG returns the items produced before the error; Gres `JsonPath::query` (jsonpath.rs:1165) returns an empty Vec on any silenced error → 0 rows. All jx-jsonpath-eval-misc, ~35 lines.
- In jsonb: `null_value_treatment => 'raise_exception'` expects ERROR + DETAIL + HINT; `eval_set_lax` (json_fn.rs:984) raises a plain FunctionError with no detail/hint → 2 lines fail longer. Adjacent, not this root: `jsonb_set_lax(..., true, null)` returns NULL where PG errors (json_fn.rs:959) — 6 lines, separate defect.
- Filling a skipped middle slot needs the real default (`'{}'::jsonb` for vars); NULL is not equivalent (PG's jsonb_path_* are STRICT, so NULL vars → NULL result; Gres treats NULL vars as "no vars", a separate small defect).

## 5. Oracle facts

Confirmed against /tmp/pg18-build.0JYIf4/postgresql-18.4/src/backend/catalog/system_functions.sql lines 507-552: `jsonb_set_lax(jsonb_in jsonb, path text[], replacement jsonb, create_if_missing boolean DEFAULT true, null_value_treatment text DEFAULT 'use_json_null')`, `jsonb_path_query(target jsonb, path jsonpath, vars jsonb DEFAULT '{}', silent boolean DEFAULT false)` STRICT. Unknown-label error format confirmed by polymorphism.out:1448 `function dfunc(x => integer, b => integer, c => integer) does not exist`.

## Size

M is fair for this cluster's 593 lines if a builtin name/default table is hand-written for the functions the corpus uses. The corpus-wide generic mechanism (AST + user-routine resolution + overload-by-name + fixture regeneration + PG error text) is closer to M-L (1-2 days).
