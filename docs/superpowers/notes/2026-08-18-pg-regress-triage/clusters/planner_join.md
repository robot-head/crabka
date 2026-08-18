# planner_join cluster — root-cause triage (2026-08-17)

Materials: certified diffs under `scratchpad/diffs/*.diff`, oracle `self-check-serial/results`, Gres `gres-serial/results`.
Helper scripts in this directory: `hunks.py` (hunk splitter), `cat.py`, `planner_est.py`, `pollute.py`.
Cluster total: 19 files, 17,290 changed lines.

## Headline findings

1. select_into poisons int4_tbl for every later file. `INSERT INTO int4_tbl SELECT 1 INTO f;` (PG: `ERROR: SELECT ... INTO is not allowed here`) is executed by Gres: `parser.rs::opt_select_into` stores the INTO target in parser state and `finish_query_statement` consumes it only at top level; a nested INTO is dropped and the INSERT runs. int4_tbl has a sixth row `1` in join, subselect, union, join_hash, aggregates ... Evidence: gres join.out `(6 rows)` for int4_tbl blocks; join_hash lateral test returns `456|457|1`; subselect `f1 NOT IN (SELECT f1+1 FROM int4_tbl ...)` loses two rows; join hunk 162 (`left join int4_tbl i4 on i4.f1 = 1`) matches where PG gives NULLs. ~140 lines in this cluster. Fix first.
2. A fixed 16 MiB blocking-query memory budget (`scanner.rs::BLOCKING_QUERY_MEMORY`, charged as size_of::<Datum>() per column) makes tenk1 (10000x16) unsortable and unjoinable and a 20000-row uuid table unsortable: `53200 blocking query exceeded the memory budget` (exec.rs key_source_rows/ensure_blocking_rows_fit, join.rs push_bounded_join_row, scanner.rs collect_cursor_bounded). 46 statements / ~830 lines (tuplesort 438, join ~190, limit ~70, memoize 54, tidscan 18, subselect ~20).
3. join_hash is a whole-file cascade on `update pg_class set reltuples = 1000 ...` -> `relation "pg_catalog.pg_class" does not exist` inside `begin;` (704 of 843 lines).
4. select_parallel (1148), explain (693), memoize (226/308), part of incremental_sort are cascades on user-defined SETOF functions in the select list (`routine.rs::validate_plpgsql_scalar` / :2609). explain also needs ARE `\m`/`\M` in regexp_replace (`regexp_fn.rs::compile_pattern`, Rust regex rejects).
5. `WITHIN GROUP` is not parsed at all (tuplesort 12 lines; agg_window cluster).
6. join: 4,461 explain lines + ~1,900 row-order lines are planner-driven; 1,552 error-block lines and ~75 wrong-result lines are concrete engine gaps.

## Per-file
(see structured output; same numbers)

## Roots (ids prefixed planner_join-)
cost-planner-explain XXL ~11000; plan-structural-transforms XL ~900 standalone; explain-renderer-gaps L ~250; explain-typed-deparse L ~200; explain-utility-formats-options M-L ~690; srf-user-functions-in-select-list M ~2250 gated; select-into-nested-contexts S ~150 (+cross-cluster); blocking-memory-budget S/XL ~830; writable-pg-class M 704; lateral-binder-gaps L ~380; row-subquery-comparisons M ~160; subqueries-outside-select-context M ~60; parser-from-clause-and-star-gaps M-L ~430; setop-type-resolution M ~55; tid-scan-access-path M ~200; hash-iteration-order XL ~40; limit-with-ties-viewdef S-M 35; column-name-inference S ~40; error-cursor-position S-M ~60; guc-gaps S ~20; regex-are-word-boundaries S (gates explain); xc-catalog-and-types ~700; xc-index-ddl ~210; xc-ddl-parse ~110; xc-dml ~35; xc-txn-serializable 8; sql-function-final-ctas 28; volatile-group-key-double-eval 3; recursive-cte-parenthesised-form ~37; for-update-restrictions ~30; xc-user-operator-resolution ~57; xc-ordered-set-aggregates 12.

Full detail (evidence, oracle facts, fix locations) is in the StructuredOutput returned to the orchestrator.
