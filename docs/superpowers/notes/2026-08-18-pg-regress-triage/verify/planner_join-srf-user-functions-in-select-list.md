# Verification: planner_join-srf-user-functions-in-select-list

Verdict: root cause CONFIRMED (with one wrong sub-claim), fix location MOSTLY confirmed
(one location wrong), attribution within 30% but misleading, hidden prerequisites found.

## 1. Root cause (confirmed)

- select_parallel.diff:138 `select sp_test_func() order by 1;` -> `ERROR: set-returning
  function sp_test_func() is only supported in FROM position`. The file runs inside
  `begin isolation level repeatable read;` (oracle .out line 17) that ends at `rollback;`
  (line 1346). Statements before it (lines 17-137) produce no error (only planner EXPLAIN
  diffs), so sp_test_func IS the first failure in the transaction and the producer of the
  cascade of 181 "current transaction is aborted" lines + 6 "savepoint does not exist".
- explain.diff: 30 `select explain_filter(...)` blocks -> same 0A000 (plpgsql SETOF text).
  Not a cascade (no BEGIN around them; the `set track_io_timing` error is separate).
- memoize.diff: 13 `SELECT explain_memoize(...)` blocks -> same 0A000 (plpgsql SETOF text).
- incremental_sort.diff: 2 explain_analyze_without_memory blocks (RETURNS TABLE plpgsql) +
  4 blocks `function explain_analyze_inc_sort_nodes(...) does not exist`.
- select.diff:293/303: sillysrf (SQL SETOF int) two blocks.
- subselect.diff:1222: `select generate_series(1, ten) as g, count(*) from tenk1 group by 1`
  -> srf.rs:1467 "not supported with aggregation or GROUP BY". This is a BUILT-IN SRF and a
  different feature (SRF vs Agg ordering); shared with tsrf(13), groupingsets(2),
  aggregates(1), json(1). Should be its own root, not folded into "user SETOF functions".

## 2. Fix locations

- routine.rs:2112 validate_plpgsql_scalar -> 0A000 for plpgsql SETOF (called from
  eval_plpgsql_scalar_with at :2049, plpgsql_declared_call_type :1892,
  plpgsql_scalar_result_type :1944). CONFIRMED.
- routine.rs:2589 inline_scalar_call -> :2607-2612 0A000 for SQL-language SETOF (called
  from inline_scalar :1863 and useragg.rs:767). CONFIRMED. Note: SQL functions are INLINED
  at rewrite time; plpgsql ones run through the SCALAR_RUNTIME request channel.
- srf.rs:300 classify — built-in only; is_set_returning(:356)/expr_contains_srf(:1453)
  are name-only predicates with no catalog; call sites exec.rs:15298, 18997, 24121,
  session.rs:13771. rewrite_expr(:1578) plans via srf::plan(:456) which errors on
  unknown names. CONFIRMED that select-list expansion never consults the routine catalog.
  Seam available: routine::scalar_runtime_catalog() (routine.rs:98).
- FROM-position path exists: exec.rs:17748-17784 (`expands_as_table` ->
  eval_plpgsql_table_function (routine.rs:3052, request channel
  FunctionRequestKind::Table, handled session.rs:8073) or table_function_expansion for
  SQL-language). explain.diff:327 proves plpgsql explain_filter runs in FROM position
  (fails later inside the body on the `\m` regex).
- plpgsql.rs "FOR ... IN SELECT" location is WRONG/imprecise: the FOR query is bound
  (bind_statement :2723) and delegated to `session.run_one` (plpgsql.rs:2100-2103), i.e. it
  is an ordinary SELECT. The message text `function X(...) does not exist` (with "(...)")
  is func.rs:501 `undefined_function`, reached only after the routine dispatch
  (eval.rs:369 eval_plpgsql_scalar / eval.rs:4282 plpgsql_scalar_result_type) declined.
  So the defect is: a user plpgsql function call used as an ARGUMENT of a built-in SRF in a
  FROM item (`jsonb_array_elements(explain_analyze_inc_sort_nodes(query))`) is typed/
  evaluated by srf::plan/from_item (srf.rs:456/1185, `static_arg_types` with
  Scope::empty()) on a path where the SCALAR_RUNTIME dispatch returns None. Exact site not
  pinned without running Gres; it is not in plpgsql.rs.

## 3. Attribution (whole-block rule)

Direct blocks (minus lines + the ERROR line(s)):
  select_parallel 7, explain 509, memoize 242, incremental_sort 119, select 18,
  subselect 6 => 901.
select_parallel cascade (expected lines 138..1346, inside the transaction): 1019 changed
lines total (7 direct + 1012 cascade). Lines outside cascade: 85 before, 44 after.
Revised total: 1913 (analyst 2250, +17.6%, within 30%).
BUT: only ~31 of those lines are recovered by the SRF fix alone (select 18, select_parallel
7, subselect 6 — and the subselect 6 need the separate SRF+GROUP BY root). The other ~1880
"fail longer": explain 509 need `\m`/`\M` regex + planner shape (+ track_io_timing /
compute_query_id GUCs for a few); memoize 242 need EXPLAIN ANALYZE with Memoize nodes;
incremental_sort 119 need EXPLAIN ANALYZE with Incremental Sort + FORMAT JSON; the
select_parallel cascade contains 564 lines of oracle EXPLAIN plan blocks (Gather/Parallel).
All GUCs set inside the select_parallel transaction (debug_parallel_query,
parallel_leader_participation, max_parallel_workers, enable_*, ...) are known to Gres, so
the transaction should stay alive after the fix.

## 4. Dependencies / hidden prerequisites

- Select-list SRF machinery (srf::project_rows_ordered) has only an EvalCtx. plpgsql SETOF
  can be served through the existing request channel (FunctionRequestKind::Table). SQL-
  language SETOF (sillysrf, sp_test_func) is today a query rewrite that needs a read
  context; per-row (correlated) args need a new seam (route SQL routines through the
  session request channel, or a LATERAL rewrite). Regression cases here have constant
  args, so a constant-arg path is enough for these files.
- explain: `\m` `\M` word-boundary regex (analyst noted) — mandatory, otherwise every
  explain_filter block errors "invalid regular expression" (explain.diff:327 shows it).
- SRF + GROUP BY (built-in) is a separate root (ProjectSet below/above Agg).
- Everything else after unblocking is planner/EXPLAIN-ANALYZE.
- Root shared beyond this cluster: also create_function_sql(2), misc(2),
  misc_functions(14), partition_prune(14), polymorphism(2+1), merge(8), plpgsql(3+1),
  rangefuncs(14+3), sqljson_queryfuncs(1) carry the same refusal.

## 5. Oracle facts

- rows expand, column named after the function, ORDER BY works: CONFIRMED
  (select.out sillysrf; select_parallel.out sp_test_func -> bar, foo).
- "generate_series(1, ten) ... group by 1 evaluates the SRF after aggregation": WRONG.
  Oracle plan (subselect.diff:1195-1207): HashAggregate -> ProjectSet -> Seq Scan; the SRF
  is a grouping key so it is expanded BEFORE aggregation; result `1 | 9000`.
