# Verification: agg_window-explain-window-projectset (R23)

Verdict: root cause CONFIRMED (the change blocks are EXPLAIN blocks that need WindowAgg /
Window: / Run Condition / ProjectSet / VERBOSE Output). Fix location CONFIRMED as the file
(explain.rs plan_select:162 has no WindowAgg/ProjectSet arm; options.verbose is never read)
but INCOMPLETE: no block in window or tsrf is fixed by a purely syntactic WindowAgg/ProjectSet
renderer. Attribution: window 595 recounted exactly (my split 197 cost-based / 398
deterministic-planner / 0 purely syntactic); tsrf 36 is low (52-67 EXPLAIN lines, and 15 of
the analyst's 36 are a dummy-relation Result block with no ProjectSet in it). Size M is too
small: L for the deterministic part alone, on top of prerequisites owned by other roots.

## 1. Root cause / cascade

- window: first failing statement is a tie-order block (hunk 0, `sum(salary) OVER (PARTITION
  BY depname)`); no cascade into the EXPLAIN blocks. empsalary/t1 exist (Gres prints
  `Seq Scan on empsalary`).
- All 37 window EXPLAIN blocks are covered by the analyst's R23 list; the only other one
  (hunk 168, `EXPLAIN SELECT * FROM pg_temp.f(2)`) is a pg_temp CREATE FUNCTION cascade
  (R35) that will fail longer on WindowAgg + Function Scan rendering.
- tsrf: first failing statement is nested SRF (R6). EXPLAIN blocks: H0b `unnest WHERE false`
  (10), H1 `few f1, (…WHERE false OFFSET 0) ss` (15), H5b degenerate ORDER BY (11), H9b
  duplicate SRF calls (10), H10 SRF ORDER BY (11), H11 lockstep SRFs (10) = 67, plus H0a
  nested-SRF explain (10) that is R6 first and fails longer on ProjectSet.

## 2. What the oracle actually prints (checked in self-check-serial/results/window.out)

- `Window: wN AS (PARTITION BY … ORDER BY … [frame])`: ORDER BY direction is NOT printed
  (`count(*) OVER (ORDER BY salary DESC)` -> `Window: w1 AS (ORDER BY empsalary.salary)`,
  Sort Key keeps `DESC`); default frame omitted; short form `ROWS UNBOUNDED PRECEDING`;
  offsets typed (`'1'::bigint`, `'10000'::bigint`); `Window: w1 AS ()`.
- wN numbering follows evaluation order bottom-up (hunk 136: c3's window is w1 at the
  bottom although declared third; hunk 118: `PARTITION BY depname` (declared 2nd) is w1).
- Frame optimisation + window merging: 6 windows with different frames over row_number/
  rank/dense_rank/ntile/percent_rank/cume_dist collapse into ONE `w1 … ROWS UNBOUNDED
  PRECEDING` (hunk 114) = PG optimize_window_clauses.
- Run Condition for row_number/rank/dense_rank/ntile/count (monotonic), suppressed for
  volatile args (142), subplans (143), incompatible frame (140), wrong direction (141);
  Subquery Scan kept with `Filter: (emp.dr = 1)` when the qual is not fully absorbed (122).
- Trivial Subquery Scan elided (119/120/123/125/127/128/130); Var qualification
  `empsalary.salary` when rtable > 1 even after elision.
- EC redundancy: `Sort Key: f1` for `partition by f1 order by f2 … where f1 = f2` (110/111,
  the analyst's own evidence block); `Window: w2 AS ()` and `Window: w2 AS (ORDER BY
  empsalary.empno)` after `depname = 'sales'` is pushed down (117/144).
- Qual pushdown into the subquery only when every PARTITION BY has the column (117 vs 118).
- Typed casts from column types: `((depname)::text = 'sales'::text)`,
  `(((empsalary.depname)::text || 'A'::text))`; Gres prints `(depname = 'sales'::text)`.
- VERBOSE: `Output:` per node with planner targetlist order, `Seq Scan on pg_temp.empsalary`,
  `public.few`, `pg_catalog.generate_series` (Gres explain.rs ignores options.verbose).
- enable_hashagg=off changes DISTINCT to Unique + (Incremental) Sort (147/148); Gres still
  prints HashAggregate and leaks `Group Key: … $w.$w0 sum, $w.$w1 min` (placeholder from
  ast.rs window_placeholder / WINDOW_QUALIFIER; the analyst wrote "Sort Key", it is
  "Group Key" in window and "Sort Key" in groupingsets).
- Cost-based only (10 blocks, 197 lines): 138 Merge Join, 145/146/147/148/150 Incremental
  Sort + Presorted Key, 164-167 Index Only Scan / Hash Join / Merge Join.

## 3. Attribution

window R23 = 595 (sum of the analyst's hunk map, verified against hunk sizes; hunks 110/111
carry 3 R26 lines each). Split: 197 cost-based, 398 deterministic-planner, 0 purely
syntactic. Analyst's "~300 planner" sits between the two readings (34% off either).
tsrf: analyst 36 (H1 15 + H10 11 + H11 10). H1 is `Result / One-Time Filter: false`
dummy-relation propagation with VERBOSE Output — no ProjectSet. Missing: H9b (10), H0b
(10), H5b (11) inside R6-tagged hunks. ProjectSet-bearing lines: H0b+H5b+H9b+H10+H11 = 52;
all EXPLAIN lines 67. Cluster-level effect is small.

## 4. Fix location / prerequisites

- explain.rs plan_select:162 (no WindowAgg, no ProjectSet, `expr_list(&select.group_by)`
  ignores `select.grouping`), plan_from/plan_table_expr (Subquery Scan always printed),
  deparse_bare_with (window placeholder `$w.$wN label` needs `select.window_calls` + the
  wN numbering to print `(sum(salary) OVER w1)`), render_text_node (no Output lines).
- Hidden prerequisites: (a) a resolved/typed tree or Scope for the FROM so implicit casts
  and frame offset types print; (b) VERBOSE Output + schema-qualified relation names;
  (c) window semantics port: optimize_window_clauses, select_active_windows ordering
  (sortgroupref-descending, longer key lists first, ORDER-BY-matching window last),
  run-condition rules, wN numbering; (d) deterministic planner transforms shared with
  planner_join-plan-structural-transforms (subquery pull-up, trivial Subquery Scan
  elision, EC redundant sort keys, qual pushdown, const-false -> Result / One-Time Filter,
  dummy-rel propagation, InitPlan, degenerate ORDER BY); (e) explain must see session GUCs
  (enable_hashagg) — session.rs:5203 passes only the Statement.
- The "EXPLAIN of ARRAY(subquery) with correlated ref" symptom is in aggregates.diff (not
  in files_affected). It is EXPLAIN-only: the executed statement returns rows. Cause is the
  describe path: session.rs explain() -> exec.rs describe_statement:25126 ->
  query.rs describe_query_expr -> subquery.rs `scalar_subquery_type` types the subquery
  without the outer scope. Fix there, not in explain.rs.
- SRF/ProjectSet: srf.rs executes select-list SRFs; ProjectSet placement is
  split_pathtarget_at_srfs (SRF-free exprs above SRFs -> Result over ProjectSet; nested
  SRF args -> stacked ProjectSets). tsrf blocks all use `explain (verbose, costs off)` so
  they also need VERBOSE Output.
- Fail-longer: once WindowAgg prints, every window block still needs one of (c)/(d)/(a);
  the analyst's evidence block (110) needs EC (`Sort Key: f1`).

## 5. Brief corrections

- what_gres_does mixes symptoms from aggregates (ARRAY subquery) and groupingsets
  (Sort Key: $w…) into a root whose files_affected are window/tsrf.
- "one per distinct window, Sort below" is not the oracle rule: windows are merged and
  reordered, Sort appears only when the input order is insufficient (else Incremental Sort).
- Size M understated; deterministic part is L, and it depends on a typed tree / VERBOSE
  root and the structural-transform root.
