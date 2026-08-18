# Verification: agg_window-sort-tie-order-fidelity

Verdict: CONFIRMED (root cause, fix location, attribution, oracle facts). Dependencies: partly missed.

## 1. Root cause

- window.diff hunk 1 (expected line 23) is the FIRST hunk in the file: `SELECT depname, empno, salary, sum(salary) OVER (PARTITION BY depname) FROM empsalary ORDER BY depname, salary;`
  expected develop 11 before 10 (equal salary 5200), sales 3 before 4 (equal 4800); Gres keeps insertion order (10 before 11, 4 before 3).
  Second block in the same hunk (`rank() OVER (PARTITION BY depname ORDER BY salary)`, no ORDER BY) is returned by Gres in raw insertion
  order (develop 10, sales 1, personnel 5, sales 4, personnel 2, develop 7, develop 9, sales 3, develop 8, develop 11) — exactly the INSERT order.
- Not a cascade: CREATE/INSERT succeeded; every hunk is an independent statement.
- Content hunks that look like value differences are also this root: last_value/first_value/nth_value/row_number/lead/lag over peer rows depend on
  peer order (H015 last_value(four) OVER (ORDER BY ten): expected 0, Gres 2; H066-69, H079/82/84/86 f_time 15:00 ids 6 vs 5; H106-109; H117 rn 4/5 for empno 9/7; H138 sales 3 vs sales 1 for row_number() OVER (PARTITION BY depname); H152).

## 2. Oracle facts (checked with a Python port of PG 18.4 src/include/lib/sort_template.h ST_SORT)

Port at analysis/verify/agg_window_tie/pgqsort.py. Runs against insertion order (empsalary) / COPY order (tenk.data) reproduce every expected tie order tested:
- (depname, salary): 7 9 11 10 8 | 5 2 | 3 4 1  (H001, rank query)
- (depname): 11 7 9 8 10 | 5 2 | 3 1 4          (H002)
- (depname, salary DESC), (salary), (salary DESC → 8 10 11 ...), (enroll_date), (depname, enroll_date → 8 10 11 9 7 2 5 1 3 4)
- x DESC NULLS FIRST on 7 rows: 43 42 5 4 3 2 1 (H071); NULLS LAST: 5 4 3 2 1 42 43 (H072)
- datetimes f_time DESC: 11 10 9 8 7 6 5 4 3 2 1 0 (id 6 before 5) (H079)
- tenk1 WHERE unique1<10 sort by four from HEAP order [4,2,1,6,9,8,5,3,7,0] → 0 8 4 | 5 9 1 | 6 2 | 3 7 (H029/H040). Index-order input gives a different (wrong) answer, so PG's input was heap order (Bitmap Heap Scan) and Gres already scans in that order.
- Two-window chain (H152): sort by (depname, enroll_date DESC) → w1; then full re-sort of that output by (depname, enroll_date) [Incremental Sort in fullsort mode, batch < 32 rows] → w2. Reproduces first_emp/last_emp exactly (develop 8: 1/5, develop 7: 5/1 ...).
- Algorithm details verified in sort_template.h: n<7 insertion sort; presorted scan returns early; pivot = middle for n==7, med3 for n>7, 9-element med3 for n>40; Bentley-McIlroy 3-way partition with the swap semantics that decide tie placement.
- Correction of detail: in PG 18 the sorter is src/include/lib/sort_template.h (instantiated as pg_qsort, qsort_tuple, qsort_ssup, qsort_tuple_{unsigned,signed,int32}); src/port/qsort.c is only the pg_qsort instantiation. Same algorithm.
- Window sort keys: planner.c make_pathkeys_for_window (line ~6555) = PARTITION BY pathkeys ++ ORDER BY pathkeys; ORDER BY of the query is a separate Sort above the last WindowAgg (no ORDER-BY-pathkey merging into the window sort in 18.4). Presorted input → no Sort node (pathkeys_count_contained_in); prefix-presorted → Incremental Sort (create_one_window_path ~4900).
- Window evaluation order: planner.c select_active_windows (6232) qsorts active windows by common_prefix_cmp: larger tleSortGroupRef first, then larger sortop OID first (so DESC before ASC), nulls_first first, longer clause list first. The LAST window's order is the output order (H019: DESC window first, ASC last → output depname ASC; H152 confirms).

## 3. Fix location

- crates/pgexec/src/window.rs `execute` (513) evaluates `evaluate_calls` (1025) per call and leaves `base_rows` in input order (551-555); `evaluate_call` (1038) hash-partitions with `partitions()` (1248, HashMap keeps first-appearance order) and stable `sort_by` at 1052 per partition. Confirmed as the decision point.
- crates/pgexec/src/exec.rs `project_rows_ordered` (24110): stable `sort_by` at 24179, 24190, 24207, 24216. Confirmed. NB the comment at 24205 ("A stable sort is load-bearing for DISTINCT ON without an ORDER BY: PostgreSQL keeps the first row of each key group in input order") is only true for presorted input; PG uses the same unstable tuplesort there.
- crates/pgexec/src/agg.rs 1842 (`sorted_input`, aggregate ORDER BY: doc says stability makes array_agg(a ORDER BY 1) read out unsorted — PG uses tuplesort here too), 1864 (`sorted_distinct`: no visible effect, equal tuples are dropped), 2947/2956 (grouped DISTINCT ON / ORDER BY). Confirmed to exist.
- Missed sort site: crates/pgexec/src/scanner.rs 2087 top-K pushdown (`compare_top_k_rows` + truncate) — decides which tied rows survive an ORDER BY … LIMIT; PG uses a bounded heapsort there (tuplesort_puttuple_common switches when memtupcount > bound*2). Not observed in window.out; matters for the "other clusters" claim.

## 4. Attribution (whole-block rule)

Reorder-only hunks (multiset of + == multiset of -): 818 lines. Content hunks caused by peer/tie order: H008 18, H015 14, H018 4 (avg part; the other 15 lines are the `GROUP BY … WINDOW win` "column two does not exist" error), H020 8 (the other 15 lines are the correlated-subplan error), H030 16, H031 14, H033 16, H034 16, H035 16, H040 16, H066 16, H067 14, H068 14, H069 14, H071 12, H079 22, H082 22, H084 22, H086 22, H106 14, H107 14, H108 14, H109 16, H117 14, H138 2, H152 8.
Total = 1196 lines (analyst: 1198). Within noise.
Not this root: EXPLAIN WindowAgg/Sort/Run Condition rendering (H023, H111-112, H115-116, H118-121, H123-124, H126, H128-131, H133-134, H136-137, H139-151, H165-169), RANGE offset on timetz (H081, 89), infinite interval offsets (H080/83/85/87), LINE/HINT decorations, moving-aggregate mode (H154-157), avg/sum(interval) (H160-164), memory budget (H041, H110), nth_value_def named args (H153), pg_get_viewdef alias `i(i)`/interval literals (H042-48), error texts (H113), pg_temp (H169).

## 5. Dependencies / hidden prerequisites

1. One GLOBAL sort per window by (partition keys ++ order keys); partitions are contiguous runs of the sorted stream. Per-partition sorting of hash-grouped rows gives develop 10,7,9,8,11 for H002 instead of 11,7,9,8,10.
2. Window evaluation order per select_active_windows/common_prefix_cmp (needs sortop OIDs or the equivalent rule); each window's sort input is the previous window's output; presorted → no re-sort; prefix-presorted → Incremental Sort (fullsort batch semantics; small inputs behave like one full qsort of the previous output).
3. Query ORDER BY = separate pg_qsort pass over the last window's output (already how project_rows_ordered is structured).
4. Sort input must be PG heap order: Gres scan order == COPY/insert order (verified on tenk1 and empsalary). Index-scan-ordered inputs (planner choice) would break this; none observed in window.out.
5. LIMIT with ties → PG bounded heapsort when count > 2*bound (other clusters); external sort past work_mem (not reached in window).
6. Hash-aggregate / hash-join output order feeding a sort with ties cannot be reproduced by the sorter alone (other clusters; window.out has no such case).
7. No parser, wire, catalog or storage change needed. No SCHEMA_VERSION bump.

Unblocked statements: none blocked; the remaining window hunks are the independent roots listed above.

## Size

M is optimistic: the sorter port is ~150 lines, but window::execute needs a restructure (window ordering, chained global sorts, partition runs) and the DISTINCT ON / agg comments assert stability that PG does not have; verifying every rewired site against regress data pushes it to M–L (2-3 days).
