# Verification: txn-x-blocking-memory-budget

Verdict: root cause CONFIRMED (with a corrected mechanism); fix location
PARTLY WRONG (the harness knob does not reach the tripping stage; cursor
laziness would not remove it); attribution reasonable (my count 778 vs 757).

## 1. Root cause

portals.diff hunk 1 (`@@ -3,744 +3,201 @@`) starts at the FIRST statement of
the file: `BEGIN; DECLARE foo1 SCROLL CURSOR FOR SELECT * FROM tenk1 ORDER BY
unique2;` -> `+ERROR:  blocking query exceeded the memory budget`. Nothing
precedes it, so it is the producer, not a cascade victim. 86 following
statements in the same transaction print "current transaction is aborted".
The same error recurs at foo24 (twice), foo26, and `DECLARE c CURSOR FOR
SELECT * FROM tenk1 JOIN tenk2 USING (unique1)`.

transactions.diff hunk 16 (`@@ -432,67 +400,38 @@`): `DECLARE c CURSOR FOR
SELECT unique2 FROM tenk1 ORDER BY unique2;` -> same error, then SAVEPOINT /
FETCH / ROLLBACK TO cascade (61 changed lines).

Which stage trips: NOT the base scan. Evidence: `DECLARE foo25 SCROLL CURSOR
WITH HOLD FOR SELECT * FROM tenk2;` (10000 rows, same 16-column shape, no
ORDER BY) succeeds and its FETCH matches (context lines in hunk 2). The base
scan is charged in `collect_cursor_bounded` (scanner.rs:950-1000) against
`read_ctx.blocking_query_memory` = the harness's 20 MiB
(exec.rs:17295-17318). Every whole-tenk1 ORDER BY, however, goes through
`project_rows_ordered` -> `key_source_rows` (exec.rs:24188 / 24225-24263),
which charges `datum_row_bytes(keys) + datum_row_bytes(row)` per row against
the hard-coded `crate::scanner::BLOCKING_QUERY_MEMORY` = 16 MiB
(scanner.rs:939), not the configured policy. Rows are full width because
`plan.projection = ProjectionPushdown::All` is forced at exec.rs:18949 and
top-K pushdown needs `table.sharded` (exec.rs:18995) - so even `SELECT
unique2 FROM tenk1 ORDER BY unique2` sorts 16-datum rows. `datum_row_bytes`
charges `size_of::<Datum>()` (~96 B, the enum carries ArrayValue = ElemType
40 + 2 Vecs) per int4 cell, so 10000 x 17 datums ~= 16.5 MB, right at the 16
MiB constant. Corroborating grid: psql.out `select unique2 from tenk1 order by
unique2 limit 19` (no cursor at all) fails the same way, as do limit.out
`select unique1, unique2, nextval('testseq') from tenk1 order by unique2 limit
10` and every tuplesort whole-table sort.

## 2. Fix location

- scanner.rs `BLOCKING_QUERY_MEMORY` (939), `datum_row_bytes` (1119),
  `scanned_row_bytes` (1133), `memory_budget_exceeded` (1136): exist.
  `collect_cursor_bounded` (~950) and `collect_partial_aggregates_bounded`
  (~1013) are the "eager scan collectors" - they are NOT the tripping stage
  for these files.
- The tripping stage is exec.rs `key_source_rows` (24225-24263) and its
  siblings that use the constant: `ensure_blocking_rows_fit` (24423),
  exec.rs:18505, agg.rs:2872, grouping.rs:496, cte.rs:492, srf.rs:2321,
  setops.rs:353. Only join.rs / subquery.rs / the scan collectors read the
  policy value (`blocking_query_memory` field).
- scripts/gres-pg-regress.sh `GRES_PG_REGRESS_BLOCKING_QUERY_MEMORY` (77,
  267) -> `--pgexec-blocking-query-memory` (gres/src/lib.rs:857) -> policy
  (pgexec/src/lib.rs:409/425) -> session.rs:2631/2851 -> SubCtx. Raising it
  alone does NOT fix portals/transactions. Commit 0100c4401's message already
  records: "--pgexec-blocking-query-memory does not govern that constant at
  all, which is why raising it changes nothing".
- session.rs `declare_cursor` (4589-4635): exists, eager (`run_select`).
  Laziness would NOT remove the consumer here: the ORDER BY sort must still
  hold all 10000 rows in `key_source_rows` because Gres has no ordered index
  scan; only a planner-chosen index order (PG uses tenk1_unique2) avoids the
  sort. So the "cursor laziness removes the largest single consumer" note is
  wrong for these statements.

Corrected fix: (a) route one configured budget through every
`crate::scanner::BLOCKING_QUERY_MEMORY` use (thread `blocking_query_memory`
into `project_rows_ordered`/`key_source_rows` etc., or make the constant the
policy default only), and (b) raise the harness default (20 MiB) high enough
for the join cursor (`tenk1 JOIN tenk2`, 31 datums x 10000 ~ 30+ MB measured
in join.rs) - 256 MiB is comfortable. Alternatively make `datum_row_bytes`
charge a realistic per-type size (int4 = 4, not 96). Size S-M.

## 3. Attribution (whole-block rule)

portals: hunk 1 = 725 changed lines (91 +, 634 -). Two of its blocks are the
`pg_cursors` view missing outside the aborted transaction (after `END;` and
under "Cursors outside transaction blocks"): 5 + 5 = 10 lines, not memory.
So 715. Plus foo26 (+1) and the join cursor (+1) = 717.
transactions: hunk 16 = 61. Total 778 (analyst 757, within 3%).

Fail-longer after a budget-only fix (still inside those 778):
- `SELECT ... FROM pg_cursors ORDER BY 1` inside the first portals txn:
  16 lines (pg_cursors view root).
- `FETCH BACKWARD 1 FROM foo24` / `FETCH ABSOLUTE 1 FROM foo24`: Gres emits
  55000 "cursor can only scan forward" (session.rs:4651) without PG's
  `HINT:  Declare it with SCROLL option to enable backward scan.` - 1 line
  each (grep finds no such HINT text in crates/).
- transactions: `DECLARE c CURSOR FOR SELECT unique2/0 FROM tenk1 ORDER BY
  unique2` - PG declares lazily and errors at FETCH under SAVEPOINT two; Gres
  errors at DECLARE and aborts the txn: ~11 lines stay (txn-cursor-lazy).
Net gain from the budget fix alone ~= 749.

## 4. Dependencies

None for the two files beyond the above. Cross-cluster caveat: the ~20 other
files listed are NOT all budget-only. groupingsets `from tenk1 t1, tenk1 t2
where x = 1` (100M-row cross product), join's 3-branch OR join, memoize's
lateral joins, aggregates' 4x UNION ALL over tenk1, subselect lateral: some
need pushdown/planner work, and a much larger budget converts a fast 53200
into a long-running statement (the watchdog already logs a 120 s lateral in
server.log). Raising the budget also flips ~20 files' fingerprints in
pg-regress-baseline.json (ratchet review, not code).

## 5. Oracle facts

Correct: self-check portals.out lines 3-800 show DECLARE foo1..foo23 with no
error and all FETCH results; transactions.out 484-496 shows the lazy-cursor
behaviour (division by zero at FETCH, then `portal "c" cannot be run`).
