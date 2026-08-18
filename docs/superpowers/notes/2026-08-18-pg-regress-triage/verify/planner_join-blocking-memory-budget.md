# Verification: planner_join-blocking-memory-budget

Verdict: root cause CONFIRMED in kind, but the framing, one fix location, the
line count, the oracle facts and the dependency list all need corrections.

## 1. Root cause

Every "blocking query exceeded the memory budget" line in the six files is a
53200 raised by `crate::scanner::memory_budget_exceeded()` (scanner.rs:1136)
after a `datum_row_bytes` charge (scanner.rs:1116: `size_of::<Datum>()` per
datum + payload for Text/JsonPath/Bytea/Numeric). All hits are on tenk1 or the
20013-row abbrev_abort_uuids table. No earlier cascade: the tuplesort INSERTs
succeed, the limit hunk starts at line 368 with the first tenk1 statement.

Two DIFFERENT budgets are in play (the analyst says "fixed 16 MiB"):

* CI runs `crabka-gres --pgexec-blocking-query-memory=20MiB`
  (ci-artifact/gres-serial/server-command.txt; scripts/gres-pg-regress.sh:267
  default `GRES_PG_REGRESS_BLOCKING_QUERY_MEMORY:-20MiB`; crates/gres/src/lib.rs:857
  flag -> RuntimePolicy.blocking_query_memory, lib.rs:409/425).
  Users of the POLICY value: base scans (exec.rs:17295 collect_cursor_bounded
  with read_ctx.blocking_query_memory), join output (join.rs push_bounded_join_row
  via JoinPolicy from subquery.rs:171), lateral cache (exec.rs:13815/13845),
  count_join_rows (exec.rs:18391), session.rs 7995/9640/9757.
* Users of the hard CONSTANT `scanner::BLOCKING_QUERY_MEMORY` = 16 MiB
  (scanner.rs:939), which IGNORE the CLI knob: exec.rs:24253 key_source_rows
  (ORDER BY sort path), exec.rs:24427 ensure_blocking_rows_fit, exec.rs:18505,
  srf.rs:2321 MemoryBudget, cte.rs:492, grouping.rs:496, setops.rs:353,
  agg.rs:2872, join.rs:57 JoinPolicy::default.

This explains the observed pattern: a full tenk1 base scan (16 cols) fits in
20 MiB, but `... FROM tenk1 ORDER BY x` (key + 16-col row per entry, charged in
key_source_rows against 16 MiB) fails; `... where hundred < 48 order by ...`
(btree_index) passes. Top-K pushdown never applies to these tables:
exec.rs:18995 `top_k_pushdown_for_select` requires `table.sharded`, NOT NULL
Int4/Int8/Text order columns, constant LIMIT, no OFFSET, no SRF.

Datum size: not buildable here; from the enum layout (ArrayValue = ElemType(40)
+ 2 Vec = 88 B payload) it is ~96-112 B, consistent with the observed
thresholds. Analyst's "~80-90 B" is the right order.

## 2. Fix locations

Exist and correct: scanner.rs BLOCKING_QUERY_MEMORY(939) / datum_row_bytes(1116)
/ collect_cursor_bounded(951) / memory_budget_exceeded(1136); exec.rs
key_source_rows(24225) / ensure_blocking_rows_fit(24423); join.rs
push_bounded_join_row(1085) / count_join_rows(139).

WRONG: "JoinIndex must accept ctid keys" — join.rs:969 hashes_like_it_compares
already lists `Datum::Tid(_)` (added in 0100c4401, 2026-08-15, before this CI
run) with a comment naming tidscan's tenk1 self-joins. The tidscan failure is
instead: exec.rs:19243 `if !wants_system_column(read_ctx.refs)` skips
try_execute_local_join_count (18343) whenever ctid is referenced, so the join
falls to build_from -> join_relations and materializes 10000 x 34-datum output
rows (> 20 MiB) in push_bounded_join_row. Fix = let the count path run with
system columns (build_table_expr already stamps ctid), or raise the budget.

MISSING fix locations (per statement group):
* crates/gres/src/lib.rs:857 + scripts/gres-pg-regress.sh:267 — the real
  budget knob; the 9 constant users must be routed through the policy.
* exec.rs:13574-13585 append_from_item: WHERE->ON pushdown for comma-joins is
  all-or-nothing (whole filter must resolve at that level and be an
  immutable_row_predicate). join.diff 2710 (`, tenk1 t4, tenk1 t5 where ...`)
  and subselect 727 (`tenk1 a, tenk1 b where a.thousand=b.thousand and exists`)
  therefore run an unconstrained 10^8-pair cross join. Needs per-conjunct
  pushdown.
* exec.rs:13595 lateral_join(...) receives no `filter`; memoize's three
  statements (`... LEFT JOIN LATERAL (...) ON TRUE WHERE s.c1 = s.c2 AND
  t1.unique1 < 1000`) loop 10000 outer x 10000 inner. Needs WHERE conjunct
  pushdown into the lateral loop (left-only to acc, right-only to the inner).
* exec.rs:19226/19255 build_from runs before the select list is resolved:
  join.diff 8891-8912 (`select t1.uunique1 from tenk1 t1 join tenk2 t2 on
  t1.two=t2.two`) — PG expects ERROR 42703 + HINT "Perhaps you meant to
  reference the column "t1.unique1""; Gres materializes a 50M-row join first.
  Gres has no column-name HINT at all (only the table-alias one,
  session.rs:15072). Not a memory root.
* 100k-row join outputs (join 3585 count=100000, join 10886 OR-join count,
  subselect 727): 100k x 34 datums x ~100 B = 300+ MB; no budget raise fixes
  them. Needs a streaming count for comma-FROM+WHERE (extend
  try_execute_local_join_count) or column pruning before join
  materialization, or the pipelined executor.

## 3. Line count (whole-block rule, incl. immediate cascades)

tuplesort 182 direct + 196 "current transaction is aborted" cascade = 378
join 61; limit 54 + 12 currval cascade = 66; memoize 18; tidscan 12;
subselect 20. TOTAL 555 (analyst: 830, +50%, outside 30%).
Budget-only fixable subset: tuplesort 378 + limit 66 + tidscan 12 + join
3404/5245/5293 (22) = 478. The other 77 need the roots above.
Not counted (other roots in the same hunks): tuplesort EXPLAIN Index Scan vs
Sort (planner, 24), EXPLAIN DECLARE -> "Result" (20), WITHIN GROUP parse
error (14); memoize explain_memoize SETOF-plpgsql-in-select-list errors.

Cross-file: the same constant-vs-policy defect fires in 15 other files
(portals: DECLARE ... FROM tenk1 ORDER BY unique2 -> ~700-line cascade;
brin/brin_bloom/brin_multi INSERT ... FROM tenk1 ORDER BY ... LIMIT;
type_sanity 123, opr_sanity 100, psql 40, window, aggregates, groupingsets,
numeric, transactions, alter_generic, create_table_like, partition_join).

## 4. Dependencies / fail-longer

* limit SRF statements (`generate_series(1,10) ... order by unique2 limit 7`):
  srf.rs:1696 project_rows_ordered expands ALL 10000 source rows (100k rows)
  before sort/limit under the 16 MiB MemoryBudget; PG's Limit sits above
  ProjectSet. Needs the budget >= 64 MiB or lazy expansion.
* subselect 1404 lateral (`t1.ten + t2.ten`): took > 120 s (watchdog in
  server.log) because the lateral cache (exec.rs:13811, 64 variants, budgeted
  against the same 20 MiB) fills after ~9 of 10 variants and every further
  outer row rescans tenk1. A bigger budget also fixes the perf.
* CI budget raise (e.g. 256-512 MiB) affects the parallel schedule (up to 20
  backends); certification is serial.
* CLUSTER-then-ORDER BY ctid blocks in tuplesort rely on exec.rs
  execute_cluster physically renumbering rows (it does: delete-and-reinsert
  sorted by index key) — plausible pass once the sort fits.

## 5. Oracle facts

"results as in expected files; no error" is right for 42 statements but WRONG
for join 8027-8047: PostgreSQL raises ERROR column ... does not exist with a
"Perhaps you meant to reference the column" HINT (4 statements).
tidscan expects count 10000; join 3126 expects 100000.
