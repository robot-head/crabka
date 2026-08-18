# Verification of dgr-planner-only (cluster datetime_geometry_ranges)

Verdict: root cause CONFIRMED for the EXPLAIN blocks and the cross-join reorders.
Attribution NOT reasonable as a single "planner-only / XXL" root: 1338 of the
1641 lines are cross-join loop-side reorders that a small heuristic in
join.rs reproduces (the analyst says so in fix_location[1] but still sizes them
XXL/PLANNER). 4 lines are sort tie-order (PG unstable qsort), not planner.

## Recount (whole-block rule, headers excluded)

| file       | total | EXPLAIN plan-shape blocks (strict planner-only) | cross-join reorder | other (not this root) |
|------------|-------|-----------------------------------------|--------------------|-----------------------|
| interval   | 173   | 10 (Index Only Scan hunk: -4 +6)         | 4 (lhst/rhst)      | 159 (interval input/precision/etc.) |
| box        | 183   | 116 (12 blocks x 8 + 9 + 11)             | 0                  | 67 (cascade from box_in `'(0,0)(0,100)'` reject: 3 error lines + missing-row result blocks) |
| polygon    | 157   | 157 (12 x 12 + 13)                       | 0                  | 0 |
| geometry   | 1346  | 0                                        | 1334 (13 pure-reorder hunks) + 4 sort-tie (hunks 11,12) | 12 (2 EXPLAIN Filter const-typing hunks) |
| rangetypes | 124   | 14 (8 + 6)                               | 0                  | 110 (Output: folding, error text, HINT/NOTICE, array colname, textrange_supp Filter) |
| **sum**    |       | **297**                                  | **1338 (+4 tie)**  | |

Analyst: 1641 (14/120/157/1334/16). Mine under the analyst's inclusive
definition: 1639. Strict "only a cost-based planner can match": 297.

## Evidence

- box.diff: every EXPLAIN block is `Index Only Scan using box_spgist on box_temp /
  Index Cond: (f1 << '(30,40),(10,20)'::box)` vs Gres `Seq Scan on box_temp /
  Filter: (f1 << '(10,20),(30,40)'::text)`. Two later blocks expect
  `WindowAgg / Window: w1 AS (...) / -> Index Scan using quad_box_tbl_idx /
  [Index Cond] / Order By: (b <-> '(123,456)'::point)`; Gres prints a bare
  `Seq Scan on quad_box_tbl` (no WindowAgg at all).
- box.diff hunk 1 also carries a non-planner cascade: `ERROR: invalid input
  syntax for type box: "(0,0)(0,100)"` (PG accepts the comma-less form) and the
  4 infinite/degenerate rows missing from every later result set.
- polygon.diff: 12 x `Aggregate / -> Bitmap Heap Scan / Recheck Cond /
  -> Bitmap Index Scan on quad_poly_tbl_idx / Index Cond` vs `Aggregate /
  -> Seq Scan / Filter: (p << '...'::text::polygon)`; last block WindowAgg+KNN.
- rangetypes.diff: `Sort / -> Index Only Scan using test_range_spgist_idx /
  Index Cond: (ir -|- '[10,20)'::int4range)` vs `Seq Scan / Filter: (ir -|-
  int4range(10, 20))`; `Aggregate / -> Index Scan using
  test_range_elem_int4range_idx` vs Seq Scan.
- interval.diff: `Index Only Scan using interval_tbl_of_f1_idx on interval_tbl_of r1`
  vs `Sort / Sort Key: f1 / -> Seq Scan on interval_tbl_of r1` (10 lines);
  CROSS JOIN comparison table reordered (4 lines).
- geometry.diff: script check — 15 hunks are exact multiset-equal reorders
  (18,126,16,72,72,144,144,144,108,108,2,2,126,126,126 = 1334); 2 hunks (12
  lines) are Filter const-typing (`'<(1,-2),1>'::circle` vs `circle(point(1, -2), 1)`;
  `'((1,1),(2,2),(2,1))'::polygon` vs `'...'::text::polygon`).

### The reorder pattern (checked against every cross join in geometry.out)

In every diffed hunk the SECOND FROM item is PG's outer loop:
`LSEG_TBL l, LINE_TBL l1` (8 vs 10 rows), `BOX_TBL b, POINT_TBL p` (5 vs 10),
`PATH_TBL p, POINT_TBL p1` (9 vs 10), `POLYGON_TBL poly, POINT_TBL p` (7 vs 10),
`CIRCLE_TBL c, POINT_TBL p` (8 vs 10), `POINT_TBL p1, POINT_TBL p2 WHERE
p1.f1[0] BETWEEN 1 AND 1000` (p1 filtered), interval `lhst CROSS JOIN rhst WHERE
NOT isfinite(lhst.i)` (lhst filtered). Cross joins NOT in the diff confirm the
rule from the other side: `POINT_TBL p, LSEG_TBL l`, `POINT_TBL p, BOX_TBL b`,
`LSEG_TBL l, BOX_TBL b`, `BOX_TBL b, POINT_TBL p WHERE p.f1[0] BETWEEN ...`,
`POINT_TBL p1, POINT_TBL p2 WHERE p2.f1[0] BETWEEN ...`, and unfiltered
`POINT_TBL p1, POINT_TBL p2` (tie -> first item outer). Rule: larger
(post-filter, PG-estimated) side outer; on a tie the first FROM item stays
outer (PG add_path keeps the first path on equal cost). PG's estimates for
these never-analyzed tables come from the 10-page fallback x tuple width, and
happen to order the same way as actual row counts here.

### Sort tie hunks (geometry 11,12 — 4 lines)

`SELECT ... FROM CIRCLE_TBL c1, POINT_TBL p1 ORDER BY distance, area(c1.f1),
p1.f1[0]`: rows `<(1,2),3>` and `<(5,1),3>` with point `(1e+300,Infinity)` /
`(NaN,NaN)` tie on all three keys. CIRCLE_TBL insertion order has `<(5,1),3>`
first, so under either join order the sort input has `<(5,1),3>` first; PG
emits `<(1,2),3>` first = PG's unstable qsort. Not planner; needs a faithful
port of PG's tuplesort qsort (n=80 > insertion-sort threshold).

## Source facts

- crates/pgexec/src/explain.rs: header comment says "no cost-based planner";
  `plan_from` (l.225) / `plan_table_expr` / `scan_node` (l.298) always emit
  `Seq Scan` + `Filter`; `plan_select` (l.162) has no window branch — no
  WindowAgg ever; joins always `Nested Loop` with first item as first child.
  No "Index", "Bitmap", "WindowAgg" strings anywhere in explain.rs.
- crates/pgexec/src/join.rs `join_relations_impl` (l.345), arm
  `JoinKind::Inner | JoinKind::Cross` (l.393): `for l in &left.rows { for ri in
  candidate_rows(...) }` — left (= accumulated first FROM item) is always the
  outer loop. Both sides are fully materialized Vec<rows>, so swapping loop
  nesting (while still emitting l ++ r) is local.
- crates/pgexec/src/exec.rs `build_from` (l.13373) folds comma items via
  `append_from_item` (l.13547) -> `push_local_where` (l.13610) which pre-applies
  single-side WHERE conjuncts before the join, BUT `leakproof_predicate`
  (l.13654) rejects any `Expr::Func`, so `NOT isfinite(lhst.i)` is not pushed:
  a row-count heuristic sees 3 vs 3 rows and keeps lhst outer (wrong for
  interval's 4 lines). `p1.f1[0] BETWEEN 1 AND 1000` (subscript, no Func) IS
  pushed, so geometry's filtered case would work on counts.
- Explicit `CROSS JOIN` (TableExpr::Join, exec.rs l.17701) reaches the same
  append_from_item.
- Indexes: catalog knows spgist (pgcatalog IndexMethod::Spgist,
  catalog_rel.rs SPGIST_AM_OID, builtin_opfamilies.rs box_ops/poly_ops/
  range_ops/quad_point_ops) but scanner.rs never reads a secondary index for a
  scan. `enable_seqscan`/`enable_bitmapscan`/`enable_indexscan` GUCs exist in
  session.rs (l.1102-1122) and are inert.

## Hidden prerequisites / fail-longer

1. WindowAgg node + PG18 `Window: w1 AS (...)` line in explain.rs (deterministic,
   non-planner) — box 20 lines, polygon 13 lines fail even with a planner.
2. Index Cond / Order By constants need typed, folded, output-normalized
   consts (`'(30,40),(10,20)'::box` from input `'(10,20),(30,40)'`;
   `'[10,20)'::int4range` from `int4range(10,20)`; `'(123,456)'::point` from
   `point '123,456'`) — the analyst's const-typing dependency; every one of the
   297 planner blocks fails longer on it.
3. Planner must know AM/opclass operator support (which operators an spgist
   box_ops / poly_ops / range_ops index can serve, KNN `<->` ordering) and
   honor enable_seqscan/enable_bitmapscan; index-only eligibility.
4. Interval reorder: estimate-based side choice or filter-presence proxy (see
   leakproof gate) — pure row-count heuristic does not cover it.
5. explain.rs `plan_from` must print the same side choice as join.rs
   (not exercised in these five files, but a split heuristic would desync).
6. Regression risk of a count-based heuristic on currently-exact files where
   PG's estimate order != actual order; certify on the full schedule.
7. 4 lines: PG qsort tie order (separate S/M root, likely shared with other files).
