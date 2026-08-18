# Verification: indexes-storage-planner-index-scans

Verdict: root cause CONFIRMED, fix location PARTLY WRONG (incomplete), attribution REASONABLE
(my count 1.8k pure + 0.43k cascade-then-planner), dependencies INCOMPLETE.

## 1. Root cause

Read all 12 diffs. Every hunk whose minus side carries Index Only Scan / Index Scan /
Bitmap* / Order By: / Memoize / Materialize / Disabled: true / Recheck Cond has on the plus
side `Seq Scan` + `Filter:` (or `Sort` + `Sort Key`, or `Nested Loop` with the whole WHERE
as `Join Filter`). This is exactly what `crates/pgexec/src/explain.rs` (module doc, lines
1-13: "Gres has no cost-based planner ... renders the shape the interpreter will actually
execute") produces: `plan_statement` -> `plan_select` -> `plan_from` -> `scan_node` is a
pure AST walk.

Not a cascade in the core files: in create_index_spgist the three SP-GiST indexes
(sp_quad_ind, sp_kd_ind, sp_radix_ind) are created without error (gres-serial
create_index_spgist.out lines 9-18); in gist the gist_tbl indexes build; in btree_index the
tenk1 indexes exist. So the plan blocks there are genuinely gated on planner+executor.

Cascade files (planner blocks are downstream of a producer defect and stay in that
producer's root, but "fail longer" on the planner once unblocked):
- gin: `create index ... using gin (i)` -> "GIN indexes currently support only tsvector
  columns" (exec.rs:2778). All 7 EXPLAIN blocks + the explain_query_json table (20 lines)
  are downstream.
- create_index array_index_op_test / hash_tuplesort_idx (name_ops) / bitmap_split_or partial
  indexes / btree_bpchar (bpchar_ops on text): producer = GIN generic opclasses, `name`
  collapsed to text, partial index refusal, opclass binary-coercibility.
- hash_index: the single plan block uses hash_i4_partial_index which was refused
  ("partial indexes ... are not supported", exec.rs:2514).
- index_including / index_including_gist: INCLUDE refused (exec.rs:1426).
- brin / brin_multi / brin_bloom: "index access method \"brin\" is not supported"
  (exec.rs:1419), plus brin_* SQL functions missing.

## 2. Fix location

- explain.rs: exists, is the syntactic renderer. Correct that it must be replaced by a
  renderer over a real plan tree.
- scanner.rs `RangeScanner` / `PredicatePushdown`: exist (scanner.rs:62, :716). Correct
  seam for pushing scan predicates to range owners, but NOT where the index decision is
  made today.
- join.rs `join_relations`: exists (join.rs:71) but it is execution only. The Join Filter
  vs inner-Filter split shown in EXPLAIN is decided in explain.rs `plan_from`, not join.rs.

Missed / corrected locations:
- The brief's statement "the read path today does not consult secondary indexes for
  ordinary scans" is only mostly true. `try_scan_with_local_index` +
  `choose_local_index_equality` + `lookup_local_index_equal` (crates/pgexec/src/exec.rs
  ~18150-18245) already probe a single-column local btree index for `col = literal`, and a
  GIN tsvector index for `@@`. It is a heuristic, invisible to EXPLAIN, equality-only.
  This is the seam a planner must replace.
- `crates/pgexec/src/plan_dist.rs::plan_scan` is the existing "planning pre-pass"
  (predicate/projection/partial-aggregate/top-k pushdown). A cost-based planner must sit
  in front of it or replace it.
- STORAGE: `crates/pgkv/src/key.rs::secondary_index_entry_prefix` keys index entries with
  `rowenc::encode_row`, whose module doc says "It is NOT order-preserving. Values are never
  sorted by raw bytes." Range Index Cond (`hundred < 48`), backward scans, ordered
  index-only scans, LIKE prefix ranges cannot be driven by the current index layout. A
  memcomparable index-key encoding (new key format, DESC/NULLS FIRST orderings) is a
  hidden prerequisite.
- STORAGE: `index_entries` (exec.rs:11240) writes entries only for plain-column btree
  indexes and GIN-on-tsvector; GiST, SP-GiST, hash and every expression index are
  catalogued but store nothing. KNN `Order By: (p <-> ...)`, `Index Cond: (p <@ box)`,
  hash Bitmap Index Scan need real (or emulated) entries.
- CATALOG: btree_index's 12 EXPLAIN blocks over pg_proc need
  `pg_proc_proname_args_nsp_index` to exist as an index the planner can choose; Gres has
  no catalog indexes ("REINDEX INDEX CONCURRENTLY pg_class_oid_index -> relation does not
  exist" in create_index).
- TYPES: `name` maps to `ColumnType::Text` (crates/pgtypes/src/datum.rs:1003). That is
  why `name_ops` is refused ("does not accept data type text", exec.rs:2686) and why
  Filter text prints `'abs'::text` where PG prints `'abs'::name`.

## 3. Attribution (whole-block rule)

Script: classify2.py (hunk-level; a hunk is "planner" when its minus side has a plan-node
marker; "cascade" when the same hunk also has a +ERROR).

file                 total  pure-planner  cascade-w-plan  non-planner
create_index          1489      797           116            576
create_index_spgist    799      513           266             20
btree_index            281      265             0             16
gin                    134       60            33             41
gist                   138      137             0              1
spgist                  12       12             0              0
hash_index              13        8             0              5
index_including        270        0           205             65
index_including_gist    83       16            25             42
brin                   148        0            84             64
brin_multi             280        0           179            101
brin_bloom             121        0            54             67
SUM                   3768     1808           962            998

Manual corrections to the "pure" bucket:
- create_index_spgist: 118 lines inside "cascade" hunks are neighbouring pure EXPLAIN
  blocks (the +ERROR in the hunk is the `~<~` lexer gap); 11 of them are the INCLUDE
  cascade. +107.
- create_index: bitmap_split_or t_a_b_idx/t_b_c_idx blocks (+19, but stats-dependent);
  point_tbl `<@ polygon` count 4 vs 5 is a geometry bug (-2, crates/pgtypes/src/geometry.rs
  point_inside).
- btree_index: btree_bpchar "do match" blocks (~22) are cascade from the bpchar_ops
  opclass check.
- gin 60, hash_index 8, index_including_gist 16 "pure" are actually cascades from CREATE
  INDEX failures in earlier hunks.

Result:
- Genuinely planner-gated now (index exists, only planner+executor missing): ~1809
  (create_index ~814, create_index_spgist ~620, btree_index ~243, gist 137, spgist 12).
- Cascade blocks that need the planner after their producer is fixed: ~433
  (gin 84, hash 8, index_including_gist 26, index_including 57, brin 24, brin_multi 129,
  brin_bloom 12, create_index ~60, create_index_spgist 11, btree_index 22).
- Analyst's 1897 lies between 1809 and 2242: within 30%. Reasonable.

Caveat: the pure blocks also contain lines only typed-deparse can fix (`'(0,0)'::point`
vs `'(0.0, 0.0)'::text`, `= ANY ('{..}'::integer[])` vs `IN (...)`, `::bpchar` vs
`::character`, WindowAgg node missing). If the sibling root
"indexes-storage-explain-typed-deparse-and-nodes" ALSO counts these lines, the cluster
double-counts. Non-planner lines that survive a perfect planner in these files: ~1000
(the non-planner column) + the deparse residue.

## 4. Dependencies missed

- Order-preserving secondary-index key encoding (pgkv key.rs / a new memcomparable
  encoder); DESC / NULLS FIRST index orderings (exec.rs:2523 refuses them today).
- Index entry storage for gist / spgist / hash / expression / partial / INCLUDE indexes
  (index_entries returns empty for them).
- Catalog indexes (pg_proc_proname_args_nsp_index etc.) visible to the planner.
- `name` as a distinct type (datum.rs:1003).
- pg_statistic ("relation pg_statistic does not exist" in create_index) and CREATE
  STATISTICS (mcv) ("extended planner statistics objects are not supported") — the
  bitmap_split_or plans are chosen from those stats.
- enable_seqscan/enable_indexscan/enable_bitmapscan already accepted; planner must honour
  them and print `Disabled: true` (PG 18 disabled_nodes semantics).
- EXPLAIN (ANALYZE, FORMAT json) with nested Plans / Actual Rows / Rows Removed by Index
  Recheck for gin's explain_query_json.
- gin_fuzzy_search_limit GUC (unrecognized), gin_clean_pending_list(),
  brin_summarize_new_values()/brin_desummarize_range()/brin_summarize_range() functions.
- Sort tie order: create_index @@-186 (Infinity,1e+300)/(1e+300,Infinity) swap under
  enable_indexscan=OFF is PG's non-stable qsort tie order, not the planner (2 lines).
- "fail longer": every cascade file above (gin/hash/index_including*/brin*) fails again on
  the planner once its DDL producer is fixed; brin* additionally need a BRIN AM that
  reports `Index Searches: 1` and (actual rows=...) under ANALYZE.

## 5. Oracle facts

Checked in self-check-serial outputs: `Disabled: true` (btree_index.out:536),
`Index Searches: 1` only in brin_multi.out (EXPLAIN ANALYZE), `Memoize`/`Materialize`
(create_index.out:2208/2259), `Index Only Scan Backward` (btree_index), KNN
`Order By: (p <-> '(0.201,0.201)'::point)` (gist), the 182/8182.. row order (btree_index),
`lossy distance functions are not supported in index-only scans` (gist, PG raises it and
Gres returns a row), gin JSON `Actual Rows` / `Rows Removed by Index Recheck`. All correct.
`WindowAgg` (create_index_spgist) is a renderer gap, not a planner decision.
