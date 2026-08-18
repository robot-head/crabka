# Gres executor architecture deep dive — for the planner / EXPLAIN project

Read-only analysis of `crates/pgexec` on branch `claude/pg-regress-gaps-explain-plan-4c4b57`
against the certified diffs of the latest CI run of `main` (175 failing files,
110,197 changed lines). Paths are relative to `crates/pgexec/src/` unless a crate is named.

## 0. Headline findings

1. There is no intermediate representation. A SELECT is executed by walking the parser AST
   directly. The only IR is `Relation { scope: Scope, rows: Vec<Vec<Datum>> }` (`join.rs:29`),
   a fully materialised row set, and `BoundExpr` (`bind.rs`), which is the same `Expr` AST with
   column references rewritten to `$pos.N`. Types are inferred lazily by `eval::infer_type`
   (`eval.rs:4154`), never stored on the tree. No iterator/Volcano model, no plan node type,
   no per-node instrumentation.
2. EXPLAIN is a second, independent syntactic walk (`explain.rs:77 plan_statement`) that never
   talks to the executor. `EXPLAIN ANALYZE` runs the statement through `Session::run_one` and
   takes only `rows.len()` (`session.rs:5176 explain`), then prints the syntactic tree with
   `actual rows=<n>` on the root and `0` elsewhere (`explain.rs:891`).
3. The read path never chooses among physical alternatives by cost. Hard-wired guards only:
   equality probe of a single-column local btree when the WHERE has `col = const` and the
   relation is not under RLS (`exec.rs:18151 try_scan_with_local_index`), GIN posting-list probe
   for `tsvector @@ const` (`exec.rs:11533`), a hash-index join over the materialised right side
   whenever an equality key exists (`join.rs:345 join_relations_impl` + `JoinIndex`), and three
   sharded-table pushdowns (`exec.rs:18261–18566`). Join order is FROM order. Every join prints
   as `Nested Loop`.
4. Secondary indexes are not ordered. `crabka_pgkv::key::secondary_index_entry_key`
   (`crates/pgkv/src/key.rs:248`) is `prefix ‖ u32 len ‖ rowenc::encode_row(values) ‖ rowid`,
   and `rowenc.rs:1` says "It is NOT order-preserving". A btree index is physically an
   equality-only index. Ordered `Index Scan` needs a memcomparable key encoding, which
   `keyenc.rs` explicitly defers.
5. Unordered result order is also a planner artefact. At least 3,032 changed lines are pure
   permutations of the expected rows inside one hunk (`geometry` 1,334, `window` 818, `join`
   444). `geometry`/`join`: nested-loop outer/inner choice. `window`: PostgreSQL's non-stable
   quicksort tie order plus `window.rs` returning rows in input order rather than in
   `Sort → WindowAgg` order. Aggregate output order (`agg.rs:2850` first-seen) matches neither
   PostgreSQL's HashAggregate (hash-table iteration order) nor GroupAggregate.
6. The corpus steers the planner with GUCs: 51 files toggle `enable_*` 569 times;
   `select_parallel`, `join_hash`, `partition_prune`, `partition_aggregate`, `incremental_sort`,
   `select_distinct`, `write_parallel`, `aggregates` set parallel cost GUCs, `work_mem`,
   `random_page_cost`, `debug_parallel_query`. Gres: `unrecognized configuration parameter
   "work_mem"` (4×), `invalid value for parameter "debug_parallel_query"` (9×).
7. A non-planner producer defect hides most of the EXPLAIN tests: `explain_filter`,
   `explain_mask_costs`, `explain_memoize`, `explain_parallel_append`, `explain_merge`,
   `explain_analyze*` are PL/pgSQL `RETURNS SETOF text` functions called in the select list.
   `routine.rs:2112 validate_plpgsql_scalar` refuses that (81 statements across `explain`,
   `memoize`, `incremental_sort`, `select_parallel`, `merge`, `partition_prune`, `partition_join`,
   `stats_ext`, `misc_functions`). That is `srf.rs`/`routine.rs` work and must land before the
   planner is measurable on those files.

## 1. Pipeline

```
session.rs:6732 run_one → run_one_inner (7050–7061)
  Query with locking → run_query_locking (8104) → run_select_locking (8128) → exec.rs:23808 execute_read_locking
  Query with DML CTE → run_write
  Query → run_select (7869) → run_select_inner → run_select_traced (7902): snapshot/read_ts, GC pin, SubCtx (subquery.rs:25), spawn_blocking
     → exec.rs:23750 execute_read → query.rs:7 query_to_relation → query_to_relation_with_ctes (11)
        cte.rs evaluate_with_clause (all CTEs materialised eagerly)
        Select → exec.rs:19170 select_to_relation_with_ctes
        Values → values.rs; Nested → recurse; SetOp → setops.rs set_expr_to_relation
     → query.rs:144 relation_to_rows_result
```

`select_to_relation_with_ctes` (`exec.rs:19170–19321`) order of operations:
subquery folding (`resolve_select_subqueries` 15530; `subquery.rs` doc: uncorrelated evaluated
once and rewritten to Const/InList; correlated re-bound per row by `LateralBinder`
13981–14320, `row_matches_correlated` 16725, `materialize_correlated_row_exprs` 16541) →
FROM (`build_from` 13374 → `build_table_expr` 17685 → `append_from_item` 13548, left fold; lateral
items rebuilt per outer row in `lateral_join` 13745 with an AST-keyed cache) → base relation
(`build_base_table` 17514 → `scan_stored_relation` 17188; CTE/transition/virtual catalog/view
(re-parsed and re-executed each reference)/stored table; `inherited_scan` 17133 and
`partitioned_scan` 17100 append child scans into one `RawScan` under the parent's permit and
policies) → physical scan (`scanner::collect_cursor_bounded` `scanner.rs:951` over
`RangeScanner::scan_cursor` with a `ScanRequest` `scanner.rs:198`; or `try_scan_with_local_index`
18151; whole relation in memory before WHERE, see comment 17224) → WHERE pushdown
(`push_local_where` 13612: immutable+leakproof single-side conjuncts before a join; cross-join
WHERE turned into ON so `JoinIndex` can key on it 13574; full WHERE re-evaluated after FROM 19276)
→ join (`join.rs:71/345`: hash `JoinIndex` over the right relation when a hash-safe equality key
exists, else nested loop; output order = left × candidate order; per-operator memory check) →
distributed join (`try_distributed_inner_equi_join` 17885, sharded only) → WHERE loop 19275 →
window (`window.rs execute` at 19302, owns projection when present) → grouping (`grouping.rs` →
`agg.rs:2742 aggregate_rows`, one hash pass, first-appearance group order via
`HashMap<Vec<Datum>,usize>` 2854; grouping sets by input augmentation) → projection/DISTINCT/
ORDER BY/LIMIT/SRF (`project_rows_ordered` 24110: HashSet dedup, Rust stable `sort_by` on
`key_source_rows`, `apply_row_window`, `distinct_on_plan` 24290, `srf::project_rows_ordered` for
select-list SRFs) → set ops (`setops.rs`), CTE scan (`cte.rs requalify_cte` from 17580), SRF in
FROM (17745–17794), Values (`values.rs`), cursors (`cursor.rs:1` materialise at DECLARE).

Describe is a third walk: `query.rs:59 describe_query_expr` → `exec.rs:19360
build_from_schema_of_select` → `build_table_expr_schema_with_ctes` 19419 → `resolve_projection`.
Writes (`exec.rs:3308 execute_write`) use the same builders for their sources (`build_from` at
6036/6115/6931/7779; `query_to_relation` at 6210/7062/7786/8301).

PostgreSQL decisions and where they live today: join order = FROM order; join method = hash
JoinIndex if any hash-safe equality key else nested loop (never printed); index vs seq =
`choose_local_index_equality` 18221 first single-column local btree with `col=const` if
`UnrestrictedTable`; Materialize/Memoize = lateral cache only; Sort placement = one sort after
WHERE/aggregate; Agg strategy = always hash-like; DISTINCT = HashSet; SetOp = setops.rs; partition
pruning = none (`partitioned_scan` scans every leaf); partitionwise = none; parallel = none
(`plan_dist` Gather is a distributed term); InitPlan/SubPlan = subquery folding + per-row
re-binding + `plan_correlated_scalar_lookup` 15106; CTE inlining = never; top-K into scanner =
sharded tables only (`top_k_pushdown_for_select` 18987).

## 2. Operator inventory

Seq Scan yes (`scan_stored_relation` → `collect_cursor_bounded`/`LocalRangeCursor` scanner.rs:1310).
Index Scan partial (`try_scan_with_local_index` + `lookup_local_index_equal` exec.rs:11509:
single-column local btree equality, unordered, always heap-fetches). Index Only Scan no
(entries carry no visibility; `visible_rows_for_rowids` 11601 always reads versions; INCLUDE
refused, 27 errors). Bitmap Index/Heap, BitmapAnd/Or partial seed (`gin_candidate_rowids` 11555
builds `BTreeSet<u64>` rowid sets and unions/intersects them; heap side = `visible_rows_for_rowids`).
Tid Scan / Tid Range Scan no (ctid derived in `scope.rs:153 row_ctid`; `row_key(table.id,rowid)`
allows a direct seek). Sample Scan partial (`apply_tablesample` 17806 samples materialised rows;
not page-based). Function Scan yes (`srf::from_item`, `routine::table_function_expansion`).
Values Scan yes. CTE Scan yes (materialised). WorkTable Scan / Recursive Union yes (`cte.rs`
fixpoint; SEARCH/CYCLE refused, 36 errors). Subquery Scan yes (17733). Named Tuplestore Scan yes
(17582). Table Function Scan yes (`jsontable::from_item`). Foreign Scan yes (17226).
Nested Loop partial (nested-loop branch of `join_relations_impl`; no semi/anti — EXISTS/IN are
folded, never turned into joins). Hash Join partial (`JoinIndex` over the RIGHT relation
regardless of size; no `Hash` node; no batches; PostgreSQL emits LIFO bucket-chain order).
Merge Join no. Hash no. Materialize partial (everything materialised; lateral cache). Memoize
partial (lateral cache keyed by specialised AST, `lateral_join` 13775). Sort yes (`sort_by`,
Rust stable — PostgreSQL's pg_qsort tie order differs). Incremental Sort no. Aggregate/
GroupAggregate/HashAggregate/MixedAggregate partial (`agg.rs:2742` one hash pass; grouping sets by
augmentation; no Partial/Finalize). Group no. WindowAgg yes (`window.rs`; must emit in sorted
order — today input order). Unique partial (`keep_first_per_distinct_on_group`). SetOp/HashSetOp
partial (`setops.rs`). Limit yes (`apply_row_window`). LockRows yes (`execute_read_locking`;
joins under FOR UPDATE refused 23886). Result yes. ProjectSet yes (`srf::project_rows_ordered`).
Append/Merge Append partial (`inherited_scan`/`partitioned_scan` concatenate; no pruning; child
order = KV key order defect noted in the plan doc). Gather/Gather Merge no. ModifyTable yes
(`execute_write`). BitmapAnd/BitmapOr seed only.

## 3. Index read path

* `index_entries` (`exec.rs:11240`): btree → one entry of the key tuple; GIN → one entry per
  tsvector lexeme; Hash/GiST/SP-GiST → NO entries; any expression key → no entries. `brin`
  refused (36 errors). Expression indexes catalog-only (17 errors). Partial indexes refused (42).
  INCLUDE refused (27). `Index` (`crates/pgcatalog/src/lib.rs:397`) has no per-column direction,
  opclass, collation, predicate, include list.
* Key layout `[table_id][index_id+INDEX_PRIMARY][u32 len][encode_row(canonical values)][rowid]`;
  `scan_prefix` answers only exact-equality. No range, no leading-column prefix, no order.
* Reads consulting an index: only `try_scan_with_local_index` (18151), reached from
  `scan_stored_relation` when not sharded, no partial aggregate pushed, `UnrestrictedTable`:
  (a) `tsvector @@ const` via `choose_local_gin_index`; (b) `col = const` via
  `choose_local_index_equality` on the first single-column local btree. All else full scan.
* MVCC coupling: entries are bare keys; probe collects rowids then `visible_rows_for_rowids`
  reads version chains and rechecks values (11526). Every index read is a bitmap-heap-with-recheck.
  No visibility map, so Index Only Scan (`Heap Fetches: 0`) is not honestly available.
* Needed: (1) `crates/pgkv/src/keyenc.rs` memcomparable encoding for indexable Datums (C/POSIX
  text; en_US would need collation keys), NULLS/DESC by inversion; drop the length prefix; add
  `secondary_index_range`; rebuild via `local_index_backfill_ops` (`exec.rs:10060`).
  (2) `pgcatalog` Index key options/predicate/include; `pg_index.indkey/indoption/indexprs/indpred`.
  (3) `pgexec` IndexPath builder (indxpath.c port using existing `builtin_opclasses.rs`/
  `builtin_opfamilies.rs`), ordered index cursor in `scanner.rs` (local KV only;
  `IndexPlacement::Global` excluded at first), bitmap path over rowid sets. (4) Merge Join /
  ORDER BY via index / Backward / Merge Append follow from (1)+(3).
* `hash` method entries: one-line change in `index_entries`. GiST/SP-GiST/BRIN scans: separate XXL.

## 4. Memory policy

* Flag `crates/gres/src/lib.rs:857` (CI 20MiB, `scripts/gres-pg-regress.sh:267`) →
  `SqlEngine.blocking_query_memory` → `Session` → `SubCtx.blocking_query_memory` →
  `JoinPolicy.memory` (`subquery.rs:171`). Per-operator cap on retained bytes raising 53200,
  no spill. Applied by `collect_cursor_bounded` (each scan), `push_bounded_join_row`/
  `JoinIndex::build`, `count_join_rows`, lateral cache, `key_source_rows` (sort),
  `ensure_blocking_rows_fit` (DISTINCT), `agg.rs:2872`, `grouping.rs:496`, `cte.rs:492`,
  `setops.rs:353`, `srf.rs:2321`.
* Inconsistency: sort/DISTINCT/aggregate/grouping/CTE/setops/SRF sites use the compile-time
  `scanner::BLOCKING_QUERY_MEMORY` (16 MiB, `scanner.rs:939`), not the flag.
* 104 statements fail with 53200 (`type_sanity` 23, `opr_sanity` 18, `tuplesort` 16, `join` 10,
  `portals` 5, `limit` 4 …).
* Planner interaction: `work_mem` feeds cost_sort, hash-vs-sort agg choice
  (`hash_mem_multiplier`), `ExecChooseHashTableSize` batches (`join_hash`), Memoize size, bitmap
  lossiness. Recommendation: `work_mem` GUC for cost model + ANALYZE text; statement-level hard
  cap for the flag (raised for the regress run so `opr_sanity` fits); keep the RangeScanner
  scan cap; real spill is a later optional operator feature.

## 5. Distributed constraints

Leaf scans via `RangeScanner` (`scanner.rs:716`: scan/scan_cursor/join/join_strategy),
implemented across ranges in `crates/gres-ranges/src/forward.rs:3260`. Pushdown contract is
`DistributedScanPlan` (`plan_dist.rs:336`). `plan_dist::plan_join` chooses
Broadcast|CoPartitioned|Gather from byte estimates, sharded tables only. Constraints: every
stored-relation leaf must be expressible as a `ScanRequest`; Index Scan leaf is local-only;
sharded reads carry the statement read timestamp (`TimestampedRangeScanner` scanner.rs:1146), no
per-node snapshot; `try_distributed_inner_equi_join` becomes one physical join path for two
sharded base tables (regress has no sharded tables); RLS/privilege proofs gate pushdowns
(`rls::sanitize_scan_plan`, exec.rs:17272) and must ride on the leaf. Keep as `plan/dist.rs`.

## 6. Insertion point

Replace `select_to_relation_with_ctes` and callees with bind → plan → execute:
`execute_read` → `plan::bind_query` (QueryExpr → Query) → `plan::plan` (Query → Plan) →
`plan::execute` (Plan → rows); `session.rs:5176 explain` calls the same bind+plan then renders
from Plan or from an instrumented PlanState. Reusable: `Scope`/`ColumnBinding` (per-RTE Var
namespace; keep `$pos` binding), `BoundExpr` + `eval::eval` + `infer_type` (add a cached type),
subquery folding / LateralBinder / scalar lookups (→ InitPlan/SubPlan), `join.rs` JoinIndex/
JoinCondition/outer markers (Hash node body + nestloop predicate), `agg.rs` Acc/AggSpec,
`grouping.rs` augmentation, `window.rs execute`, `setops/values/srf/jsontable/cte`,
`scan_stored_relation`/`inherited_scan`/`partitioned_scan`/`RawScan`/`apply_row_security`/
`ReadPermit` (only way to build a scan leaf), `plan_dist`, `explain.rs` deparser (feed it typed
bound exprs so constants print `'-3'::integer`, known gap `explain.rs:676`), render_json/yaml/xml
envelopes; delete the describe walk. Invariants: RLS default-deny via `RawScan` single exit
(`rls.rs:1–47`), parent policies for trees; leakproof pushdown (`push_local_where` 13612) →
`security_level` on RestrictInfo; `ReadPermit::acquire` before any read (`privilege.rs:542`),
`ReadPermit::inherited` for tree children; snapshot/read_ts/GC pin statement-level;
`check_query_canceled` at operator boundaries.

## 7. Proposal (phased)

Modules under `crates/pgexec/src/plan/` (not a new crate at first — needs pub(crate) Scope/eval/
RLS types): `mod.rs`, `query.rs` (Query IR: RangeTbl entries, TargetEntry, Var{rti,attno,ty},
RestrictInfo{clause,is_pushed_down,security_level,leakproof,required_relids}), `bind.rs`,
`rewrite.rs` (view expansion, RLS qual injection, CTE inlining, sublink→semi/anti join, pull-up,
reduce_outer_joins, const folding), `stats.rs` (relpages/reltuples + pg_statistic-like data,
`compute_scalar_stats`, `estimate_rel_size` fallback), `selfuncs.rs`, `cost.rs` (all cost_*,
disabled_nodes, planner GUCs), `paths.rs` (RelOptInfo, Path, add_path, pathkeys, equivalence
classes), `indexpath.rs`, `joinpath.rs`, `grouping.rs`, `partition.rs`, `parallel.rs`,
`createplan.rs` (set_plan_references), `exec/` (Volcano PlanState with per-node counters),
`explain.rs`, `deparse.rs`, `dist.rs`. Core types: Query, RangeTblEntry, RestrictInfo,
EquivalenceClass, PathKey, RelOptInfo, Path, Plan (one variant per node), PlanState, PlannedStmt.

Phase 0 (prereqs, non-planner): user SRF in select list (`routine.rs`/`srf.rs`, M) — unblocks ~81
EXPLAIN-wrapper statements; full planner GUC surface (M); `window.rs` output order (S).
Phase 1 (XL): Query IR + single-relation plan tree executed for real; ANALYZE counters; VERBOSE
Output; full JSON/YAML/XML keys; SUMMARY. Buys single-relation EXPLAIN text, ~1,119 `Output:`
lines, `Rows Removed by Filter`; strict refactor with old path deleted.
Phase 2 (XXL): stats + cost model + base-rel paths + join search; ordered index encoding in pgkv
(L, parallel); Hash/Merge/NestLoop/Materialize/Memoize; semi/anti; Index/Bitmap/Tid leaves.
Buys the bulk of `join` (4,608 EXPLAIN + 444 reorder), `subselect` 1,264, `aggregates` 691,
`create_index` 789, `union` 591, `join_hash` 322, `updatable_views` 340, `rowsecurity` 946,
`inherit` 989, plus nested-loop reorder lines (`geometry` 1,334). Only 46 unmasked `cost=` lines
exist (`explain` 30, `misc_functions` 14) so decision fidelity, not numeric fidelity, is the target.
Phase 3 (L): upper rels — Hash/Group/Mixed agg, Incremental Sort, WindowAgg Run Condition,
Unique-vs-Hash, SetOp/HashSetOp, LockRows, top-N. Buys `window` 531+818, `groupingsets` 703,
`incremental_sort` 582, `select_distinct_on`, `tuplesort`.
Phase 4 (L): partition pruning (`Subplans Removed`), partitionwise join/agg, Merge Append,
OID-order children. Buys `partition_join` 3,726, `partition_prune` 3,325, `partition_aggregate`
1,079, `inherit` rest (~8k) — after producer defects in those files are fixed by other slices.
Phase 5 (M planner): plan parallel, execute serially, account as if parallel. Evidence
`select_parallel.out:583–598`: `Parallel Seq Scan on tenk1 (actual rows=1960.00 loops=50)` =
ntuples/nloops with nloops = outer × (workers+1); EXPLAIN prints averages, so a serial executor
that sets nloops that way prints identical text; `Workers Planned/Launched: N` and `Worker N:
Sort Method` are plan-time strings (memory masked). Trade-off stated: the plan claims parallelism
that does not happen; no wall-clock benefit; Launched always equals Planned. Honest enough for
the corpus (all 59 Workers lines in select_parallel are Planned/Launched: 4). Real parallel
execution can replace the accounting later. Buys `select_parallel` 558, `write_parallel`, ~350
embedded parallel lines elsewhere.
Phase 6 (XXL, optional): GiST/SP-GiST/BRIN/hash index scans (`create_index_spgist` 607, `gist`,
`brin*`, `hash_index`).

EXPLAIN rendering: `ExplainState` with all option flags (extend `ExplainOptions`,
`parser.rs:6095` currently drops buffers/wal/timing/summary/settings/generic_plan/memory/
serialize); render headline (`Node Type on rel alias`, `using idx`, `Backward`, `Parallel`,
`Partial/Finalize`), costs, `(actual time= rows= loops=)` with two-decimal rows, ordered detail
lines (Output, Index Cond, Recheck Cond, Filter, Join Filter, Hash Cond, Merge Cond, Sort Key,
Presorted Key, Group Key, Cache Key, One-Time Filter, Rows Removed by …, Heap Fetches, Sort
Method, Buckets/Batches/Memory Usage, Workers, Subplans Removed, Disabled: true), InitPlan/
SubPlan children, `->` children, Planning/Execution Time, Triggers, JSON/YAML/XML full key set
(Startup/Total Cost, Plan Rows/Width, Actual*, Disabled, Buffers zeros, Planning block, Triggers []).

Dead in `exec.rs`: roughly 13300–19330 and 24100–24900 — build_from, append_from_item,
push_local_where, leakproof/immutable predicate helpers, filter_relation, is_lateral_item,
lateral_join, lateral_cacheable*, LateralBinder + the correlated-subquery rewriting family
(plan_correlated_*, install_lazy_initplans, resolve_select_subqueries,
fold_correlated_lazy_expressions, materialize_correlated_row_exprs,
replace_subqueries_with_typed_nulls …), try_execute_partial_aggregate_pushdown,
try_execute_local_streaming_aggregate, try_execute_local_join_count, single_table_scan_plan,
top_k_pushdown_*, select_to_relation_with_ctes, project_rows_ordered, key_source_rows,
distinct_on_plan, keep_first_per_distinct_on_group, apply_row_window, the describe walk
(build_from_schema_*, build_table_expr_schema_with_ctes, lateral_schema_item,
query.rs::describe_query_expr*), explain.rs::plan_* (keep the deparser). join.rs join_relations*
and count_join_rows become Hash/NestLoop node bodies; subquery.rs folding becomes InitPlan
evaluation. ~8–10k lines removed from exec.rs.

## 8. Files and risks

Files: `crates/pgexec/src/plan/` (new); `exec.rs` (execute_read/execute_read_locking/
execute_write call the planner; delete read-path region); `session.rs` (explain 5176,
run_select_traced 7902, GUC registry via set_guc 7077); `explain.rs` → plan/deparse.rs; join.rs,
agg.rs, grouping.rs, window.rs, setops.rs, srf.rs, values.rs, cte.rs, subquery.rs re-homed under
plan/exec; scanner.rs (streaming cursor to Seq Scan node, ordered index cursor); relstats.rs →
plan/stats.rs + ANALYZE in run_maintenance; crates/pgkv/src/{key,keyenc,rowenc}.rs;
crates/pgcatalog/src/lib.rs Index/NewIndex; routine.rs, srf.rs (phase 0); crates/pgparser
ast.rs ExplainOptions + parser.rs:6059; docs/PG_COMPAT_MATRIX.md:274.

Parallel lanes (no overlapping files): A Query IR+bind+createplan+exec skeleton
(plan/{query,bind,rewrite,createplan,mod}.rs, plan/exec/*; only lane that edits exec.rs);
B stats+selfuncs+cost+paths+joinpath+indexpath (pure functions, unit-testable like join.rs);
C index storage (pgkv key/keyenc, pgcatalog Index, exec.rs write-path index_entries/backfill —
coordinate the exec.rs touch with A or do it via a new module); D EXPLAIN renderer + deparser +
parser ExplainOptions; E phase-0 SRF-in-select-list + GUC surface + window order (routine.rs,
srf.rs, session.rs GUC table, window.rs); F partition planning (plan/partition.rs,
partition.rs, inheritance.rs) after A; G parallel accounting (plan/parallel.rs,
plan/exec/gather.rs) after A,B. Freeze Plan/PlanState/Executor trait in one small commit first.

Risks: (1) cost fidelity is a page-count problem — no pages in Gres; synthesise relpages from
tuple width, but dead tuples/HOT/fillfactor drift after UPDATE/DELETE; keep a heap-page
simulator and PG's `estimate_rel_size` fallback. (2) statistics fidelity (`compute_scalar_stats`,
`estimate_num_groups`, `eqjoinsel`, extended stats — 143 `extended planner statistics objects
are not supported` errors). (3) unordered output order beyond nested loops: hash join LIFO
bucket chains, HashAggregate simplehash iteration order, UNION dedup order — needs PG hash
functions (`hash_any`, `hashint4`, `hashtext`) and simplehash growth policy; distinct L item,
~1–2k lines stay red without it. (4) Sort tie order: port pg_qsort (M). (5) exec.rs 39k-line
serialisation bottleneck — lane A owns it. (6) RLS/privilege regressions — keep RawScan/
UnrestrictedTable/ReadPermit as sole constructors of scan leaves/pushdown paths. (7) distributed
regressions — planner always prefers the sharded pushdown paths until costed. (8) memory-policy
change observable by soak harness. (9) no autovacuum in Gres = un-analysed estimates must match
PG's un-analysed defaults (advantage). (10) cursors: SCROLL needs Materialize under the portal.

## 9. Planner-only estimate

EXPLAIN bucket: 15,520 (`PG_COMPAT_MATRIX.md:37`) vs 27,138 (`scratchpad/classify.py`, includes
rulers inside plan hunks). Expected-side node inventory in the diffs: Seq Scan 2,654, Hash 1,039,
Sort 966, Append 494, Index Scan 420, Result 346, Index Only Scan 340, Bitmap Index Scan 216,
Hash Join 196, Nested Loop 186, Nested Loop Left Join 185, Bitmap Heap Scan 183, Materialize 158,
Parallel Seq Scan 142, Hash Right Join 92, Gather 91, WindowAgg 75, HashAggregate 76,
GroupAggregate 58, Incremental Sort 52, Merge Append 52, Unique 51, Partial HashAggregate 48,
Merge Join 37, Memoize 36, ProjectSet 37, Gather Merge 34, … ; Index Cond/Hash Cond/Merge Cond/
Recheck Cond/Cache Key 1,583; Output: 1,119; actual rows 459; unmasked cost= 46. Estimate:
24k–27k lines planner-only EXPLAIN text + ≥3.0k pure-reorder lines caused by plan shape + part
of Output: lines ≈ 27k–30k of 110,197 (25–27 %). The rest of the EXPLAIN-bearing files is
cascade (explain_filter SRF, `relation does not exist` in partition_join/partition_prune,
missing GUCs) attributable to concrete non-planner roots.

## 10. Brief corrections

* "the read path does not consult secondary indexes for ordinary scans": nearly right — it
  consults a local single-column btree for `col = const` and a GIN for `tsvector @@ const`, only
  when not under RLS and not sharded (`exec.rs:18151`); never ranges/ORDER BY/joins/multi-column.
* EXPLAIN ANALYZE prints `actual rows` on the root only, `loops=1` everywhere, no timing, no
  Planning/Execution Time (`explain.rs:891–895`).
* `read_gate.rs` is the linearizable-read `Linearizer` seam, unrelated to planning.
* `plan_dist.rs` `Gather` is a distributed strategy, not PostgreSQL's Gather node.
* The memory flag reaches scans and joins only; sorts/DISTINCT/aggregates/CTEs/setops/SRF use the
  hard-coded 16 MiB (`exec.rs:24253`, `24427`, `agg.rs:2872`, `grouping.rs:496`, `cte.rs:492`,
  `setops.rs:353`, `srf.rs:2321`).
* `explain`, `memoize`, `incremental_sort` and the `explain_*` wrapper calls elsewhere fail on
  `routine.rs:2112` (user SRF in the select list), a non-planner defect.
