# pg_regress 231/231 — synthesis of the triage (2026-08-18)

Inputs: 16 cluster ledgers (analysis/*/findings.md + StructuredOutput), 36 adversarial verify notes
(analysis/verify/*.md), the EXPLAIN census (analysis/explain_census), the executor architecture
report (analysis/executor_architecture/report.md), file_stats.json (175 files, 110,197 changed lines).
Counting rule: whole change block attributed to the producing root; a one-line Gres error that
replaces a 30-row result costs 31 lines. Numbers below are +-30 % (verify notes recounted every
large root and landed within that band).

## 1. Ledger summary

Total: 110,197 changed lines in 175 failing files (56 exact).

By kind (corpus-wide, verified counts where a verify note exists):

| kind | lines | notes |
|---|---:|---|
| Cost-planner-only (plan-node choice, join order/method, index/bitmap/tid access, Materialize/Memoize/Sort/Incremental Sort, parallel, partitionwise, row estimates) | ~23,000 | census: 15,971 visible inside QUERY PLAN blocks + ~2,050 join cross-join row order + ~3,500 hidden behind cascade producers (join_hash 700, select_parallel 900, memoize 240, explain 165, partitioning 1,182, indexes 433) + stats_ext estimated-selectivity rows 762 + ~250 misc |
| Deterministic plan-shape / EXPLAIN renderer (needs a plan tree and typed deparse but no cost model) | ~6,600 | structural transforms 1,835 (join removal, SJE, pull-up, One-Time Filter), partition Append/prune 2,430, VERBOSE Output ~1,100, InitPlan/SubPlan/CTE/WindowAgg/ProjectSet/Merge/Conflict nodes, JSON/YAML/XML field sets, EXPLAIN EXECUTE/CTAS dispatch |
| Reorder lines fixable by a join-side heuristic (larger post-filter side outer) | ~1,340 | geometry 1,334, interval 4 — not planner-only under the brief's rule (verify dgr-planner-only) |
| Sort tie order (pg_qsort port + window ordering) | ~1,200 | window 1,196 (+ small counts elsewhere) |
| Everything else: parser, executor, catalog, DDL, types, functions, subsystems absent (text search, rules, FDW, pubsub, LO, stats system, jsonpath, xml, ...) | ~78,000 | see workstreams |

Planner separation, per cluster (cost-planner-only lines): planner_join 10,900 (+1,835 structural),
partitioning 5,817 (+2,430 shape), agg_window 1,577, indexes_storage 1,809 (+433 hidden),
datetime_geometry_ranges 297 (+1,338 heuristic), scalar_types 286, security_roles 337
(rowsecurity: 365 planner + 463 shape + 228 shape-after-INHERIT), txn_session 360, views_rules 614,
statistics ~870, dml ~307, ddl_objects ~117, constraints_triggers_plpgsql ~59, json_xml 48,
textsearch 80, catalog_psql 0.

Files that can become exact WITHOUT any cost-based planner: 93 of the 175 (plus the 56 already exact
= 149/231). List: select_into, tidrangescan (rule-based Tid Range Scan + typed deparse),
partition_info, hash_part, create_aggregate, polymorphism, functional_deps, tsrf (VERBOSE Output only),
psql, psql_pipeline, type_sanity, opr_sanity, misc_sanity, oidjoins, largeobject, constraints,
without_overlaps, triggers, replica_identity, date, timestamp, timestamptz, horology,
multirangetypes, alter_generic, alter_operator, create_operator, create_cast, create_procedure,
create_type, typed_table, create_table_like, drop_if_exists, object_address, dependency,
event_trigger, insert, copy, copy2, copydml, copyencoding, truncate, hash_func, amutils, reloptions,
tablespace, indirect_toast, compression, random, json, jsonpath, jsonpath_encoding, jsonb_jsonpath,
sqljson, sqljson_queryfuncs, xml, xmlmap, name, text, float8, uuid, enum, strings, numerology,
encoding, conversion, errors, collate, collate.utf8, create_role, password, guc, database,
namespace, sysviews, misc, stats_import, tsdicts, tstypes, foreign_data, publication, subscription,
transactions, combocid, lock, advisory_lock, prepared_xacts, temp, vacuum, prepare, sequence,
identity, rules (5 marginal lines). geometry joins them once the join-side heuristic lands.
The other 82 files need the planner programme for at least part of their lines; of these, 25 files
have fewer than 100 planner-only lines.

Whole-file / dominant cascades (producer -> victims): SRF-in-select-list (routine.rs:2112/2609) ->
select_parallel 1,012, explain 509, memoize 242, incremental_sort 119, plus explain_* wrappers in
merge/partition_prune/misc_functions/stats_ext; UPDATE pg_class -> join_hash 744; blocking memory
budget -> portals 725, transactions 61, tuplesort 438, type_sanity 122, opr_sanity 100, psql 48;
create_view interpt_pp -> select_views 901; drop table cascade of inheritance children -> inherit
~330; expression partition keys -> partition_prune 589, partition_join 561, inherit 220,
partition_aggregate 99, insert 354, update 250, copy 12, and the create_table DROP failure that
leaks parted/list_parted2/range_parted2/part1..4 into alter_table (~150), triggers (51), merge (17),
inherit (16); publication/subscription (grammar) whole files; largeobject (lo_create) whole file;
oidjoins (pg_get_catalog_foreign_keys) whole file; tsdicts (ispell template) 594; prepared_xacts
(missing max_prepared_transactions GUC) whole file; combocid (cmin) whole file; uuid (assignment
cast) 165; encoding/conversion (regress C functions) 172/430; collate.utf8 (builtin provider) 168;
typed_table, alter_operator, create_am, object_address (function missing) whole/most; plpgsql
(bpchar NEW/OLD field types) 570; foreign_key (partitioned FK refusal) ~900; index_including(_gist),
brin*, gin (CREATE INDEX refusals); compression_1 (COMPRESSION grammar) 230; database, namespace.
Cross-file poison: select_into's `INSERT INTO int4_tbl SELECT 1 INTO f` adds row 1 to int4_tbl for
every later file (~450 lines in join/subselect/union/join_hash/aggregates/with/polymorphism);
constraints leaks atacc1 into alter_table; stats leaks test_io_local into tablespace; list_parted/
range_parted leak into inherit.

## 2. Workstreams

Sizes: S hours, M a day, L several days, XL 1-2 weeks, XXL multi-week. "lines" = estimated changed
lines that flip once the workstream and its listed dependencies land (whole-block, +-30 %). File sets
are the verified fix locations; exec.rs (39k lines), session.rs (25k), parser.rs (16k+) are the
shared bottlenecks and are annotated with the region/function each workstream may touch.

### Enablers (batch 0/1)

- N00 exec.rs carve-out (S-M, 0 lines, certify alone, OPTIONAL): mechanically move DDL families out of
  crates/pgexec/src/exec.rs into ddl_index.rs / ddl_partition.rs / ddl_inherit.rs / ddl_alter.rs /
  catalog_rows.rs (no behaviour change) so later workstreams do not share one 39k-line file. If skipped,
  every batch below carries exec.rs region overlaps (listed per batch).
- N01 SELECT ... INTO rejected in nested contexts (S, ~450 lines: select_into 12 + int4_tbl poison in
  join ~80, subselect ~37, union 13, join_hash 6, aggregates 14, with 10, polymorphism, ...). Roots
  planner_join-select-into-nested-contexts, agg R25. Files: crates/pgparser/src/parser.rs
  opt_select_into (12427) / finish_query_statement / query_statement.
- N02 blocking-query memory policy (M, ~2,000 lines: tuplesort 438, join ~190, limit ~70, memoize 54,
  tidscan 18, subselect ~20, portals 696, transactions 61, type_sanity 122, opr_sanity 100, psql 48,
  agg R24 64, brin*/partition_join ~80; certify alone — observable by the soak harness). Roots
  planner_join-blocking-memory-budget, txn-x-blocking-memory-budget, agg R24. Route the nine
  compile-time users of scanner::BLOCKING_QUERY_MEMORY (exec.rs key_source_rows 24225 /
  ensure_blocking_rows_fit 24423 / 18505, agg.rs 2872, grouping.rs 496, cte.rs 492, setops.rs 353,
  srf.rs 2321, join.rs JoinPolicy::default) through the policy value; make the cap statement-level
  with a work_mem GUC; per-conjunct WHERE->ON pushdown in exec.rs append_from_item (13574) and a filter
  into lateral_join (13595); let try_execute_local_join_count run with system columns (exec.rs
  19243). Files: crates/pgexec/src/{scanner.rs, exec.rs [key_source_rows, ensure_blocking_rows_fit,
  append_from_item, lateral_join, 19243], join.rs, agg.rs, grouping.rs, cte.rs, setops.rs, srf.rs
  [MemoryBudget only]}, crates/gres/src/lib.rs:857, scripts/gres-pg-regress.sh:267.
- N03 user SETOF functions in the select list + built-in SRF with GROUP BY + ARE regex escapes (M,
  ~300 direct; gates ~2,900 planner/EXPLAIN lines in select_parallel/explain/memoize/incremental_sort/
  merge/partition_prune/misc_functions/stats_ext). Roots planner_join-srf-user-functions-in-select-list,
  part-srf-in-select-list, planner_join-regex-are-word-boundaries, agg R6 (GROUP BY part). Files:
  crates/pgexec/src/routine.rs validate_plpgsql_scalar (2112) / inline_scalar_call (2589-2612),
  crates/pgexec/src/srf.rs classify/is_set_returning/expr_contains_srf/rewrite_expr/plan/
  reject_in_aggregate (built-in SRF as grouping key -> ProjectSet below Agg), crates/pgexec/src/
  regexp_fn.rs compile_pattern (472) (\m \M \y \Y \A \Z -> regex crate \b{start}/\b{end}).
- N04 GUC surface (S-M, ~500 lines: prepared_xacts 286 with max_prepared_transactions=0, guc 149,
  namespace 27, explain 4, select_parallel ~20, work_mem 4x, ...). Roots planner_join-guc-gaps,
  sec-guc-registry-and-session-state, sec-guc-runtime-scope, txn-prepared-transactions-guc. Files:
  crates/pgexec/src/session.rs GUC table (~976-1360, guc_enum 999) only: every planner GUC (enable_* x20,
  work_mem, hash_mem_multiplier, seq/random_page_cost, cpu_*_cost, parallel_*_cost,
  min_parallel_*_scan_size, max_parallel_workers[_per_gather], debug_parallel_query bool spellings,
  jit*, track_io_timing, compute_query_id, plan_cache_mode, geqo*, join/from_collapse_limit,
  constraint_exclusion, enable_partition_pruning, enable_partitionwise_*, default_statistics_target,
  effective_cache_size, cursor_tuple_fraction, max_prepared_transactions=0, role in SET clauses,
  intervalstyle/password_encryption HINT texts).
- N05 writable pg_class + relpages storage (M, ~100 direct; unblocks join_hash 744 which then needs the
  planner). Roots planner_join-writable-pg-class, stats pg-class-relpages-defaults. Files:
  crates/pgexec/src/exec.rs execute_write_body Statement::Update arm (6453) + pg_class_rows/PgClassRow
  (~20380/20826), crates/pgexec/src/relstats.rs (new relpages/relallvisible keyspace), superuser gate.
- N06 harness dbname (S, ~144 lines: psql 132, sequence 4, rules 8). Files: scripts/gres-pg-regress.sh
  (GRES_DB -> regression).

### Planner / EXPLAIN / statistics / index programme (P-series; details in section 4)

- P0h Plan IR header commit (S): freeze Plan/PlanState/PlannedStmt/RangeTblEntry/RestrictInfo types and
  the Executor trait in crates/pgexec/src/plan/{mod,query}.rs before P0a/P0b/P2/P3 start.
- P0a Query IR + bind + node-tree executor (XXL, ~500 direct lines: describe-walk errors in rowsecurity
  68, rangefuncs B 492 partly, aggregates ARRAY-subquery; enables everything else). Files: NEW
  crates/pgexec/src/plan/{mod,query,bind,rewrite(skeleton),createplan}.rs, plan/exec/*, exec.rs READ
  PATH ONLY (execute_read 23750, execute_read_locking 23808, select_to_relation_with_ctes 19170 and the
  region 13300-19330 / 24100-24900 deleted at the end), query.rs, session.rs run_select_traced (7902)
  + explain (5176) call sites; join.rs/agg.rs/grouping.rs/window.rs/setops.rs/values.rs/srf.rs/cte.rs/
  subquery.rs are CALLED as node bodies, not moved or renamed (so N16-N19 can run in parallel).
  Certify alone at each milestone. Absorbs N11 (lateral binder) if it lands first.
- P0b EXPLAIN renderer + typed deparser + ExplainOptions (L, ~3,000 lines: VERBOSE Output ~1,100 across
  join 418/subselect 174/with 76/returning 49/rangetypes 97/sqljson 105+56/tsrf 67/fast_default 52,
  planner_join-explain-renderer-gaps 250, explain-typed-deparse 200, explain-utility-formats-options
  ~500 non-planner, insert_conflict 89, limit 57, agg R23 deterministic part, rowsecurity pure shape 463
  incl. RLS qual deparse order, EXPLAIN ANALYZE per-node actuals/(never executed)/Rows Removed).
  Files: crates/pgexec/src/explain.rs -> plan/explain.rs + plan/deparse.rs (keep deparse_with,
  plan_sort_key, cast/subscript parenthesisation), crates/pgparser/src/ast.rs ExplainOptions (1805) +
  parser.rs explain (6059-6095: buffers/wal/timing/summary/settings/generic_plan/memory/serialize),
  session.rs explain option handling + EXECUTE/DECLARE/CTAS/CREATE MATERIALIZED VIEW dispatch,
  eval.rs infer_type reuse.
- P1 rule-based planner transforms (XL, ~2,500 lines: planner_join-plan-structural-transforms 900-1,835,
  predicate 200, window deterministic 398 shared with P5, union UNION ALL pushdown, join removal/SJE/
  pull-up/sublink->semi-anti/NullTest/const-fold/IN->=ANY/alias numbering/Var qualification/One-Time
  Filter; executor must gate const quals once — subselect tattle NOTICE count). Files: crates/pgexec/
  src/plan/rewrite.rs, plan/bind.rs (nullability, RTE identity), plan/deparse.rs (qualify iff >1 RTE).
- P2 statistics subsystem (XL-XXL, ~2,000 direct: stats_ext 384+69+762, stats_import 645, pg_stats refs
  ~60, CREATE STATISTICS refusals 143 sites; prerequisite for P4 cost fidelity). Roots stats-ext-*,
  stats-import-*, PLANNER-row-estimate, cat-pg-statistic, statistics-import-functions. Files: NEW
  crates/pgexec/src/plan/stats.rs (compute_scalar_stats, estimate_rel_size, relpages emulation),
  plan/selfuncs.rs (eqsel/scalarltsel/eqjoinsel/nulltestsel/estimate_num_groups, DEFAULT_*),
  relstats.rs, catalog_rel.rs (pg_statistic, pg_stats, pg_statistic_ext[_data], pg_stats_ext), NEW
  stats_fn.rs (pg_restore_relation_stats/pg_restore_attribute_stats/pg_clear_* with a notice sink on
  EvalCtx — clock.rs), session.rs run_maintenance ANALYZE (~5299), exec.rs CREATE STATISTICS DDL arm +
  ALTER COLUMN SET STATISTICS, parser.rs CREATE STATISTICS (qualified names).
- P3 index storage: ordered keys + full index catalog (L-XL, ~700 direct: index_including 256+83,
  partial 42+19, DESC/NULLS FIRST 5, unique expression 4, nulls-not-distinct 31, opclass params 23,
  xc-index-ddl 210 shared, gin generic opclasses 30, hash entries; prerequisite for every Index/Bitmap
  node in P4; certify alone — index key format rebuild). Roots idx-*, part-partial-index,
  part-include-index, planner_join-xc-index-ddl, index_read_path findings. Files: crates/pgkv/src/
  {key.rs 232-257, keyenc.rs (memcomparable), rowenc.rs}, crates/pgcatalog/src/lib.rs Index/NewIndex
  (397) + serde.rs, crates/pgexec/src/exec.rs [index_entries 11240, local_index_backfill_ops 10060,
  CREATE INDEX validation 1419-2686 incl. 2514 partial / 2523 DESC / 2564 unique-expr / 1426 INCLUDE /
  2778 GIN], scanner.rs (ordered index cursor, local only), catalog_rel.rs pg_index indkey/indoption/
  indexprs/indpred/indnkeyatts, catalog_fn.rs IndexDef (INCLUDE/WHERE text).
- P4 planner core (XXL, ~11,700 lines: cost model, path generation, join search, index/bitmap/tid
  paths, Hash/Merge/NestLoop/Materialize/Memoize operators, semi/anti from sublinks, hash join LIFO
  bucket order + HashAggregate simplehash iteration order + PG hash functions, join-side heuristic
  reproducing geometry 1,334, planner_join-tid-scan-access-path, equivclass, select, limit,
  select_distinct*, subselect, aggregates planagg, create_index btree plans, btree_index, union
  SetOp/HashSetOp, updatable_views/select_views/rowsecurity/txn planner lines; certify alone).
  Files: plan/{cost,paths,indexpath,joinpath,pathkeys,equivclass}.rs, plan/exec/{hashjoin,mergejoin,
  nestloop,material,memoize,indexscan,bitmap,tidscan,sort}.rs, join.rs (JoinIndex as Hash body),
  scanner.rs (index cursor consumers), plan/dist.rs (from plan_dist.rs), exec.rs try_scan_with_local_index
  (18151-18245) removed.
- P5 upper-relation planning + sort fidelity (L-XL, ~3,200 lines: agg R8 sort tie order 1,196 via
  pg_qsort/sort_template.h port + one global sort per window + select_active_windows order, R9 grouped
  output order 73, groupingsets 685, window 197 cost + Run Condition + optimize_window_clauses,
  incremental_sort 499, tuplesort 90, select_distinct 150, select_distinct_on 70, aggregates
  Hash/Group/Mixed/Partial/Finalize ~300, LockRows, top-N; certify alone — replaces stable sorts).
  Files: plan/grouping.rs (new), plan/exec/{agg,windowagg,incsort,unique,setop}.rs, window.rs execute
  (513-555, 1038-1052), agg.rs 1842/1864/2947 sort sites, exec.rs 24179-24216 (until deleted),
  scanner.rs 2087 top-K.
- P6 partition-aware planning (XXL, ~8,200 lines: part-planner-cost-based-plans 5,817 + part-explain-
  append-prune 2,430: Append over leaves in bound order with _n aliases, static + run-time pruning
  (partprune.c port), Subplans Removed / (never executed), EXPLAIN EXECUTE generic plans under
  plan_cache_mode, constraint_exclusion for inheritance, Merge Append, partitionwise join/agg,
  Update/Delete child lines, inherit Append blocks; depends on N31a, N32, N33a and P4). Files:
  plan/partition.rs (new), partition.rs partitions_of/leaves_of/descendants (479-551, bound order),
  inheritance.rs children_of (95, OID order), exec.rs partitioned_scan (17100) / inherited_scan (17133)
  until re-homed, session.rs prepared-statement lookup for EXPLAIN EXECUTE, plan/exec/append.rs.
- P7 parallel plan shapes (M if plan-parallel/execute-serial accounting; XL if real workers; ~2,200
  lines: select_parallel ~900, write_parallel 42, join_hash Parallel Hash ~600, partition_prune
  explain_parallel_append 365, aggregates/incremental_sort/select_distinct/memoize/partition_aggregate
  parallel ~300). Files: plan/parallel.rs, plan/exec/gather.rs, cost.rs parallel costs, ALTER TABLE
  SET (parallel_workers) reloption stored (exec.rs reloptions region shared with N33b).
- P8 GiST/SP-GiST/BRIN/hash/GIN-generic index access methods with real scans (XXL, ~1,400 lines:
  create_index_spgist 620, gist 137, brin* 165 + brin functions 169, gin 84, hash_index 8, polygon 157,
  box 116, tsearch 80, spgist 12, rangetypes 16, KNN Order By, WindowAgg wrappers). Files:
  crates/pgexec/src/index_am/{gist,spgist,brin,gin}.rs (new), exec.rs index_entries, builtin_opclasses.rs
  /builtin_opfamilies.rs, plan/indexpath.rs, geometry.rs point_inside bug.

### Language / executor workstreams

- N07 parser lane 1: expression, FROM and select-list grammar (L, ~1,000 lines). Roots
  planner_join-parser-from-clause-and-star-gaps 430, agg R39 140, jx row-star 47, dml whole-row 73,
  part-parenthesized-setop 24, part-insert-as-alias 25 (grammar half), CTAS AS EXECUTE (select_into
  24, write_parallel 14, prepare 28), WHERE CURRENT OF grammar (executor in N36a), DROP INDEX list,
  `$1.f1`, `name mode type` routine params, x.* in ROW()/args/casts/VALUES, empty select list,
  parenthesised set-op operands, ((subq)) alias, JOIN USING alias, (a JOIN b) AS x(cols), BEGIN WORK
  (agg R22). Files: crates/pgparser/src/parser.rs [join_onto ~12990-13030, table_factor 13105,
  parse_from 12960, expression primary Star ~1233/11330/14993, select target list ~11897, set-op
  operands 12514-12660, create_table_as 7001, UPDATE/DELETE WHERE, DROP INDEX, routine_arg ~14375,
  begin 6628], crates/pgparser/src/ast.rs (Star-in-expr, CTAS execute), crates/pgexec/src/exec.rs
  [join alias binding, CTAS EXECUTE arm].
- N09 lexer + syntax-error parity + error positions/DETAIL/HINT (M-L, ~800 lines: scalar syntax-parity
  306, error-position/caret ~60+97+58+41+35+14+18, part error DETAIL split 33, obj-msg-quoting,
  'at or near' wording). Roots planner_join-error-cursor-position, agg R26, dgr-datetime-error-caret,
  scalar syntax-parity/error-position, jx cursor. Files: crates/pgparser/src/lexer.rs (699 at_or_near,
  string continuation), crates/pgparser/src/error.rs (34 ParseError::new, 141 reporting_position),
  parser.rs syntax_error_at_token (13684) adoption, crates/pgexec/src/error.rs (ExecError -> PgError
  position/detail/hint), session.rs error emission (LINE/caret from offsets), scope.rs column-resolution
  messages ('column t1.x does not exist' + HINT, 'table reference "x" is ambiguous').
- N10 result column naming (S-M, ~1,100 lines: agg R7 1,057 = LATERAL alias pin 830 + FigureColname arms
  140 + function-scan naming 96; planner_join-column-name-inference 40; arrays naming 84; case 22;
  collate 8). Files: crates/pgexec/src/exec.rs [named_expr_inner 24919, BindPass::set_expr ~14785 pin
  alias before substitution, derived_name 24850], routine.rs table_function_columns (2951), viewdef.rs
  661 follows.
- N11 LATERAL / correlated outer-reference binding (L, ~670 lines: planner_join-lateral-binder-gaps 380,
  rangefuncs LATERAL scope 207, subqueries-in-ON 60, part-update-set-row-subselect 24). ABSORBED by
  P0a's bind.rs (scope chain) if P0a lands first; standalone only if the planner is deferred. Files:
  crates/pgexec/src/exec.rs [LateralBinder/BindPass/Shadow 13981-14320, lateral_join 13745,
  is_lateral_item 13713], scope.rs, subquery.rs resolve_in_select.
- N12a arrays of any element type + domain over composite (XL, ~1,850 lines: json 368, jsonb 354,
  agg R14 346, arrays 185, ctp user-type arrays/domain-over-composite 364, with 90, enum 36,
  polymorphism 35, rangefuncs 27, queryfuncs 19, union varbit 19; certify alone — array storage
  format). Roots jx-array-elem-types, agg R14, ctp-user-type-arrays-and-domain-over-composite,
  part-hash-array-partitioning (hash side). Files: crates/pgtypes/src/datum.rs ElemType (261) /
  from_column_type (355-446) / array_of (1118) / code()/write_code/read_code, crates/pgtypes/src/
  cast.rs, crates/pgcatalog/src/serde.rs (SCHEMA_VERSION bump), crates/pgexec/src/usertype.rs
  create_domain (98-106), parser.rs 776 array-type allow-list, partition/hash.rs hash_array_extended.
- N12b relation rowtypes as first-class types + whole-row references (L, ~1,000 lines: agg R15 468,
  misc 218 ($1.name postquel), views R18 117, planner_join-xc mki8 39, part-table-rowtype-casts 29,
  rangefuncs whole-row params 60, generated 40). Files: crates/pgexec/src/usertype.rs (register relation
  composite types on CREATE/ALTER/DROP TABLE|VIEW, pg_type typrelid), routine.rs resolve_type (468),
  scope.rs 1299, eval.rs 4199 whole_row_reference (55), exec.rs 14892, rowexpr.rs row-type coercion,
  parser.rs `$1.f1` / `alias.*` args (shared with N07).
- N12c row comparison semantics + row-valued subqueries (M, ~260 lines: planner_join-row-subquery-
  comparisons 160, agg R16 97). Files: crates/pgexec/src/subquery.rs run_scalar/run_single_column
  (640/660), eval.rs InSubquery/Quantified with Row lhs + apply_binary on records, rowexpr.rs,
  crates/pgtypes/src/ops.rs:1039 wording.
- N13 SQL-language function executor (XL, ~1,100 lines: agg R19 ~490-705, ddl-sql-routine-executor
  (create_procedure CALL args, create_function_sql) ~250, planner_join-sql-function-final-ctas 28,
  views R17 37, dml SQL-function body deparse 62, views R48 merge-sql-body 73, rules H23 25,
  polyf implicitly_coercible 14, OR REPLACE/OUT checks, inlining CONTEXT/QUERY lines via pgwire
  DiagnosticFields). Files: crates/pgexec/src/routine.rs (inline_scalar 1825, final_query 2211,
  callable 2272, bound_args 2347, substitute 2377, check_replaceable 875, build_routine 677,
  implicitly_coercible 1535, resolve_type 468, 2628/2841 final-statement check), session.rs
  drive_scalar_worker (8027) dispatch for a SQL-body executor, plpgsql.rs execute_scalar_function seam,
  crates/pgwire/src/error.rs DiagnosticFields (internal_query/position), catalog_fn.rs
  pg_get_functiondef.
- N14 user-defined function scans / FunctionScan relation builder (L, ~1,050 lines: agg R18 930,
  part-pg-get-indexdef-lateral 55, object_address FROM-position calls 20, stats_import LATERAL
  pg_restore_*, plpgsql FOR implicit lateral 3). Files: crates/pgexec/src/exec.rs [build_table_expr
  Function arms 17745-17794, build_table_expr_schema_with_ctes 19576-19605, lateral_schema_item 19326,
  from_column_names 14634] (or their P0a successors in plan/bind.rs), routine.rs expands_as_table 2911 /
  table_function_expansion 2924 / table_function_columns 2941 / plpgsql_table_function_schema 3020 /
  eval_plpgsql_table_function 3052 / expand_table_function 2817, srf.rs from_item 1185 / plan 456 /
  user_function_relation 1357 / undefined_function 2295 fallback to a one-row scalar-function scan,
  plpgsql.rs execute_table_function 402-421 + Return arm 1756.
- N15 named / default / VARIADIC call arguments (M, ~1,500 lines: jx-named-args 593, agg R20 458,
  polymorphism named 260 + variadic 203 (overlaps R20), text variadic 82, json 83, jsonb 83, dgr 40,
  part 18, fast_default 19, stats_import 4). Files: crates/pgparser/src/parser.rs func_call (1901-1950)
  / positional_from_named (3012) / peek_named_argument_label (2991), crates/pgparser/src/ast.rs
  FuncArgs (labels + VARIADIC), crates/pgexec/src/routine.rs resolve_call/bound_args (RoutineArgMode::
  Variadic ~826), builtin_procs_*.tsv.zst regenerated from a post-initdb pg_proc (proargnames,
  pronargdefaults, proargdefaults for the 82 system_functions.sql functions), one resolution point
  before eval.rs 369-426 guard chain (json_fn.rs 874-899, srf.rs 321/755, format_fn.rs make_interval),
  viewdef.rs FuncCall deparse (name => value).
- N16 aggregate completeness (L, ~1,300 lines: agg R1 ordered-set/hypothetical 283 (+ tuplesort 12),
  R2 builtin support functions callable 219, R3 useragg fidelity 79 + R3b moving aggregates 60, R5
  overload coverage 154, R4 outer-level aggregates 254, R10 functional dependency 94, R11 grouping-sets
  semantics 47, volatile group key double-eval 3, polymorphic aggregates 56, create_aggregate 95).
  Files: crates/pgexec/src/agg.rs (AggFunc enum, validate_grouped 1161, func_in_scalar_context_error),
  useragg.rs (lookup via routines_named -> builtin table, build/accepts, msfunc/minvfunc), grouping.rs,
  func.rs (int4pl, int8inc, float8_accum, numeric_avg_accum, ordered_set_transition ...), exec.rs
  comment_ops (COMMENT ON AGGREGATE), parser.rs WITHIN GROUP suffix + ast.rs (shares func_call
  region with N15 -> sequence after N15).
- N17 window frames and specs (M, ~470 lines: agg R12 409, R3b moving-aggregate execution 60).
  Files: crates/pgexec/src/window.rs resolved_frame (1354) / execute, parser.rs frame_bound (2138),
  useragg.rs mstype hooks.
- N18 recursive CTE shape + SEARCH/CYCLE + DML CTE ordering + CREATE RECURSIVE VIEW (L, ~1,100 lines:
  agg R13 883-922, R32a 161, planner_join-recursive-cte-parenthesised-form 37; depends on N12a for
  record[]). Files: crates/pgexec/src/cte.rs split_recursive_terms (294-315), check_recursive_term
  (533-541), 329-340 refusals, 380 MAX_RECURSION_ITERATIONS + lazy LIMIT, evaluation_order /
  cte_references (164-169), exec.rs execute_write_parts (~4362) DML forward references, parser.rs
  create_view (9259) RECURSIVE arm + parse_with_clause (~12735) WITH-prefixed DML bodies, viewdef.rs
  194-230 SEARCH/CYCLE printing.
- N19 SRF ProjectSet semantics (L, ~510 lines: agg R6 508 minus the GROUP BY part done in N03: nested
  SRF args, SRF in GROUP BY/PARTITION BY, DISTINCT ON, error contexts (CASE/COALESCE/LIMIT/UPDATE/
  window args), split_pathtarget_at_srfs placement). Files: crates/pgexec/src/srf.rs (1467/1587/1709
  refusals, project_rows_ordered 1696, rows).
- N20 datetime typmod precision (L, ~1,980 lines: timestamp 726, timestamptz 870, interval 91,
  horology 296; certify alone — SCHEMA_VERSION bump). Root dgr-datetime-typmod-precision (verified).
  Files: crates/pgtypes/src/datum.rs ColumnType::{Time,Timetz,Timestamp,Timestamptz,Interval} payload
  + typmod() 1391, crates/pgtypes/src/cast.rs cast_in 512 / cast_assign_in 423 (round half away from
  zero, interval field-mask truncation), crates/pgtypes/src/datetime.rs helpers next to IntervalField
  1977 / parse_interval_ranged 2086, crates/pgparser/src/parser.rs parse_type_name 651-663 +
  interval_literal 1852-1880, crates/pgexec/src/exec.rs coerce 12800-12813 + catalog_typmod 21529,
  crates/pgcatalog/src/serde.rs 355-376 / 500-524 + SCHEMA_VERSION 50, crates/pgexec/src/func.rs
  builtin_format_type 2745 (interval typmod spelling), viewdef.rs cast deparse.
- N21 datetime / geometry / range function fidelity (M-L, ~880 lines: dgr-missing-datetime-functions
  165 (pg_sleep, timestamptz(date,time), date_add/subtract, interval_hash, avg(interval)),
  dgr-overlaps-predicate 96, multirange literal parser 83, format-fn fidelity 76, generate_series
  timestamp 73, tz-abbrev 70, box_in adjacent points 63, unknown-literal args 59 (shared with N40b),
  cross-type compare 41, epoch UTC 30, range DDL 29, BETWEEN SYMMETRIC 28, date_bin overflow 21,
  interval fidelity 16, decode 8, multirange adjacent 4, 'now' at transaction start (datetime.rs
  clock_now 972)). Files: crates/pgtypes/src/{datetime.rs, geometry.rs (box_in), range/multirange
  parsers}, crates/pgexec/src/{func.rs, format_fn.rs, datetime_fn.rs, srf.rs generate_series lazy},
  parser.rs OVERLAPS/BETWEEN SYMMETRIC.
- N21x datetime range representation (XXL, 247 lines: date 8, timestamp 44, timestamptz 91, horology 98,
  rangetypes 6): jiff civil range +-9999 vs PG 4713 BC..294276 AD / 5874897; DECISION needed (replace
  jiff calendar arithmetic with a PG-faithful julian-day implementation in crates/pgtypes/src/datetime.rs).
- N22 jsonpath grammar/printer + datetime methods + evaluator (L, ~2,600 lines: jsonpath 909,
  jsonpath_encoding 124 (target the UTF8 expected file, not _2), jsonb_jsonpath datetime 1,280 +
  grammar 117 + misc 107 + .decimal 66). Roots jx-jsonpath-grammar-printer, jx-jsonpath-datetime,
  jx-jsonpath-eval-misc. Files: crates/pgexec/src/jsonpath.rs (lex 401, lex_number 522 -> numeric::
  parse_finite, lex_quoted 555, Parser 610, Method 174/1007-1035, datetime_method 1995-2040, compare
  1784-1807, printer 2118-2246 round-trip escaping), crates/pgtypes/src/datetime.rs template engine
  std mode (parse_by_template 5481, Scanner 5049, Assembly 5490, tokenize_template 4613; new error
  texts + field mask), json_fn.rs path_args/eval_path_func 874-945 + jsonb_path_query_rows 1714 (use_tz
  + ctx.time_zone), jsontable.rs 346/385/532, EvalCtx warning sink for 'TIME(10) precision reduced'.
- N23 JSON/JSONB function fidelity (L, ~1,900 lines: sqljson constructors 293 + aggregates 248 +
  JSON_ARRAY(subquery) 50, sqljson_queryfuncs returning coercion 201 + analysis checks 184 + constraint
  deparse 40 + index immutability 27, sqljson_jsontable deparse 143 + cursor 40 + user-type oid in view
  52, jsonb scalar casts 158, subscript polish 133, populate_record_valid 80, srf-record-in-tlist 74+23,
  #- and set-path messages 51, pg_column_size 30, diagnostics 30+4, json_object shape 20+24, set_lax 6,
  populate coercion 7+7, array_to_json multidim 6, jsonb stack HINT 2). Files: crates/pgexec/src/
  {json_fn.rs, jsontable.rs, sqljson*.rs, viewdef.rs JSON deparse}, crates/pgtypes/src/jsonb.rs,
  parser.rs JSON_* clauses, lexer #- token.
- N24 XML (XL, ~1,200 lines: xpath 425, xmltable 349, xmlelement family 253, SET XML OPTION 40,
  xmlmap 115, refcursor 12, oid[] 10, cursor 8, libxml second error line 3). DECISION: target xml.out
  (native implementation must reach libxml parity for xpath/xmltable) vs xml_1.out (no-libxml variant,
  1,327 lines today). Files: crates/pgtypes/src/xml.rs (quick-xml), crates/pgexec/src/xml_fn.rs,
  parser.rs XMLTABLE/PASSING/COLUMNS, xmlmap functions (table_to_xml family), session.rs SET XML OPTION.
- N25 text search subsystem (XXL, ~2,850 lines: tsearch 1,308 non-planner (default parser 389,
  headline 299, unknown-literal tsargs 244, aux functions 163, match exec 96, websearch 70, rank 28,
  gist opclass options 19), tsdicts 599 (ispell/hunspell/synonym/thesaurus dictionaries + config
  mapping DDL), tstypes 489 (tsvector/tsquery I/O, unknown-literal args 282, aux fns 95, match 32),
  json/jsonb tsvector 217+229). Files: crates/pgtypes/src/text_search.rs (rewrite: parser token
  types, tsquery precedence printer, matcher), crates/pgexec/src/text_search_fn.rs (default parser
  port ts_parse/ts_token_type/ts_debug/ts_stat/ts_rewrite/headline/rank/unnest/tsvector_to_array),
  text_search_catalog.rs (templates, dictionaries with options, configs with mappings, dictinitoption
  deparse), parser.rs create_text_search 6534 / alter_text_search 6579 / def_arg_name 439, exec.rs
  text_search_catalog_rows 21710, dictionary files from src/backend/tsearch (ispell_sample,
  hunspell_sample*, synonym_sample, thesaurus_sample).
- N26 FDW DDL + catalogs (L-XL, ~1,550 lines: fdw-ddl-grammar 758-811, fdw-catalogs-psql 613-617,
  fdw-object-semantics 159, foreign-table-relkind 36, set-statistics/storage 24, check-deparse 18,
  reassign/drop owned 7, alter-sequence 3). Files: crates/pgparser/src/parser.rs [ALTER dispatch
  3696-3789 add Foreign arm, parse_options 13843, parse_user_mapping_user 13865, create_fdw 13887,
  create_server 13916, alter_server 13934, create_user_mapping 13960, alter_user_mapping 13977,
  drop_user_mapping 13995, create_foreign_table 14014, drop_foreign_table 14057, import_foreign_schema
  14073, grant/revoke 4568/4615 ON FOREIGN ..., comment_on 8592], crates/pgparser/src/ast.rs 955-1032,
  crates/pgcatalog/src/lib.rs 610-633 (owner/handler/validator/type/version/acl/oid) + 268
  ForeignTableMeta per-column options (SCHEMA_VERSION bump) + 6279/6355/6321/6538 ops, serde.rs
  1973/2313/2342, crates/pgexec/src/catalog_rel.rs (pg_foreign_data_wrapper, pg_foreign_server,
  pg_user_mapping[s], pg_foreign_table, information_schema.foreign_*/user_mapping*/usage_privileges/
  role_usage_grants), catalog_fn.rs privilege functions (has_server_privilege etc.),
  pg_options_to_table SRF, exec.rs 1826-1952 arms + comment_ops 31868.
- N27 logical replication DDL + catalogs (XL, ~1,550 lines: publication-ddl 1,144 (catalog/psql 539,
  parse 390, semantics 154, refusal 61) + DML-time replica identity checks, subscription-ddl 374,
  predefined roles 9; DDL + catalog + psql listing only, no replication). Files: parser.rs
  create_statement (5488) / ALTER dispatch publication|subscription productions, ast.rs
  NON_GOAL_REFUSALS (27 -> fewer; parser.rs:23197 test, session.rs:21046), NEW crates/pgexec/src/
  publication.rs (row-filter/column-list validation beside policy_ddl.rs), pgcatalog lib.rs
  Publication/Subscription records, catalog_rel.rs pg_publication (pubgencols), pg_publication_rel
  (prattrs int2vector, prqual), pg_publication_namespace, pg_publication_tables view +
  pg_get_publication_tables(), pg_subscription, pg_subscription_rel, pg_stat_subscription_stats,
  catalog_fn.rs pg_relation_is_publishable (732), exec.rs comment_ops + DropColumn dependency (28370)
  + execute_write_parts replica-identity check (4361), session.rs dispatch 6921/6973,
  docs/PG_COMPAT_MATRIX.md rows 145/201/251.
- N28 rule system (XXL, ~1,450 lines: views R1 1,002 (rules 805, updatable_views 183, create_view 14),
  copydml 52, returning 147, with ~200, generated_* 40, errors drop-rule 10, foreign_key 8, portals 2).
  Files: parser.rs CREATE [OR REPLACE] RULE / DROP RULE / ALTER RULE / COMMENT ON RULE / ALTER TABLE
  ENABLE|DISABLE RULE (~4008), ast.rs NonGoalCommand::{AlterRule,CreateRule,DropRule} removed from
  refusals + command.rs rows, pgcatalog Rule record + pg_rewrite rows, NEW crates/pgexec/src/
  rewrite_rules.rs (query rewriter: DO INSTEAD/ALSO, ON SELECT view rules, NEW/OLD, conditional rules,
  RETURNING), exec.rs execute_write_parts hooks, catalog_rel.rs pg_rules, catalog_fn.rs pg_get_ruledef,
  viewdef.rs, docs/PG_COMPAT_MATRIX.md rows 148/203/254.
- N29a cumulative statistics system (XL, ~1,300 lines: stats relation counters 442, pg_stat_io 290,
  cluster-wide views 189, function counters 174, snapshot/have-stats 147, pg_stat_database 61, GUCs 14,
  vacuum pg_stat_*_tables 74, select_parallel pg_stat_database/pg_stat_force_next_flush 23). Files:
  NEW crates/pgexec/src/pgstat.rs (per-relation/function/io/database counters, snapshot semantics,
  pg_stat_force_next_flush/pg_stat_reset*/pg_stat_have_stats/pg_stat_get_*), catalog_rel.rs pg_stat_*
  / pg_statio_* views, scan leaf + execute_write counter hooks (plan/exec after P0a), session.rs GUCs
  track_functions/track_counts/track_io_timing/stats_fetch_consistency.
- N29b pg_locks / pg_prepared_statements / pg_prepared_xacts / pg_database rows (M, ~320 lines: lock 73,
  advisory_lock 90, stats_import 24, prepare 40+4, psql 8, lock privileges 4). Files: catalog_rel.rs
  rows() arms (pg_locks from lockmgr.rs holds incl. advisory + tuple/relation locks), lockmgr.rs
  renderers, session.rs prepared-statement catalog (pg_prepared_statements text), exec.rs pg_database
  rows (postgres row).
- N29c system view definitions dump (XXL if honest — 80 real views bootstrapped from system_views.sql
  with their SRFs; 1,437 lines in rules; DECISION). Files: catalog_rel.rs pg_views_rows (2902) +
  RELATION_NAMES/system_view_oid, exec.rs virtual_table registries (19853/22320/22681/20573) retired,
  bootstrap of view definitions.
- N30a create_view: regress C adapter for interpt_pp (S, 902 lines: create_view 1 + select_views 901).
  Files: crates/pgexec/src/routine.rs RegressionCAdapter (1694-1746, 2041-2063) + has_exact_regression_
  c_signature (1699) result-type parameter, crates/pgtypes/src/geometry.rs Lseg::intersection_point.
- N30b view storage, deparse and updatable-view semantics (XL, ~1,670 lines: views R2 view-bound
  storage 421, R3 viewdef deparse 356 + agg R31 76 + limit-with-ties viewdef 35 + jx N 26 + planner_join
  \sv unnamed_subquery, R6 view column defaults 173, R8 view-write-rewrite gaps 264, R7 view privileges
  98 (shared with N37a), R16 security-barrier qual ordering 26, R11 matview refresh 12, R12 view
  reloptions 42, R13 temp-view notice 17, R9 merge into views 22, R10 row locking through views 34,
  part-prepare-dml-on-view 18, R44 alter view rename column 10, R43 explain create matview 14 (P0b),
  R14/R15, R24 values typmod 24, R25 unknown pseudo-type 45, R27 misc catalog fns 74). Files:
  crates/pgexec/src/viewdef.rs (write_query 141/270-292, write_select 430-445, expression_text 243,
  1093), viewwrite.rs, exec.rs CreateView arm (1095, 2152) + build_base_table view expansion
  (17612-17660) (bound view storage: store the analysed query, not text), pgcatalog View record +
  serde, session.rs PREPARE/describe of DML on views (25126), rls.rs security barrier ordering.
- N31a expression partition keys + partition DDL/catalog misc (L, ~1,700 direct lines; a further ~1,700
  behind it are planner/Append lines for P6: part-expression-partition-keys 1,629 verified, dml
  partition expression keys 633, create_table cascade ~200 and its leaks into alter_table ~150 /
  triggers 51 / merge 17 / inherit 16, part-hash-opclass-support-function 40, part-hash-array-
  partitioning 10, part-attach-column-mismatch 3, part-partition-key-constraint-check 37,
  part-partition-scan-order 6, part-pg-partition-tree-root 294 + cat-pg-partition-tree 153 + fk 107 +
  cluster 32, part-satisfies-hash-partition 74, pg_get_partition_constraintdef 112, MINVALUE/MAXVALUE
  ordering check). Files: crates/pgparser/src/parser.rs opt_partition_by (7436-7488) + ast.rs
  PartitionKeyElem (2394) keep Expr/collation/opclass, crates/pgexec/src/partition.rs (SCHEME_VERSION
  55 -> 3, Scheme 128, serialize 258/268, key_ordinals 691, key_values 708, key_description 769, route
  873, satisfies 895, key_columns 1114, expression_key_error 1156, key_column_type 1166, partitions_of
  479 / leaves_of 537 / descendants 504 bound order), partition/hash.rs (opclass support fn 2,
  hash_array_extended), exec.rs [partition_scheme_from_ast 25588, reject_incomplete_partitioned_key
  25617, resolve_partition_bound 25660, route_row_to_leaf 17478, 5067, pg_partitioned_table_rows 20315,
  reject_partition_key_column ~29912, RENAME COLUMN scheme rewrite ~30178, attach_partition_ops 29002
  column-set check], eval.rs infer_type 4154 reuse, catalog_fn.rs part_key_def 2019 via viewdef.rs
  expression_text 243, srf.rs pg_partition_tree + catalog_fn.rs pg_partition_root, func.rs
  satisfies_hash_partition, useroperator.rs opclass lookup, inheritance.rs children_of OID order.
- N31b partitioned foreign keys + FK misc (L, ~1,050 lines: ctp-partitioned-foreign-keys 815,
  ctp-fk-referencing-partitioned-pk 102, truncate FK on partitioned 65, ctp NOT ENFORCED 4+, drop
  cascade notice for partitions 22, agg R34 self-referential FK 152, ctp-fk-misc-semantics, ON COMMIT
  FK 5). Files: crates/pgexec/src/exec.rs [718-723 CREATE TABLE refusal, 29302-29308
  add_foreign_key_constraint, 943 drop_table_and_dependents_ops(removed vs dropping),
  drop_foreign_key_constraint 29390, alter_constraint ~29150, ValidateConstraint 28822,
  partition_definition 25300 FK clones, attach_partition_ops 29002 FK clone (shared with N31a/N33a ->
  sequence)], fk.rs (resolve_foreign_key 239, run_child_check 1815, run_parent_check 1889,
  find_referencing_rows 2000, violation 735 names root table, types_are_comparable 557,
  FkParts::resolve rows in partitions), crates/pgcatalog/src/lib.rs ForeignKey (529) parent link +
  serde.rs 1626/1663 FOREIGN_KEY_VERSION, catalog_rel.rs constraint_row (2669) conparentid/conenforced.
- N31c cross-partition UPDATE / MERGE / partitioned DML (L, ~950 lines: update row movement 241 + column-
  order refusal 67 + part-cross-partition-update 4, MERGE into partitioned 240, generated_stored/virtual
  partition rules 137+139, identity partitions 114, ON CONFLICT on partitioned 4+4+4). Files:
  crates/pgexec/src/exec.rs [execute_timestamp_update 12085, check_partition_constraint 5047, MERGE
  executor, INSERT ... SELECT routing 17486, apply_locked_row_update 10878], partition.rs route,
  timestamp_txn.rs rowid contract, trigger.rs partition trigger clone rules (2050/598/2150).
- N32 inheritance + NOT NULL / CHECK constraint catalog (XL, ~2,000 lines: part-drop-cascade-
  inheritance-children 269, part-inherits-column-merge 106, part-notnull-constraint-catalog 301 +
  ctp-notnull-constraint-catalog 234, part-check-constraint-inheritance 302, part-alter-table-inherit
  49 + rowsecurity ALTER TABLE INHERIT cascade 397 + generated inheritance DDL 165 + identity 8,
  part-merge-notice-count 36, part-rename-inherited-column 9, part-column-default-persistence 13
  (shared with N35a), part-regclass-oid-union-types 15, rules H42 43, views R31 43, ctp-alter-table-
  inherit, ctp-detail-failing-row ~50, ctp-comment-on-subobjects). Files: crates/pgcatalog/src/lib.rs
  Column.not_null (170) -> NotNullConstraint record, CheckConstraint (254) + no_inherit/enforced/
  conislocal/coninhcount/normalised expr, serde.rs, crates/pgexec/src/exec.rs [inherited_table_definition
  25217-25290, drop_table_and_dependents_ops 9304 + cascade notices 9526-9600, apply_table_not_null_
  constraints, ALTER TABLE SET/DROP/ADD NOT NULL + ALTER CONSTRAINT ~29092-29233, add_check_constraint
  29489, DROP CONSTRAINT, ALTER TYPE inherited-column check, AlterTableAction::RenameColumn, ALTER TABLE
  INHERIT/NO INHERIT executor near 29002], inheritance.rs (descendants/children_of OID order, rename_ops,
  attach_ops/drop_metadata_ops), catalog_rel.rs pg_constraint rows (contype n/c: conislocal,
  coninhcount, connoinherit, conenforced, convalidated; check_constraint_rows 2547-2604) + pg_get_expr,
  session.rs inheritance_notices (~9488), parser.rs alter_table_action (3997-4190) INHERIT/NO INHERIT +
  NOT NULL NO INHERIT + ALTER CONSTRAINT INHERIT, viewdeps.rs (RENAME COLUMN spurious refusal),
  error.rs check-violation DETAIL 'Failing row contains'.
- N33a partitioned index tree + index DDL/catalog fidelity (XL, ~1,150 lines: part-partitioned-index-tree
  404, cat-index-pg-attribute-rows 372 (\d <index> in tablespace 345 + indexing/psql), part-dropped-
  column-placeholders 30 (XL on its own — DECISION), part-index-autoname-and-deparse 19, part-unique-
  index-per-partition-enforcement 13, part-pk-on-partition-column 7, part-drop-index-concurrently 4,
  part-replica-identity-using-index, idx-reindex-effects 69, idx-concurrently-txn-block-and-invalid 44,
  ddl-alter-table-add-constraint-using-index 87, idx-partitioned-index-cascade 37 + ALTER INDEX ATTACH
  8, ddl-alter-index-alter-column 15, ddl-drop-index-multi-and-concurrently 17, index comment 6,
  psql partitioned-index pg_inherits 68). Files: crates/pgcatalog/src/lib.rs Index (parent link,
  indisvalid, indisreplident) + put_index_ops, crates/pgexec/src/exec.rs [cloned_partition_index 25438,
  partition_index_clones 25474, attached_partition_index_ops 25507, clone_indexes_onto_partitions 25529,
  attach_partition_ops 29002 (index half), pg_inherits_rows 20277, pg_attribute_rows 20874 (rows for
  indexes), Statement::DropIndex, REINDEX relfilenode, CIC failure state, index name chooser 9474/9675],
  parser.rs alter_index (3960) ATTACH PARTITION / ALTER COLUMN SET STATISTICS, drop_index (11081)
  CONCURRENTLY + list, ADD ... USING INDEX, catalog_fn.rs IndexDef (128) ON ONLY + expression
  parenthesisation, catalog_rel.rs pg_index/pg_constraint partition rows.
- N33b storage misc (L, ~1,300 lines, independent files: cat-reloptions-storage 98, ddl-tablespace-
  semantics 79, guc-toast-compression-and-column-compression 233 + pg_toast 9, exec-tablesample-pg-
  faithful 178, fn-random-pg-prng 282, fn-hash-functions 184, fn-index-property-functions 178,
  fn-make-tuple-indirect 73, pg_relation_size 2). Files: crates/pgexec/src/{reloptions in exec.rs
  ALTER TABLE SET (...) + pg_class.reloptions, tablespace.rs, func.rs (hash*, random via PG xoroshiro
  port, pg_indexam_has_property/pg_index_column_has_property/pg_index_has_property, brin_* stubs),
  tablesample (page-based SYSTEM/BERNOULLI sampler over rowid blocks), toast/compression columns
  (parser COMPRESSION, pgcatalog Column compression, pg_column_compression, \d+ Compression)},
  crates/gres pglz? (compression_1 variant = no-lz4 build).
- N34a operators: shells, ALTER OPERATOR / OPERATOR FAMILY, resolution, lexing (M-L, ~850 lines:
  ddl-operator-shell-and-alter 135, create_operator 78, planner_join-xc-user-operator-resolution 57,
  agg R17 generic operator lexing 238, lex-pattern-and-startswith-ops 181, stats_ext user-operator-
  symbol-lexing 65, part-user-operator-lexing 14, expressions shell-operators 103, equivclass ALTER
  OPERATOR 33, rowsecurity <<< 7, privileges <<< >>>). Files: crates/pgparser/src/lexer.rs (42 lex:
  PG operator token rule; fixed punctuation table), parser.rs ALTER OPERATOR / ALTER OPERATOR FAMILY /
  CREATE OPERATOR CLASS options, crates/pgexec/src/useroperator.rs (shell operators 373, families,
  opclass functions), eval.rs apply_binary user-operator resolution before builtin comparison,
  crates/pgtypes/src/ops.rs 1039 wording.
- N34b object addressing, dependency graph, role-owned objects, DROP notices (L, ~1,450 lines:
  ddl-object-address-functions 611+43+59+33+8+11, dependency 96, event_trigger 247, drop_if_exists 170,
  drop-cascade NOTICE/DETAIL family (part 3, dgr 3, agg R28 6, indexes 6, stats_ext 13, stats_import 8,
  views R21 dependency-cascade-order 73, views R20 2, foreign_key 22), pg_depend/pg_shdepend rows
  (views R41 45, cat-pg-describe-object-and-depend 48, misc_sanity 7), DROP OWNED / REASSIGN OWNED,
  ddl-if-exists-notices, temp-view NOTICE). Files: crates/pgexec/src/catalog_fn.rs CatalogFunc (68/118:
  pg_describe_object, pg_identify_object, pg_get_object_address record form), srf.rs classify (300)
  FROM-position forms (pg_get_object_address, pg_identify_object_as_address), catalog_rel.rs
  pg_depend_rows (1017) all object classes + pg_shdepend, reg_fn.rs, exec.rs [drop notice builders
  9526-9600 (all object kinds, OID order), DROP ... IF EXISTS notices, event trigger ddl_command_end
  rows], pgcatalog dependency edges (owner, ACL, function<-operator/cast/aggregate), parser.rs DROP
  OWNED/REASSIGN OWNED/CREATE GROUP.
- N34c type / cast / access method / typed table / LIKE DDL (L, ~1,300 lines: create_type 197 (shell type
  autocreate, base-type attributes), create_cast 34 (WITH FUNCTION), create_am 295 + psql 120 + pg_am
  fidelity 59 (CREATE ACCESS METHOD TYPE TABLE|INDEX with handler, ALTER TABLE SET ACCESS METHOD),
  typed_table 108 (CREATE TABLE OF type), create_table_like 377 (LIKE INCLUDING ..., column order with
  INHERITS), create_table misc ~150 (unknown type message, CTAS quirks), domain comment/validation
  (domain 898 minus arrays 364 minus integration 40 ~ 200), views R25 unknown pseudo-type 45).
  Files: crates/pgexec/src/usertype.rs (shell types, typinput/typoutput attributes, domain checks
  1357), exec.rs [CREATE CAST / CREATE ACCESS METHOD / CREATE TABLE OF / LIKE expansion regions ~1000-
  2400, comment_ops 31880 (COMMENT ON DOMAIN/TYPE/CAST/AM)], parser.rs (CREATE TYPE attribute list,
  CREATE TABLE OF, LIKE options, CREATE ACCESS METHOD, COMMENT ON subobjects 8592), catalog_rel.rs pg_am
  / pg_type rows.
- N34d ALTER TABLE subcommand completeness (XL, ~1,500 lines: alter_table 1,841 minus leaks 180 minus
  planner 68 minus N31/N32 shares ~400 = ~1,200; alter_generic 309; views R30 ALTER ... SET SCHEMA 17;
  identity SET LOGGED 30). Files: crates/pgparser/src/parser.rs alter_table_action (3997-4190) remaining
  subcommands (SET SCHEMA, SET LOGGED/UNLOGGED, ALTER COLUMN SET STATISTICS/STORAGE/(n_distinct)/
  COMPRESSION, SET ACCESS METHOD, OF/NOT OF, CLUSTER ON, SET WITHOUT OIDS, VALIDATE, ALTER
  CONSTRAINT ...), exec.rs ALTER TABLE executor (28900-30300 region, Action::Unsupported 28981),
  alter_generic object families (ALTER AGGREGATE/COLLATION/CONVERSION/FUNCTION/LANGUAGE/OPERATOR
  CLASS|FAMILY/STATISTICS/TEXT SEARCH ... RENAME/OWNER/SET SCHEMA).
- N35a sequence DDL + column DEFAULT expressions + drop-owned sequences (L, ~860 lines: txn-seq-full-ddl
  387 (sequences are CreateIndex on a fake relation; ALTER SEQUENCE unparsed; no lastval/regclass
  overloads/pg_sequences/smallserial), drop-table-leaves-owned-sequence 141, column DEFAULT expressions
  stored unevaluated (fast_default 100 + constraints DEFAULTEXPR 128 + sequence 15 + part 13 + inherit
  conflict rule), seq privileges 38, views R19 25, identity seq DDL 11, psql sequence describe 106
  (with N42)). Files: crates/pgparser/src/parser.rs create_sequence/alter_sequence, crates/pgcatalog/
  src/lib.rs Sequence (data type, owner, owned-by) + ColumnDefault (expression AST/text, not Value) +
  serde.rs (SCHEMA_VERSION bump), crates/pgexec/src/exec.rs [column_from_ast 2355,
  ensure_default_can_be_persisted 2794, drop_table_ops/drop_table_and_dependents_ops emit
  drop_sequence_ops, INSERT default evaluation], func.rs (lastval, nextval/currval/setval regclass
  forms), catalog_rel.rs pg_sequences / pg_sequence.
- N35b identity + generated columns completeness (L, ~900 lines: identity ALTER forms 174 + OVERRIDING
  95 + ALWAYS enforcement 31 + info_schema 26 + create validation 19 + drop-owned-seq 11 + typed tables
  6 + drop view multi 3, generated_stored/virtual remainder ~480: diagnostics 45+53, virtual-only rules
  19, qualified names 9+30, info_schema 15+15, triggers 14+24, tableoid 12+12, sublink name 12+12, alter
  view 10+10, deparse 8+8, copy where 6+6, fn privileges 20+12, ...). Files: crates/pgparser/src/
  parser.rs ALTER COLUMN ADD/SET/DROP IDENTITY, OVERRIDING SYSTEM|USER VALUE, exec.rs identity/generated
  column DDL + INSERT/UPDATE enforcement (shares ALTER TABLE region with N34d -> sequence after N34d),
  catalog_rel.rs information_schema.columns is_generated/is_identity, viewdef.rs generated deparse.
- N36a cursors and portals (L, ~650 lines: portals WHERE CURRENT OF 284 + pg_cursors 88 + lazy/error-at-
  FETCH 28 + no-scroll 5 + cursor-lazy 4 + FOR UPDATE join 20 + toast GUC 19, tidscan CURRENT OF 79,
  generated_virtual CURRENT OF 24, locking-path resolution 28+8 (temp/search_path relations under
  FOR UPDATE), planner_join-for-update-restrictions 30, views R10 row locking through views 34,
  txn cursor-lazy 28). Files: crates/pgexec/src/session.rs declare_cursor (4589-4635) / FETCH / cursor
  state, cursor.rs (materialise at first FETCH; SCROLL/NO SCROLL; WITH HOLD; error surfacing at FETCH),
  exec.rs execute_read_locking (23808, 23886 joins) -> LockRows after P0a, catalog_rel.rs pg_cursors,
  parser.rs WHERE CURRENT OF (with N07), UPDATE/DELETE CURRENT OF executor (cursor's current rowid).
- N36b transaction characteristics + implicit blocks (M, ~450 lines: txn-transaction-characteristics 60
  (SET TRANSACTION READ ONLY/DEFERRABLE, SET SESSION CHARACTERISTICS, AND CHAIN), implicit-block for
  multi-statement simple queries 29 + psql_pipeline 142+18+8+4, temp CTAS ON COMMIT 28 + PREPARE
  TRANSACTION refusal 22 + ON COMMIT DROP inheritance 17, xmin 18, cmdid 11, plpgsql RETURN 9, carets 8,
  snapshot 4, warning 2, routine SET clause 2, txn-commit-of-failed-block-keeps-ddl 10). Files:
  crates/pgexec/src/session.rs simple_query (implicit transaction block), sync(), set_transaction_tail,
  TxnCtx (deferrable/read-only), parser.rs SET TRANSACTION / SET SESSION CHARACTERISTICS / CTAS ON
  COMMIT, temp namespace objects (34).
- N36c MVCC system columns + heap order after UPDATE (L, ~150 direct lines: combocid 101 (xmin/xmax/
  cmin/cmax, command-id visibility), heap-order-after-update ~40 across inherit 8, rules 6, returning,
  with y-state, psql 2, cluster 2, ctp; DECISION: emulate new-version-at-end placement against the
  timestamp-domain rowid contract). Files: crates/pgexec/src/scope.rs SYSTEM_COLUMNS, exec.rs
  execute_timestamp_update (12085) / apply_locked_row_update (10878-10943), timestamp_txn.rs, pgmvcc.
- N37a privilege model: column/default/object ACLs, role membership, role lifecycle (XL, ~1,150 lines:
  sec-column-privileges 230, sec-default-privileges 157 (ALTER DEFAULT PRIVILEGES: also select_into 8,
  object_address 2), sec-role-membership-model 154 (GRANT ... WITH ADMIN OPTION GRANTED BY, INHERIT/SET
  options), sec-nonrelation-object-privileges 144, sec-role-attributes-lifecycle 131+65 (create_role,
  password), views R7 view privileges 98, zeropriv 37, role grant options 23, dgr type privileges 12,
  fn privileges 32, seq privileges 38, GRANT ON DATABASE/TABLESPACE, SET ROLE for non-bootstrap
  superuser (crabka_pgcatalog::role_can_set), predefined roles). Files: crates/pgexec/src/privilege.rs
  (542 ReadPermit, 42/1127 column grants), crates/pgcatalog/src/lib.rs roles/ACL records +
  role_can_set, parser.rs GRANT/REVOKE/ALTER DEFAULT PRIVILEGES/CREATE ROLE options, exec.rs grant
  arms + role DDL, catalog_fn.rs has_*_privilege family, catalog_rel.rs pg_default_acl/pg_auth_members
  columns, session.rs SET ROLE.
- N37b large objects (L, ~880 lines: largeobject 457, privileges sec-large-objects 427). Files: NEW
  crates/pgexec/src/largeobject.rs (lo_create/lo_open/lo_read/lo_write/lo_lseek64/lo_tell64/lo_truncate/
  lo_unlink/lo_import/lo_export/lo_get/lo_put/lo_from_bytea, pg_largeobject + pg_largeobject_metadata
  with owner/ACL, lo_compat_privileges GUC), catalog_rel.rs rows, func.rs dispatch, session.rs COPY?
  none.
- N37c misc admin functions, sysviews, routine namespaces (M-L, ~600 lines: sec-misc-admin-functions 404
  (num_nonnulls/num_nulls, pg_ls_*, pg_current_logfile, pg_settings_get_flags, pg_input_is_valid family,
  pg_get_wal_*, gen_random_uuid, ...), sec-sysviews-catalog-views 130 (pg_available_extension_versions,
  pg_timezone_abbrevs, pg_backend_memory_contexts, pg_config, ...), pg_temp function schema 28+9+1,
  schema-qualified routine names (parser routine_name), harness \gset wal_segment_size). Files:
  crates/pgexec/src/func.rs, catalog_rel.rs, parser.rs routine_name qualifier, routine.rs schema field,
  usertype/routine namespaces.
- N38 PL/pgSQL fidelity (L, ~2,000 lines: plpgsql record field types (bpchar NEW/OLD) 570, expression
  SQL tail (`if count(*) = 0 from ...`), error CONTEXT lines with line numbers, %ROWTYPE (rangefuncs 43,
  F 104), RETURN tail (plancache 25), composite field assignment on unassigned record (agg R21 60),
  RETURNS setof composite in FROM, RAISE diagnostics, CONTEXT for SQL functions from plpgsql, psql
  plpgsql CONTEXT 12, plpgsql planner-ish 45 excluded). Files: crates/pgexec/src/plpgsql.rs
  (rewrite_record_field 3045, 288/385/425 context, 1392, 402-421 execute_table_function, 1756 Return
  arm), crates/pgparser/src/plpgsql.rs (parse_expr_range 1245 -> parser.rs parse_expression 15878 SQL
  tail, %ROWTYPE, statement line numbers), eval.rs bpchar_to_text_value (115), routine.rs
  plpgsql_scalar_result_type (1936).
- N39a COPY (M, ~450 lines: COPY FROM fires plpgsql triggers (copy 45 + copy2 132 = ctp-copy-fires-
  triggers 146+), COPY ON_ERROR 106, copy encoding 4, precheck ordering (cannot copy to view after
  CopyInResponse; duplicate-column check before), TRUNCATE notices 158-65-68 ~25, copy WHERE 6+6).
  Files: crates/pgexec/src/session.rs run_copy_in (10527) with_scalar_runtime(request_tx) +
  precheck_copy_from, trigger.rs invoke (132), copy ON_ERROR ignore/log_verbosity, session.rs
  client_encoding LATIN1.
- N39b DML grammar and semantics (L, ~1,150 lines: RETURNING OLD/NEW 195 + views R35 94 (bare old/new,
  in subqueries, MERGE RETURNING, partitioned targets), ON CONFLICT target grammar 102 + arbiter
  inference, INSERT target indirection 68 + arrays insert-target-subscripts 124, multi-column SET
  110 (+ part-update-set-row-subselect 24 half), MERGE grammar 86 + clause scope 135 + MERGE ... USING
  joined source (part-merge-using-join-parse 97) + views R9, INSERT ... AS alias 86 (executor half),
  agg R32b upsert/merge 48, dml correlated subqueries 9, views R23, EXCLUDED typing, MERGE correlated
  target). Files: crates/pgparser/src/parser.rs (insert target/alias/indirection, ON CONFLICT
  conflict_target, merge 6863 source table_ref, RETURNING OLD/NEW), crates/pgexec/src/exec.rs
  execute_write regions (INSERT/UPDATE/MERGE arms 3308-8300, ON CONFLICT DO UPDATE binding, RETURNING
  old/new evaluation), viewwrite.rs.
- N39c constraint / trigger fidelity (L, ~1,000 lines: ctp-period-temporal-fk 250 + ctp-format-type-
  range 136 (without_overlaps), ctp-exclusion-constraints-full, ctp-replica-identity 73 + 50 +
  publication 33 (ALTER TABLE REPLICA IDENTITY, pg_class relreplident, indisreplident), ctp-c-regress-
  trigger-adapter 113 (trigger_return_old etc.), ctp-column-dependency-tracking 192 (DROP COLUMN with
  dependent views/triggers), ctp-trigger-deparse-and-info-schema, ctp-detail-failing-row ~50 (shared
  N32), ctp-comment-on-subobjects, ctp-partition-trigger-clone-rules, ctp-domain-integration-gaps 40,
  triggers create_table leak 52 (N31a), ctp-whole-row-star-reference (N07/N12b)). Files: crates/pgexec/
  src/exec.rs [reject_temporal_foreign_key 26489, enforce_exclusion_constraint 10249, enforce_not_null
  3240 / enforce_check_constraints 26688 DETAIL, DROP COLUMN dependency check, REPLICA IDENTITY action
  28981, pg_class relreplident 20148], fk.rs PERIOD FKs (shares fk.rs with N31b -> sequence after),
  func.rs builtin_format_type (2674) ranges, routine.rs STATIC_REGRESS_ENTRYPOINTS/
  call_regression_c_adapter (4782), trigger.rs invoke/when_matches (1408)/drop 598/set_table_trigger_mode
  2150, catalog_fn.rs 600-705 pg_get_triggerdef, parser.rs 7825 EXCLUDE, alter_table_action REPLICA
  IDENTITY.
- N40a regex engine: PostgreSQL ARE port (XL, ~500 lines: regex 381, strings regex-engine 18 +
  similar-explain-fold 55 + similar-substring 17, regexp-arg-msgs 19, arrays like-any 48 partly;
  DECISION: port src/backend/regex (Spencer) vs keep the regex crate with a translation layer — back-
  references, \1 in patterns, ARE escapes, PG error texts). Files: crates/pgexec/src/regexp_fn.rs
  (472 compile_pattern), crates/pgexec/src/pattern.rs (117 similar_to_regex), NEW crates/pgtypes/src/
  regex/ (port).
- N40b scalar type fidelity (L-XL, ~2,600 lines: name type 63-byte truncation 82 (datum.rs 1002 name ->
  Text), float8 send/recv 247 + math/pow special cases 116 + erf/gamma 100, numeric typmod-overflow/
  negscale 69 + overflow-detect 22 + width_bucket 40+9 + to_char/to_number 18 + arith 12 + gen-series 22
  + lcm 24, uuid assignment cast I/O-to-string 165 (pgtypes cast.rs 314) + uuidv7/extract 89, enum
  pg_enum 79 + funcs 104+8 + unknown-literal-adopts-type 128 + anyenum 9 + unsafe-new-value 18 + alter
  msgs 8, strings scs 82 + bytea input msgs 24 + to_bin/oct 64 + int-bytea 96 + unistr 15 + toast 6,
  unknown-literal typing (dgr 59, tsearch/tstypes 244+282 -> shared with N25), expressions sql-value-fn
  precision 62 + current_catalog 6 + typmod-casts-views 50, case const-folding 33 + operator-on-domain
  13, text undefined-object-msg 5 + datestyle-concat 4 + format(*) 47, arrays array-fn-family 89 +
  point-subscript 24 + concat-op 7 + anyall-msgs 8 + array-literal-detail 26 + pg_input-type-brackets
  24 + assign-validation 5 + width_bucket 9 + fipshash-resolution 18 (UDF overload with column args,
  also brin_multi/rowsecurity 440 cascade!), collate literal-collate-fold, int->text assignment cast
  (planner_join-xc, stats_ext 8, views R36 3)). Files: crates/pgtypes/src/{datum.rs 1002, cast.rs 314,
  numeric.rs 48 Typmod, float8 I/O}, crates/pgexec/src/{func.rs 3128 power, eval.rs 1370 Pow / 1473
  coerce_untyped_literal_operands, math_fn.rs 792 width_bucket, string_fn.rs 1119 format_sql,
  usertype.rs 505-535/1281 enum + pg_enum rows, routine.rs resolve_call (fipshash overload with column
  args), catalog_rel.rs pg_enum}.
- N40c collations (L, ~450 lines: collation-derivation 119, create-collation 82 + collate.utf8 168 +
  publication 12 (CREATE COLLATION incl. provider = builtin, locale, deterministic), collation-for 20,
  index-collation 14, explain-sortkey-collation 12, domain-func-resolution 7, ...; ordered-index text
  keys under non-C collations need collation sort keys (P3 dependency)). Files: crates/pgparser/src/
  parser.rs expect_collation_name (1346) + CREATE COLLATION, ast.rs Expr::Collate (3753 no-op today),
  crates/pgtypes collation module (icu/builtin providers, deterministic flag), crates/pgexec/src/eval.rs
  collation derivation (implicit/explicit, 'collation mismatch' errors), catalog_rel.rs pg_collation.
- N40d encoding conversions + regress C functions (M-L, ~620 lines: regress-c-encoding-functions 172
  (test_bytea_to_text ...), multi-encoding-library + regress-c 430 (conversion), convert_to 8,
  conversion-ddl 12, copyencoding LATIN1). Files: crates/pgexec/src/routine.rs regression-C adapter table
  (shared with N30a/N39c -> sequence), NEW crates/pgtypes/src/encoding/ (conversion tables for the
  encodings the tests exercise), parser.rs/ast.rs 2156 CREATE/DROP CONVERSION, catalog_rel.rs
  pg_conversion.
- N41 catalog self-description (L, ~1,000 lines: type_sanity pg_type columns 111 + fixture 42 + pg_range
  22 + self-fixture 27 + C entrypoint 5, opr_sanity column naming 24 (proargtypes[0]::regtype header) +
  pg_type columns 14 + correlated ARRAY subquery 5 + regtype/oid unify 5 + tableoid 29 + regprocedure
  quoting 14 + oidvector 31 + amvalidate 7 + pg_am 10 + user opclass family 8 + self-fixture 116,
  misc_sanity self-fixture 210 + missing catalogs 7 (pg_shdepend), oidjoins pg_get_catalog_foreign_keys
  221, psql pg_type columns 12 + fixture 19 + missing catalogs 82, part-regclass-oid-union-types 15,
  catalog oid columns typed integer -> oid). Files: crates/pgexec/src/catalog_rel.rs (pg_type full 32
  columns, pg_range, pg_am, pg_shdepend, catalog PK/index/toast self-description, oid typing 1218 etc.),
  exec.rs pg_type_rows / pg_class_rows for catalogs (relkind, reltoastrelid, indexes of catalogs),
  catalog_fn.rs pg_get_catalog_foreign_keys SRF (srf.rs), reg_fn.rs regprocedure quoting,
  crates/pgexec/src/setops.rs regclass/oid unification.
- N42 psql describe support (M-L, ~550 lines: builtin pg_get_function_* 188 (pg_get_function_arguments/
  identity_arguments/result for builtins), sequence describe type + Owned by 106 (with N35a), format_type
  4, describe utility \gdesc 13+18, bind-count FATAL 4+8, empty SELECT 2+4, BEGIN ATOMIC \df+ 11,
  builtin SQL-bodied functions 67, tableoid 18, obj_description 2, empty query in aborted tx 2, param
  inference in func args 13, AUTOCOMMIT-off `\;` batch 87 (needs live repro), pg_prepared_statements
  text 8 (N29b)). Files: crates/pgexec/src/catalog_fn.rs (pg_get_function_* over builtin_procs), func.rs
  builtin_format_type, session.rs describe/\gdesc utility describe + bind-count FATAL + simple_query
  batch, catalog_rel.rs pg_sequences.

Decision-gated items not in a batch until answered: N21x, N24 (variant), N29c, N36c, N40a, P7 (real
workers?), P8, SERIALIZABLE (planner_join-xc-txn-serializable + txn serializable ~60 lines, XXL SSI),
compression variant (_1 no-lz4), collate.utf8 (builtin provider), jsonpath_encoding (UTF8 target).

## 3. Batches

Rules: dependencies respected; within a batch no two workstreams claim the same FUNCTION-LEVEL region of
a shared file; whole-file overlaps of exec.rs / session.rs / parser.rs / ast.rs / catalog_rel.rs /
routine.rs / srf.rs / pgtypes datum.rs are ACCEPTED only when the regions are disjoint and are listed
per batch. Workstreams marked (certify alone) get their own certification run before the batch is
ratcheted (see memory: never ratchet from an artifact certified while another slice was in flight).

Batch 0 (optional, certify alone): N00 exec.rs carve-out. Rationale: removes the exec.rs bottleneck for
every later batch; pure moves. If skipped, batches below still hold with the listed region overlaps.

Batch 1 (enablers + self-contained subsystems; biggest cheap recovery first): N01, N02 (certify alone
after the batch), N03, N04, N05, N06, N20 (certify alone: SCHEMA_VERSION), N22, N24, N25, N30a, N41.
Recovers ~13,000 lines directly and unblocks ~5,000 for later batches. Overlaps accepted: parser.rs
(N01 opt_select_into | N20 parse_type_name/interval_literal | N25 create/alter_text_search |
N24 XMLTABLE); exec.rs (N02 key_source_rows/ensure_blocking_rows_fit/append_from_item/lateral_join/
19243 | N05 Update arm + PgClassRow | N20 coerce/catalog_typmod | N25 text_search_catalog_rows |
N41 pg_type/pg_am/catalog rows); srf.rs (N02 MemoryBudget const | N03 classify/plan/rewrite);
routine.rs (N03 validate_plpgsql_scalar/inline_scalar_call | N30a RegressionCAdapter);
pgtypes/datetime.rs (N20 rounding helpers | N22 template std-mode); session.rs (N04 GUC table only).

Batch 2 (planner Phase 1 + independent subsystems): P0h first (tiny header commit), then P0a (certify
alone at milestones), P0b, P2, P3 (certify alone: index key rebuild), N12a (certify alone: array
storage), N15, N17, N19, N26, N34c, N37b, N38. Overlaps accepted: exec.rs (P0a read path 13300-19330 /
24100-24900 / execute_read | P3 index_entries/backfill/CREATE INDEX validation | P2 CREATE STATISTICS
arm | N26 FDW arms 1826-1952 | N34c CREATE CAST/AM/TABLE OF/LIKE arms); session.rs (P0a
run_select_traced | P0b explain | P2 run_maintenance); scanner.rs (P0a Seq Scan leaf wrapper | P3
ordered index cursor); parser.rs (P0b explain options | P2 CREATE STATISTICS | N15 func_call/
positional_from_named | N26 FDW productions | N34c CREATE TYPE/CAST/AM/OF/LIKE); ast.rs (P0b
ExplainOptions | N15 FuncArgs | N26 FDW variants); catalog_rel.rs (P2 pg_statistic* | N26 pg_foreign_*
| N34c pg_am/pg_type); pgtypes datum.rs (N12a ElemType) — N40b's name-type change waits for batch 3;
srf.rs (N15 arg mapping | N19 ProjectSet internals; P0a calls srf.rs unchanged); window.rs (N17
frames; P0a calls execute unchanged); routine.rs (N15 bound_args | N38 plpgsql_scalar_result_type).

Batch 3 (after P0a certified; planner Phase 2a + language/DDL families): P1, N07, N09, N10, N12b, N12c,
N13, N14, N16, N18, N23, N27, N31a, N32, N40b, N40c, N40d. Overlaps accepted: parser.rs (N07
FROM/select/expr | N09 error positions | N16 WITHIN GROUP after N15 | N27 publication/subscription |
N31a opt_partition_by | N32 alter_table_action INHERIT/ALTER CONSTRAINT | N40c CREATE COLLATION |
N40d CREATE CONVERSION); exec.rs (P1 const-qual gating in plan/ only | N10 named_expr_inner/
BindPass | N14 build_table_expr Function arms + schema path (or plan/bind RTE) | N31a partition
regions 25588-25720 / 17478 / 20315 / ~29912-30178 / attach_partition_ops column check | N32
inherited_table_definition 25217 / drop_table_and_dependents 9304-9600 / ALTER TABLE NOT NULL,
CHECK, INHERIT ~29092-29489 | N27 dispatch + execute_write_parts hook + DropColumn 28370 | N18
execute_write_parts DML CTE order (coordinate with N27 on 4361)); routine.rs (N12b resolve_type | N13
executor | N14 table_function_* | N40d adapter table); scope.rs (N09 messages | N12b 1299); eval.rs
(N12c row compare | N40b coerce_untyped_literal_operands | N40c collation derivation); catalog_rel.rs
(N27 pg_publication* | N32 pg_constraint | N40b pg_enum | N40c pg_collation); pgcatalog lib.rs/serde.rs
(N32 constraints | N27 publication records | N31a none (partition.rs own keyspace)); pgtypes datum.rs
(N40b name/numeric | N12b none). N31a and N32 both touch attach_partition_ops/inheritance.rs
children_of: N31a owns partition.rs ordering + attach column check; N32 owns inheritance.rs.

Batch 4 (planner Phase 2b/3 + remaining DDL/DML): P4 (certify alone), N21, N29a, N29b, N31b, N31c,
N33a, N34a, N34b, N35a, N36a, N36b, N39a, N39b, N42. Overlaps accepted: exec.rs (P4 removes
try_scan_with_local_index only | N31b FK regions 718/29302/29390/943 + partition_definition FK clones |
N31c execute_timestamp_update/MERGE/INSERT routing | N33a partition-index regions 25438-25529 /
pg_inherits_rows / pg_attribute_rows / attach_partition_ops index half / DropIndex / REINDEX | N34b drop
notices 9526-9600 (coordinate with N32 done) | N35a column_from_ast / drop_table_ops sequences |
N39b execute_write INSERT/UPDATE/MERGE arms (coordinate with N31c on MERGE: N31c owns partitioned
routing, N39b owns grammar/RETURNING/ON CONFLICT) | N29b pg_database rows); session.rs (N36a cursors
| N36b simple_query/txn | N39a run_copy_in | N42 describe/\gdesc | N29b prepared statements);
parser.rs (N33a alter_index/drop_index | N34a ALTER OPERATOR | N34b DROP OWNED/REASSIGN | N35a
sequences | N36b SET TRANSACTION | N39b DML productions); lexer.rs (N34a only); fk.rs (N31b only);
catalog_rel.rs (N29a pg_stat_* | N29b pg_locks | N33a pg_index/pg_inherits | N34b pg_depend |
N35a pg_sequences); catalog_fn.rs (N33a IndexDef | N34b object address | N42 pg_get_function_*);
srf.rs (N29a? no; N34b FROM forms | N31a done); trigger.rs (N39a invoke).

Batch 5 (planner Phases 3/4 + remaining subsystems): P5 (certify alone: sort tie order), P6, N28, N30b,
N33b, N34d, N37a, N37c, N39c, plus decision-gated N40a/N36c/N21x/N29c/N24-variant if approved.
Overlaps accepted: exec.rs (P6 partitioned_scan/inherited_scan re-home | N28 execute_write_parts
rule hooks | N30b CreateView 1095/2152 + build_base_table view expansion | N34d ALTER TABLE region
28900-30300 (N32/N33a/N31b done) | N37a grant arms | N39c enforce_*/reject_temporal/DROP COLUMN
dependency); session.rs (N30b PREPARE on views | N37a SET ROLE | N28 dispatch); parser.rs (N28 RULE
grammar | N34d alter_table_action rest + alter_generic | N37a GRANT/ROLE | N39c EXCLUDE/REPLICA
IDENTITY | N33b COMPRESSION); fk.rs (N39c temporal after N31b); viewdef.rs (N30b | N28 rule deparse
-> N30b owns viewdef.rs, N28 adds pg_get_ruledef in catalog_fn.rs); routine.rs (N39c adapter table
after N40d); rls.rs (N30b barrier ordering).

Batch 6 (planner Phase 5 + tail): P7 (accounting mode; certify alone), N35b (after N34d), N11 only if
P0a slipped, remaining decision items (SERIALIZABLE), variant-file decisions.

Batch 7 (planner Phase 6, optional): P8 index AMs.

Certify alone: N00, N02, N20, P0a (each milestone), P3, N12a, P4, P5, P7, N36c, N40a, P8, N21x.

## 4. Planner / EXPLAIN / statistics / index-read-path programme

Phase 0 — prerequisites (batch 1): N01, N02, N03, N04, N05, N06 (+ window.rs emitting rows in
window-sort order can be pulled forward from P5 as an S item). Moves: ~13,000 lines directly;
un-cascades explain 693 / memoize 308 / select_parallel 1,148 / join_hash 843 / incremental_sort 119 /
portals 725 / tuplesort 438 / limit 134 so the planner becomes measurable on them. Files: parser.rs
opt_select_into, scanner.rs/exec.rs/join.rs/agg.rs/grouping.rs/cte.rs/setops.rs/srf.rs memory sites,
routine.rs/srf.rs/regexp_fn.rs, session.rs GUC table, exec.rs Update arm + relstats.rs, scripts.
Exit: every explain_* wrapper statement in the corpus reaches EXPLAIN (0 'only supported in FROM
position' errors); `set` of every planner GUC used by the schedule is accepted; int4_tbl has 5 rows in
join.out; `SELECT * FROM tenk1 ORDER BY unique2` and `abbrev_abort_uuids ORDER BY` succeed; join_hash
runs to `rollback` without 'current transaction is aborted'.

Phase 1 — Query IR + node executor + EXPLAIN renderer (batch 2: P0h, P0a, P0b): bind AST -> Query
(RangeTbl, TargetEntry, Var{rti,attno,ty}, RestrictInfo{security_level, leakproof}), single-relation
plan tree executed for real (Seq Scan -> Filter -> Agg|Sort|Unique|Limit|ProjectSet|WindowAgg|Result;
Values/Function/Subquery/CTE/Named Tuplestore/Table Function scans; nested loops in FROM order for
joins as today), per-node ntuples/nloops/rows_removed counters, VERBOSE Output with schema
qualification, full JSON/YAML/XML key sets with zero costs, SUMMARY/BUFFERS/MEMORY/SERIALIZE/SETTINGS/
GENERIC_PLAN options, EXPLAIN EXECUTE/DECLARE/CTAS/SELECT INTO/CREATE MATERIALIZED VIEW dispatch,
InitPlan/SubPlan numbering from subquery folding, typed deparse from BoundExpr ('42'::bigint,
'(0,1)'::tid, 'ATAAAA'::name, ordinals resolved), RLS quals deparsed in PG order, describe walk
deleted, old read path deleted at the end. Invariants: RawScan single exit, ReadPermit before any read,
statement-level snapshot, check_query_canceled at node boundaries, RangeScanner leaves. Files:
crates/pgexec/src/plan/{mod,query,bind,rewrite,createplan,explain,deparse}.rs, plan/exec/*, exec.rs
read path, query.rs, session.rs run_select_traced/explain, pgparser ExplainOptions. Expected to move
~3,500 lines (VERBOSE Output ~1,100, renderer nodes ~700, formats/options ~500, ANALYZE actuals, EXPLAIN
utility dispatch, describe-path errors ~500) and to hold every currently-exact file exact. Exit:
zero regressions on the 56 exact files + Phase-0 gains; explain.out format/option blocks match except
the parallel JSON block; select_into, tsrf, rangetypes, fast_default EXPLAIN blocks match; EXPLAIN
ANALYZE prints per-node actual rows/loops and '(never executed)'; `cargo test -p crabka-pgexec` green
with the read path served only by plan/exec.

Phase 2a — rule-based transforms (batch 3: P1): pull_up_subqueries, distribute quals to scans vs
joins, flatten AND/OR, IN -> = ANY, X = X -> IS NOT NULL, NullTest reduction on NOT NULL columns,
constant-false -> Result / One-Time Filter: false (also Join Filter: false), remove_useless_joins,
self-join elimination, sublink -> semi/anti join, single-row VALUES -> Result, non-materialized CTE
inlining, immutable call folding, alias numbering (_1.._n, "*VALUES*_1", unnamed_subquery), Var
qualification iff >1 RTE, join-type suffixes with ON quals; executor gates one-time quals once.
Files: plan/rewrite.rs, plan/bind.rs, plan/deparse.rs. Moves ~2,500 (predicate 238 exact, equivclass
structural, join ~1,200 structural, union pushdown, window/rowsecurity shape). Exit: predicate.out
exact; every join.out EXPLAIN whose expected plan contains no Hash/Merge/Index/Bitmap/Materialize/
Memoize node matches; subselect tattle NOTICE counts match.

Phase 2b — statistics (batch 2: P2, in parallel with Phase 1): ANALYZE computes PG's
compute_scalar_stats (nullfrac, width, ndistinct, MCV, histogram, correlation; deterministic for
<= 30k rows), pg_statistic + pg_stats + pg_statistic_ext[_data] + pg_stats_ext, extended statistics
(ndistinct/dependencies/mcv), pg_restore_*/pg_clear_* import functions with WARNINGs, relpages/
relallvisible emulation (estimate_rel_size + heap-page simulator per table), selfuncs port. Files:
plan/stats.rs, plan/selfuncs.rs, relstats.rs, catalog_rel.rs, stats_fn.rs, session.rs run_maintenance,
exec.rs CREATE STATISTICS. Moves ~2,000 (stats_ext 1,215, stats_import 645, pg_stats refs). Exit:
stats_ext 'estimated' column exact for every MCV/ndistinct/dependencies query; stats_import exact;
`SELECT reltuples, relpages FROM pg_class` for tenk1/onek after test_setup's VACUUM ANALYZE equals PG.

Phase 2c — index storage (batch 2: P3, in parallel): memcomparable secondary-index keys (crates/pgkv
keyenc.rs; NULLS FIRST/LAST, DESC by inversion; C/POSIX and default collation first), index catalog with
per-key direction/nulls/opclass/collation, predicate (partial), INCLUDE columns, expression entries,
unique expression indexes, hash-method entries, NULLS NOT DISTINCT, opclass parameters, index rebuild
via local_index_backfill_ops, ordered index cursor in scanner.rs (local only). Moves ~700 directly
(index_including 256+83, partial/DESC/unique-expression refusals, insert_conflict expression indexes,
create_index DDL). Exit: zero 'not supported' CREATE INDEX errors in the whole schedule; pg_index /
pg_get_indexdef / \d index output match for indexing/index_including/create_index DDL blocks; ORDER BY
over an indexed column can be served by the index cursor in a unit test.

Phase 3 — cost-based planner core (batch 4: P4, certify alone): cost.rs (all cost_* + PG 18
disabled_nodes + enable_*/work_mem/random_page_cost/... GUCs), paths.rs (RelOptInfo, add_path,
pathkeys, equivalence classes), indexpath.rs (clause matching via builtin_opclasses/opfamilies, Index
Cond vs Filter, bitmap AND/OR, index-only eligibility with a visibility-map analogue -> Heap Fetches),
joinpath.rs (join_search_one_level, nestloop/hash/merge, parameterised inner paths, Materialize/
Memoize placement, semi/anti/unique), Tid/Tid Range paths, hash join with LIFO bucket order,
HashAggregate simplehash iteration order + PG hash functions (hashint4/hashtext/hash_array), join-side
heuristic result (geometry), planagg MIN/MAX -> InitPlan + Index Only Scan Backward, plan/dist.rs for
sharded relations always preferred until costed. Files: plan/{cost,paths,indexpath,joinpath}.rs,
plan/exec/{hashjoin,mergejoin,nestloop,material,memoize,indexscan,indexonlyscan,bitmap,tidscan}.rs,
join.rs bodies, scanner.rs index cursor consumers, exec.rs try_scan_with_local_index removed. Moves
~11,700 (join ~4,600 EXPLAIN + ~2,050 row order, subselect ~1,000, aggregates planagg ~500,
create_index btree ~800, btree_index 243, union ~500, equivclass ~300, select 130, limit 60, tidscan/
tidrangescan ~130, memoize 48+, join_hash 93+, updatable_views 405, select_views 113, rowsecurity 365,
txn ~250, views 614, geometry 1,334, misc). Exit: EXPLAIN (COSTS OFF) of every statement in join.sql,
subselect.sql, equivclass.sql, select.sql, limit.sql, tidscan.sql, tidrangescan.sql, aggregates.sql
planagg section, create_index.sql btree section matches upstream node choice on the tenk1/onek/int4_tbl/
int8_tbl data under the schedule's enable_* GUCs; unsorted result order of join.sql cross joins and
geometry.sql cross joins matches; union.sql / select_distinct.sql hashed output order matches.

Phase 4 — upper relations and sort fidelity (batch 5: P5, certify alone): pg_qsort (sort_template.h)
port for every sort site incl. top-N bounded heapsort, one global sort per window with
select_active_windows ordering and optimize_window_clauses, Run Condition, Incremental Sort with
Presorted Key and group counts, Hash/Group/Mixed aggregate strategy + Partial/Finalize, GroupAggregate
sorted output, DISTINCT Unique-vs-HashAggregate, SetOp/HashSetOp, LockRows, ordered-set placement.
Files: plan/grouping.rs, plan/exec/{agg,windowagg,incsort,unique,setop,sort}.rs, window.rs, agg.rs
sort sites, scanner.rs top-K. Moves ~3,200 (window 1,196 tie + 197 cost + shape, groupingsets 685,
incremental_sort 499, tuplesort 90, select_distinct(_on) 220, aggregates ~300). Exit: window.out
exact except non-planner roots (frames, moving aggregates, LINE carets); groupingsets/incremental_sort
EXPLAIN blocks match; DISTINCT ON without ORDER BY keeps PG's tie choice.

Phase 5 — partition-aware planning (batch 5: P6; needs N31a, N32, N33a): Append/Merge Append over
leaves in PartitionDesc order with _n aliases (surviving children only), partprune.c port (static +
run-time + InitPlan params, Subplans Removed, (never executed)), constraint_exclusion for inheritance
CHECKs, partitionwise join/aggregate under enable_partitionwise_*, EXPLAIN EXECUTE generic plans with
$n under plan_cache_mode = force_generic_plan, Update/Delete child lines, VERBOSE public.part_p1 p_1.
Files: plan/partition.rs, partition.rs ordering, inheritance.rs, plan/exec/append.rs, session.rs
prepared statements. Moves ~8,200 (partition_prune ~3,400, partition_join ~3,600, partition_aggregate
~1,100, inherit ~750). Exit: partition_prune.out and partition_join.out exact; partition_aggregate
exact except parallel blocks (Phase 6); inherit Append/Merge Append blocks match.

Phase 6 — parallel plan shapes (batch 6: P7): plan parallel, execute serially, account
ntuples/nloops as if parallel (nloops = outer x (workers+1)), Workers Planned = Launched from
max_parallel_workers_per_gather / parallel_workers reloption / min_parallel_*_scan_size, Gather /
Gather Merge / Parallel Seq|Index|Bitmap Scan / Parallel Append / Parallel Hash / Partial+Finalize
Aggregate, 'Worker N: Sort Method' strings; DECISION whether real workers are required later.
Files: plan/parallel.rs, plan/exec/gather.rs, cost.rs. Moves ~2,200 (select_parallel ~900,
write_parallel 42, join_hash ~600, partition_prune 365, others ~300). Exit: select_parallel.out and
write_parallel.out exact; join_hash exact except hash_join_batches() JSON needing batch counts from
work_mem/hash_mem_multiplier (ExecChooseHashTableSize port, part of P4).

Phase 7 — index access methods (batch 7: P8, optional): GiST/SP-GiST/BRIN/hash/GIN-generic entries and
scans with opclass operator support, KNN Order By, brin_summarize_* functions, gin_fuzzy_search_limit /
gin_clean_pending_list, WindowAgg wrappers in create_index_spgist. Moves ~1,400. Exit:
create_index_spgist, gist, spgist, brin, brin_multi, brin_bloom, gin, hash_index, box, polygon exact.

## 5. Open questions (decisions the user must make)

1. Parallel query: accept plan-parallel/execute-serial accounting (P7 = M, prints identical text) or
   require real parallel workers (XL, later swap keeps the planner)?
2. XML: target xml.out (libxml-parity xpath/xmltable in the native quick-xml implementation) or the
   no-libxml variant xml_1.out (1,327 lines today, mostly 'unsupported XML feature' stubs)? xmlmap -> xmlmap_1?
3. Variant expected files: compression (_1 = no-lz4: cheaper; or lz4 build), collate.utf8 (real
   target collate.utf8.out needs the builtin provider; _1 is the skip variant), jsonpath_encoding
   (target the UTF8 file, not _2), password_1, prepared_xacts_1 (with max_prepared_transactions=0 the
   file is byte-exact = S), xml/xmlmap.
4. SERIALIZABLE isolation (SSI, XXL) for ~60 lines (tidscan 8, transactions 28, portals 17, vacuum 1,
   temp 22 PREPARE TRANSACTION refusal): in scope now, or accept those lines red?
5. Heap physical order after UPDATE (new tuple version at the end of the heap; ~40-80 lines in inherit,
   rules, returning, with, psql, cluster, ctp): emulate against the timestamp-domain rowid contract
   (delete+insert placement) or accept red?
6. Datetime calendar range (jiff +-9999 vs PG 4713 BC..294276 AD, 247 lines): replace jiff's civil
   arithmetic with a PG-faithful julian-day port (XXL) or accept red?
7. System catalog view definitions (rules 1,437 lines = pg_views dump of 80 pg_catalog views): honest
   route (bootstrap system_views.sql with dozens of SRFs, XXL) or accept red / store definition text as
   catalog data for the views Gres does not execute?
8. Rule system (XXL, ~1,450 lines): full query rewriter, or scoped to the shapes in rules.sql/
   updatable_views.sql (DO INSTEAD/ALSO on INSERT/UPDATE/DELETE, ON SELECT view rules, conditional rules)?
9. Text search (XXL, ~2,850 lines incl. ispell/hunspell/thesaurus dictionary files): full subsystem port
   in scope now, or after the planner?
10. Regex engine: port PostgreSQL's Spencer ARE engine (XL, ~500 lines) or keep the regex crate with a
    translation layer (covers \m \M and simple ARE differences but not back-references)?
11. exec.rs carve-out (N00) before batch 2, or accept the per-batch region overlaps listed above?
12. Cost fidelity policy: accept a heap-page simulator (relpages from tuple width, dead tuples until
    VACUUM) and certify per file, or first measure how many plans in the corpus depend on relpages of
    never-ANALYZEd tables (estimate_rel_size fallback)?
13. Memory policy: statement-level cap + work_mem GUC (no spill) for the regress run vs real spill; the
    soak/loadtest harness observes the change.
14. Large objects (L) and the cumulative statistics system (XL) — in scope for this programme?
15. Dropped-column placeholders (attnum stability, XL, cross-cluster) — in scope, or accept the 30 +
    downstream lines red?
16. Certification cadence: which workstreams may be batched under one certification and which must be
    certified alone (proposal in section 3).
