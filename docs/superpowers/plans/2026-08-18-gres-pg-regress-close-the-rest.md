# Close the remaining pg_regress gaps, EXPLAIN plan text included

## Context

Gres (Crabka's PostgreSQL-compatible engine) is measured against PostgreSQL 18.4's
unmodified `pg_regress` schedule. The certified state on `main` (CI run 32047096161,
artifact 9295866181, commit `14c87bfe0`) is **56 / 231 files exact, 175 failing,
110,197 canonical changed lines, 4,903 hunks**. The floor is
`crates/gres-conformance/pg-regress-baseline.json`.

The earlier programme (docs/superpowers/plans/2026-08-02-gres-pg-regress-100-percent.md)
recorded EXPLAIN as "not a target" and left rules, publications, large objects, FDW
objects, text-search dictionaries and several other families as matrix `Non-goal`s. The
user has now put **everything** in scope: 231/231, including plan text. That means Gres
needs a real cost-based planner whose plan tree is what the executor runs and what
`EXPLAIN` prints, plus every absent subsystem the schedule exercises.

This plan was built from a full triage of the CI `regression.diffs` (all 175 files
bucketed into 799 roots by 16 analysts, the 36 largest roots adversarially re-verified,
an EXPLAIN census, and an executor-architecture study). The ledgers live in
[`docs/superpowers/notes/2026-08-18-pg-regress-triage/`](../notes/2026-08-18-pg-regress-triage/README.md).

## What the triage established (load-bearing facts)

Line kinds across the 110,197 (whole-block attribution, ±30%):

| kind | lines |
|---|---:|
| cost-planner-only (node choice, join order/method, index/bitmap/tid access, Materialize/Memoize/Sort placement, parallel, partitionwise, row estimates) | ~23,000 |
| deterministic plan shape / EXPLAIN renderer (needs a plan tree + typed deparse, no cost model) | ~6,600 |
| cross-join row order fixable by PG's join-side rule (geometry 1,334) | ~1,340 |
| sort tie order (pg_qsort + window ordering; window 1,196) | ~1,200 |
| parser / executor / catalog / DDL / types / functions / absent subsystems | ~78,000 |

- The EXPLAIN census over the failing files: 2,106 EXPLAIN statements, 29,857 changed
  lines inside QUERY PLAN blocks, **only 1 block prints unmasked costs** (34 filtered,
  126 with `actual`). Decision fidelity, not numeric fidelity, is the target. Node
  inventory (expected side): Seq Scan 2,886, Sort 564, Append 508, Hash 474, Index Scan
  420, Result 396, Nested Loop 350, Index Only Scan 340, Aggregate 246, Bitmap Index
  Scan 216, Hash Join 196, Nested Loop Left Join 185, Bitmap Heap Scan 183, Materialize
  158, Parallel Seq Scan 142, Gather 91, WindowAgg 75, HashAggregate 118, GroupAggregate
  58, Incremental Sort 52, Merge Append 52, Memoize 36, Merge Join 37, ProjectSet 37,
  Gather Merge 34, Tid Scan/Tid Range Scan 25, … 51 files toggle `enable_*` 569 times.
- Gres today: no IR — the executor walks the AST (`exec.rs:19170
  select_to_relation_with_ctes`); `EXPLAIN` is a second, independent syntactic walk
  (`explain.rs:77`); secondary indexes are equality-only (`pgkv key.rs:248`, non-order-
  preserving row encoding); join order = FROM order; hash `JoinIndex` when an equality key
  exists but always printed `Nested Loop`; no pruning, no partitionwise, no parallel.
- **93 of the 175 files can become exact with no planner at all** (149/231 total); 82 need
  the planner for part of their lines (25 of those for <100 lines).
- Cheap producers poison many files: `INSERT INTO int4_tbl SELECT 1 INTO f` in
  `select_into` is executed instead of erroring, adding a 6th row to `int4_tbl` for every
  later file (~450 lines); nine compile-time users of `scanner::BLOCKING_QUERY_MEMORY`
  (16 MiB) ignore the 20 MiB flag and produce 53200 in 21 files (~2,000 lines); user
  SETOF functions in a select list are refused (`routine.rs:2112`), which hides ~2,900
  EXPLAIN lines behind `explain_filter()` wrappers (`explain`, `memoize`,
  `select_parallel`, `incremental_sort`, `merge`, `partition_prune`, `stats_ext`);
  `UPDATE pg_class` is refused (`join_hash` 744); missing `max_prepared_transactions` GUC
  costs the whole `prepared_xacts` file (286, S); the harness connects as `dbname=crab`
  while the oracle is `regression` (~144 lines, one-line fix).
- Corrections to earlier programme notes: `partition_join`/`partition_prune` are NOT
  cascades (86%/71% is EXPLAIN over existing tables); `geometry` has zero function
  failures — 1,334 of 1,346 lines are nested-loop side order; `rules` is 58% one block (the
  `pg_views` dump of pg_catalog view definitions, 1,437 lines); `explain.out` is not
  failing on EXPLAIN but on the SRF wrapper; `xml` scores against `xml.out` (Gres has a
  native XML type) while `xmlmap` scores against `xmlmap_1.out`; `jsonpath_encoding`
  should target the UTF8 file, not `_2`; datetime `typmod` (`timestamp(2)`,
  `interval(p)`) is parsed and discarded — 1,980 lines, the largest single scalar root.

## Decisions (defaults; override at approval if you disagree)

1. **Parallel query**: plan parallel paths for real (cost model, Gather/Gather Merge/
   Parallel Seq|Index|Bitmap Scan/Parallel Append/Parallel Hash, Partial/Finalize) AND
   execute them with real worker tasks (tokio tasks over disjoint scan ranges, results
   funnelled through Gather). `Workers Launched: N` must be true. The planning half (P7)
   is the same either way; the executor half (P7x) is XL and lands after P4. Fallback if
   rejected: plan-parallel/execute-serial accounting (M) — text-identical, but the plan
   claims work that never happens; this project has refused that kind of claim before.
2. **Memory policy**: keep the blocking-query budget honest and make statements complete:
   (a) batch 0 routes every constant user through the flag with statement-level
   accounting; (b) the planner's Sort/Hash/Materialize nodes spill under `work_mem`
   (external merge sort, batched hash join, tuplestore) so the regress corpus completes
   under the certified 20 MiB policy without special-casing.
3. **XML**: target `xml.out` (libxml parity in the native quick-xml implementation) AND
   `xmlmap.out` (real `table_to_xml` family). Claiming XML support while scoring `xmlmap`
   against the no-libxml variant is inconsistent; the honest pair costs ~1,200 more lines.
4. **Variant expected files**: `compression_1` (no-lz4 build; legitimate PG
   configuration; pglz already exists), `collate.utf8.out` (real target: builtin
   provider), `jsonpath_encoding.out` (UTF8), `prepared_xacts_1` (max_prepared_transactions
   = 0, legitimate configuration, S), `password_1` as scored today.
5. **Hard-floor items are in scope, scheduled last**: SERIALIZABLE (SSI) ~60 lines;
   jiff calendar range → PG julian-day arithmetic (247 lines); heap order after UPDATE
   (new version at heap end, ~40-80 lines); dropped-column placeholders with stable
   attnums (30 + downstream); pg_catalog view definitions bootstrapped from
   `system_views.sql` (1,437 lines). Each is a decision-gated workstream in batch 6/7 and
   is certified alone; the plan states their cost so they can be dropped explicitly.
6. **Rule system**: the full rewriter (DO INSTEAD/ALSO, conditional rules, ON SELECT view
   rules, NEW/OLD, RETURNING) — that IS PostgreSQL's system; scoping to the test shapes
   would need the same machinery.
7. **Regex**: port PostgreSQL's ARE engine (`src/backend/regex`, Spencer) into
   `crates/pgtypes/src/regex/`; the `regex` crate cannot do back-references or PG's error
   texts. A translation layer for `\m \M \y \Y` lands first (batch 0, S) because it gates
   `explain.out`.
8. **exec.rs carve-out first**: mechanically split `crates/pgexec/src/exec.rs` (39k lines)
   into DDL/catalog modules before batch 1, certified alone. It is the serialisation
   bottleneck for every later batch.
9. **Cost fidelity policy**: emulate `relpages` with a heap-page simulator (PG tuple
   layout, 8 kB pages, dead tuples until VACUUM) and PG's `estimate_rel_size` fallback for
   never-analysed tables; the tests are designed to be plan-stable across platforms, so
   approximate page counts are expected to hold. Measure per file; do not tune to tests.
10. **Certification cadence**: one certification per batch of the committed tree, plus a
    solo certification for every workstream marked "certify alone" (storage-format bumps,
    memory policy, sort-order changes, planner phases). Never ratchet from an artifact
    certified while another slice was in flight.

## Rules of execution (non-negotiable; from CLAUDE.md and the programme's memory)

- Subagent-driven development in **parallel batches with disjoint file sets**. Every brief
  names the agent's file set AND every other live agent's file set (from `git status
  --porcelain` at dispatch time, not from the plan), and says "stop and report rather than
  edit" for foreign files. Every brief demands a section **"Where the brief was wrong"**.
- Never `git checkout -- / restore / stash / clean` in the shared worktree; never
  `cargo +nightly fmt --all` in write mode there; format and gate in the agent's isolated
  `git archive HEAD` tree; copy back only owned files.
- Certification: `scripts/gres-pg-regress.sh gres serial` from an **isolated source
  copy**, `GRES_PG_REGRESS_BIN` pointing at a binary built from that copy, artifact dir
  under `target/pg-regress-runs/<name>/` (never `/tmp`), launched in the background
  (`nohup` + PID file; the Bash timeout clamps at 10 min), machine-wide flock respected
  (`GRES_PG_REGRESS_WAIT=1`). ~40 min per run. Health-check every artifact before reading
  its numbers: 231 `results/*.out`, none empty; an empty/truncated `.out` is a killed run.
- Measure before claiming: A/B against a binary rebuilt from a known commit; a fix that
  is correct can move the metric zero or negative ("unblocked statements fail longer");
  count the population a change acts on; attribute whole change blocks; check line WIDTH
  effects on psql frames.
- Tests exercise behaviour, never source text; `assert2` only; no `#[allow(clippy::…)]`;
  no `unsafe`; no compatibility shims (bump `SCHEMA_VERSION`, wipe data dirs).
- `docs/PG_COMPAT_MATRIX.md` is gated (`tools/check-pg-compat-matrix.sh`): every command
  whose disposition changes (Non-goal → Implemented, etc.) needs its row and probe updated
  in the same change.
- Kill only servers you started (match `--data-dir`/port); create uniquely named
  scratchpad subdirectories; probe destructive catalog behaviour only in a throwaway
  database on a throwaway oracle.
- Prose (commits, PRs, docs) in ASD-STE100 style; run humanizer + unslop before delivery.

## Programme shape

Seven batches. Batch 0 is cheap enablers and the exec.rs split. Batches 1-6 interleave
the planner programme (P-series) with independent feature workstreams (N-series) whose
file sets do not overlap the planner lane. Sizes: S hours, M a day, L several days,
XL 1-2 weeks, XXL multi-week. Total: ~45 workstreams, roughly a dozen XL/XXL — this is a
multi-month programme even with wide parallel dispatch; the batches are ordered so the
largest, safest recovery lands first and every batch ends with a certified, ratcheted
floor.

### Batch 0 — enablers (target ≈ −13,000 lines; two certifications)

| id | workstream | size | lines | file set |
|---|---|---|---:|---|
| N00 | **exec.rs carve-out** (moves only: DDL families → `ddl_index.rs`, `ddl_partition.rs`, `ddl_inherit.rs`, `ddl_alter.rs`, `catalog_rows.rs`; read path stays until P0a). Certify alone. | M | 0 | `crates/pgexec/src/exec.rs`, `lib.rs` |
| N01 | `SELECT … INTO` rejected in nested contexts (`INSERT INTO int4_tbl SELECT 1 INTO f` → `SELECT ... INTO is not allowed here` + caret; `views must not contain SELECT INTO`; `COPY (SELECT INTO)`). | S | ~450 | `crates/pgparser/src/parser.rs` (`opt_select_into` ~12427, `finish_query_statement`, `query_statement`) |
| N02 | Blocking-query memory policy step (a): route the nine compile-time users of `scanner::BLOCKING_QUERY_MEMORY` (`exec.rs key_source_rows/ensure_blocking_rows_fit`, `agg.rs:2872`, `grouping.rs:496`, `cte.rs:492`, `setops.rs:353`, `srf.rs:2321`, `join.rs JoinPolicy::default`) through the flag; statement-level cap; `work_mem` GUC registered; per-conjunct WHERE→ON pushdown (`append_from_item` 13574) and a filter into `lateral_join` (13595). Certify alone (soak harness observes it). | M | ~2,000 | `scanner.rs`, `exec.rs` (named fns), `join.rs`, `agg.rs`, `grouping.rs`, `cte.rs`, `setops.rs`, `srf.rs` (constant only), `crates/gres/src/lib.rs:857`, `scripts/gres-pg-regress.sh:267` |
| N03 | User SETOF functions in the select list (`routine.rs validate_plpgsql_scalar` 2112 / `inline_scalar_call` 2589-2612 → reuse `eval_plpgsql_table_function`); built-in SRF as a grouping key (`srf.rs classify/plan/rewrite_expr/reject_in_aggregate` → ProjectSet below Agg); ARE `\m \M \y \Y \A \Z` translation in `regexp_fn.rs compile_pattern` (472). Un-cascades ~2,900 EXPLAIN lines. | M | ~300 direct | `routine.rs`, `srf.rs`, `regexp_fn.rs` |
| N04 | GUC surface: every planner GUC the schedule sets (`enable_*` ×20, `work_mem`, `hash_mem_multiplier`, `seq/random_page_cost`, `cpu_*_cost`, `parallel_*_cost`, `min_parallel_*_scan_size`, `max_parallel_workers[_per_gather]`, `debug_parallel_query` bool spellings, `jit*`, `track_io_timing`, `compute_query_id`, `plan_cache_mode`, `geqo*`, `join/from_collapse_limit`, `constraint_exclusion`, `enable_partition_pruning`, `enable_partitionwise_*`, `default_statistics_target`, `effective_cache_size`, `cursor_tuple_fraction`), `max_prepared_transactions=0` (→ `prepared_xacts` exact), `intervalstyle`/`password_encryption` HINT texts, role in SET clauses. | S-M | ~500 | `crates/pgexec/src/session.rs` GUC table (~976-1360) only |
| N05 | Writable `pg_class` (superuser `UPDATE pg_class SET reltuples/relpages`) + durable `relpages`/`relallvisible` storage. Unblocks `join_hash` (744, then planner). | M | ~100 direct | `exec.rs` (`execute_write_body` Update arm 6453; `pg_class_rows`/`PgClassRow` ~20380/20826), `relstats.rs` |
| N06 | Harness: connect as `regression` (`GRES_DB`) so printed database names match the oracle. | S | ~144 | `scripts/gres-pg-regress.sh` |

Exit for batch 0: `int4_tbl` has 5 rows in `join.out`; every `explain_*` wrapper statement
reaches EXPLAIN (zero "only supported in FROM position"); every planner GUC in the
schedule is accepted; `SELECT * FROM tenk1 ORDER BY unique2` and the `portals` cursors
succeed; `join_hash` reaches its `rollback`; `prepared_xacts` exact. Ratchet.

### Batch 1 — Planner Phase 1 + independent subsystems (≈ −12,000)

Planner lane: **P0h** (S; header commit freezing `Plan`/`PlanState`/`PlannedStmt`/
`RangeTblEntry`/`RestrictInfo` types and the `Executor` trait in
`crates/pgexec/src/plan/{mod,query}.rs`), then **P0a** and **P0b** (below), **P2**
(statistics), **P3** (index storage). Independent: N12a arrays-of-any-type (certify
alone; storage format), N15 named/default/VARIADIC args, N17 window frames, N19 SRF
ProjectSet semantics, N20 datetime typmod (certify alone; SCHEMA_VERSION), N22 jsonpath,
N24 XML, N25 text search, N26 FDW DDL/catalogs, N30a `interpt_pp` C-adapter (S, 902
lines: unblocks `select_views`), N34c type/cast/AM/typed-table/LIKE DDL, N37b large
objects, N38 PL/pgSQL fidelity, N41 catalog self-description.

Accepted region overlaps (function-level, listed so briefs can name them): `parser.rs`
(P0b explain options | P2 CREATE STATISTICS | N15 `func_call`/`positional_from_named` |
N20 `parse_type_name`/`interval_literal` | N22 none | N24 XMLTABLE | N25 text-search DDL |
N26 FDW productions | N34c CREATE TYPE/CAST/AM/OF/LIKE); `exec.rs` (P0a read path only |
P3 `index_entries`/backfill/CREATE INDEX validation | P2 CREATE STATISTICS arm | N20
coerce/`catalog_typmod` | N26 FDW arms | N34c CREATE CAST/AM/OF/LIKE arms | N41
`pg_type`/`pg_am` rows | N25 `text_search_catalog_rows`); `session.rs` (P0a
`run_select_traced` | P0b `explain` | P2 `run_maintenance`); `scanner.rs` (P0a Seq Scan
leaf | P3 ordered index cursor); `catalog_rel.rs` (P2 `pg_statistic*` | N26 `pg_foreign_*`
| N34c `pg_am`/`pg_type` | N41 rest); `pgtypes/datum.rs` (N12a `ElemType`; N40b waits);
`routine.rs` (N15 `bound_args` | N30a `RegressionCAdapter` | N38
`plpgsql_scalar_result_type`); `srf.rs` (N15 arg mapping | N19 internals); `window.rs`
(N17 frames; P0a calls `execute` unchanged).

### Batch 2 — Planner Phase 2a + language/DDL families (≈ −14,000)

**P1** rule-based transforms; N07 parser lane (FROM/select-list/expression grammar); N09
lexer + syntax-error parity + positions/DETAIL/HINT; N10 result column naming; N12b
relation rowtypes + whole-row refs; N12c row comparison; N13 SQL-function executor; N14
FunctionScan relation builder; N16 aggregate completeness; N18 recursive CTE + SEARCH/
CYCLE + DML-CTE order; N23 JSON/JSONB function fidelity; N27 logical-replication DDL +
catalogs; N31a expression partition keys + partition catalog misc; N32 inheritance + NOT
NULL/CHECK constraint catalog; N40b scalar type fidelity; N40c collations; N40d encoding
conversions + regress C functions.

Overlaps: `parser.rs` (N07 | N09 | N16 WITHIN GROUP after N15 | N27 | N31a
`opt_partition_by` | N32 `alter_table_action` INHERIT/ALTER CONSTRAINT | N40c CREATE
COLLATION | N40d CREATE CONVERSION); `exec.rs` regions per workstream (N10
`named_expr_inner`/`BindPass` | N14 `build_table_expr` Function arms or their `plan/bind`
successors | N31a partition regions 25588-25720/17478/20315/~29912-30178 | N32
`inherited_table_definition` 25217, `drop_table_and_dependents` 9304-9600, ALTER TABLE
NOT NULL/CHECK/INHERIT ~29092-29489 | N27 dispatch + `execute_write_parts` hook + DropColumn
28370 | N18 `execute_write_parts` DML-CTE order — coordinate with N27); `routine.rs`
(N12b `resolve_type` | N13 executor | N14 `table_function_*` | N40d adapter table);
`eval.rs` (N12c | N40b `coerce_untyped_literal_operands` | N40c collation derivation);
`catalog_rel.rs` (N27 | N32 `pg_constraint` | N40b `pg_enum` | N40c `pg_collation`).
N31a owns `partition.rs` ordering + attach column check; N32 owns `inheritance.rs`.

### Batch 3 — Planner Phase 3 (cost-based core) + remaining DDL/DML (≈ −18,000)

**P4** planner core (certify alone); N21 datetime/geometry/range function fidelity; N29a
cumulative statistics system; N29b `pg_locks`/`pg_prepared_statements`/`pg_prepared_xacts`/
`pg_database` rows; N31b partitioned foreign keys + FK misc; N31c cross-partition
UPDATE/MERGE/partitioned DML; N33a partitioned index tree + index DDL/catalog fidelity;
N34a operators (shells, ALTER OPERATOR [FAMILY], PG operator lexing); N34b object
addressing/dependency graph/role-owned objects/DROP notices; N35a sequence DDL + column
DEFAULT expressions + drop-owned sequences; N36a cursors/portals; N36b transaction
characteristics + implicit blocks; N39a COPY; N39b DML grammar/semantics; N40a regex
engine port; N42 psql describe support.

Overlaps: `exec.rs` (P4 removes `try_scan_with_local_index` only | N31b FK regions +
`partition_definition` FK clones | N31c `execute_timestamp_update`/MERGE/INSERT routing |
N33a partition-index regions 25438-25529, `pg_inherits_rows`, `pg_attribute_rows`,
DropIndex, REINDEX | N34b drop notices 9526-9600 | N35a `column_from_ast`,
`drop_table_ops` sequences | N39b `execute_write` INSERT/UPDATE/MERGE arms — N31c owns
partitioned routing, N39b owns grammar/RETURNING/ON CONFLICT | N29b `pg_database` rows);
`session.rs` (N36a cursors | N36b `simple_query`/txn | N39a `run_copy_in` | N42
describe/`\gdesc` | N29b prepared statements); `parser.rs` (N33a `alter_index`/`drop_index`
| N34a ALTER OPERATOR | N34b DROP OWNED/REASSIGN | N35a sequences | N36b SET TRANSACTION
| N39b DML productions); `lexer.rs` (N34a only); `fk.rs` (N31b only); `catalog_fn.rs`
(N33a IndexDef | N34b object address | N42 `pg_get_function_*`); `trigger.rs` (N39a).

### Batch 4 — Planner Phases 4/5 + views/rules/privileges (≈ −16,000)

**P5** upper relations + sort fidelity (certify alone); **P6** partition-aware planning;
N28 rule system; N30b view storage/deparse/updatable-view semantics; N33b storage misc
(reloptions, tablespace semantics, compression, tablesample, PRNG, hash functions,
amutils); N34d ALTER TABLE subcommand completeness + `alter_generic`; N37a privilege
model; N37c misc admin functions/sysviews/routine namespaces; N39c constraint/trigger
fidelity.

Overlaps: `exec.rs` (P6 `partitioned_scan`/`inherited_scan` re-home | N28
`execute_write_parts` rule hooks | N30b CreateView 1095/2152 + `build_base_table` view
expansion | N34d ALTER TABLE region 28900-30300 | N37a grant arms | N39c
`enforce_*`/`reject_temporal`/DROP COLUMN dependency); `session.rs` (N30b PREPARE on views
| N37a SET ROLE | N28 dispatch); `parser.rs` (N28 RULE | N34d rest of `alter_table_action`
+ alter_generic | N37a GRANT/ROLE | N39c EXCLUDE/REPLICA IDENTITY | N33b COMPRESSION);
`fk.rs` (N39c temporal, after N31b); `viewdef.rs` (N30b owns; N28 adds `pg_get_ruledef`
in `catalog_fn.rs`); `rls.rs` (N30b barrier ordering).

### Batch 5 — Planner Phase 6 (parallel) + tail (≈ −4,000)

**P7** parallel planning + **P7x** real worker execution (certify alone); N35b identity +
generated columns completeness (after N34d); N11 LATERAL binder only if P0a slipped
(otherwise absorbed by `plan/bind.rs`); N29c pg_catalog view definitions bootstrapped
from `system_views.sql` (after N30b); N36c MVCC system columns + heap order after UPDATE
(certify alone).

### Batch 6 — hard-floor items (each certified alone; each droppable by explicit decision)

**P8** GiST/SP-GiST/BRIN/hash/GIN-generic index access methods with real scans (~1,400
lines: `create_index_spgist` 620, `gist` 137, `brin*` 334, `gin` 84, `box` 116,
`polygon` 157, `tsearch` 80, `hash_index` 8, `spgist` 12); N21x PG julian-day datetime
arithmetic (247); N40x SERIALIZABLE (SSI, ~60); N33x dropped-column placeholders with
stable attnums (30 + downstream `\d`/attnum lines).

### Batch 7 — closeout

Empty and delete `pg-regress-baseline.json` per the original plan's Task 7 once 231/231
holds serially; then re-run the parallel schedule (`gres both`) and fix parallel-only
defects; retire `crates/gres-conformance` corpus headline duplicates; write the dated
evidence document (tag, archive hash, commands, three clean run ids).

## Planner / EXPLAIN / statistics / index programme (P-series)

Architecture (from the executor study): new modules under `crates/pgexec/src/plan/` —
`query.rs` (Query IR: `RangeTblEntry`, `TargetEntry`, `Var{rti,attno,ty}`,
`RestrictInfo{clause,is_pushed_down,security_level,leakproof,required_relids}`),
`bind.rs`, `rewrite.rs`, `stats.rs`, `selfuncs.rs`, `cost.rs`, `paths.rs`
(`RelOptInfo`, `Path`, `add_path`, pathkeys, equivalence classes), `indexpath.rs`,
`joinpath.rs`, `grouping.rs`, `partition.rs`, `parallel.rs`, `createplan.rs`,
`exec/` (Volcano `PlanState` nodes with per-node counters), `explain.rs`, `deparse.rs`,
`dist.rs` (from `plan_dist.rs`). Not a new crate at first (needs `pub(crate)` `Scope`,
`eval`, RLS types). Reuse: `Scope`/`ColumnBinding`, `BoundExpr` + `eval::eval` +
`infer_type` (add a cached type), subquery folding/`LateralBinder`/scalar lookups (→
InitPlan/SubPlan), `join.rs JoinIndex` (Hash node body), `agg.rs Acc/AggSpec`,
`grouping.rs`, `window.rs execute`, `setops`/`values`/`srf`/`jsontable`/`cte`,
`scan_stored_relation`/`inherited_scan`/`partitioned_scan`/`RawScan`/`apply_row_security`/
`ReadPermit` (sole way to build a scan leaf), the `explain.rs` deparser (fed typed bound
exprs), the JSON/YAML/XML envelopes. Invariants preserved by construction: RLS
default-deny via the single `RawScan` exit; leakproof pushdown → `security_level` on
`RestrictInfo`; `ReadPermit::acquire` before any read; statement-level snapshot/read_ts/GC
pin; `check_query_canceled` at node boundaries; sharded leaves stay `ScanRequest`s and
`plan/dist.rs` paths are preferred until costed.

Dead after P0a (delete, no shims): `exec.rs` ~13300-19330 and 24100-24900 (build_from,
append_from_item, push_local_where, lateral_join, LateralBinder family,
resolve_select_subqueries, try_execute_* pushdowns, select_to_relation_with_ctes,
project_rows_ordered, key_source_rows, distinct_on_plan, apply_row_window, the describe
walk `build_from_schema_*`, `query.rs describe_query_expr*`), `explain.rs plan_*`.

- **P0h** header commit (S): freeze the Plan/PlanState/PlannedStmt/RTE/RestrictInfo types
  and the Executor trait so P0a, P0b, P2, P3 can start in parallel.
- **P0a** Query IR + bind + node-tree executor (XXL; certify alone at each milestone).
  Bind AST → Query; single-relation plan tree executed for real (Seq Scan → Filter →
  Agg|Sort|Unique|Limit|ProjectSet|WindowAgg|Result; Values/Function/Subquery/CTE/Named
  Tuplestore/Table Function scans; nested loops in FROM order for joins as today);
  per-node ntuples/nloops/rows_removed counters; InitPlan/SubPlan from subquery folding;
  describe walk deleted; old read path deleted at the end. Files: NEW `plan/{mod,query,
  bind,rewrite(skeleton),createplan}.rs`, `plan/exec/*`, `exec.rs` read path only
  (`execute_read` 23750, `execute_read_locking` 23808), `query.rs`, `session.rs`
  `run_select_traced` (7902) + `explain` (5176). `join.rs`/`agg.rs`/`grouping.rs`/
  `window.rs`/`setops.rs`/`values.rs`/`srf.rs`/`cte.rs`/`subquery.rs` are CALLED as node
  bodies, not moved (so N16-N19 run in parallel). Exit: zero regressions on the exact
  files; `cargo nextest -p crabka-pgexec` green with the read path served only by
  `plan/exec`.
- **P0b** EXPLAIN renderer + typed deparser + `ExplainOptions` (L, ~3,000 lines: VERBOSE
  `Output:` ~1,100 across join/subselect/with/returning/rangetypes/sqljson/tsrf/
  fast_default; renderer nodes ~700; formats/options ~500 — `parser.rs` 6059-6095 drops
  buffers/wal/timing/summary/settings/generic_plan/memory/serialize; EXPLAIN ANALYZE
  per-node actuals with two-decimal rows, `(never executed)`, `Rows Removed by Filter`;
  EXPLAIN EXECUTE/DECLARE/CTAS/CREATE MATERIALIZED VIEW dispatch; typed constants
  `'42'::bigint`, `'(0,1)'::tid`; RLS quals deparsed in PG order — restrictive quals first;
  full JSON/YAML/XML key sets with zero costs). Files: `explain.rs` → `plan/explain.rs` +
  `plan/deparse.rs` (keep `deparse_with`, `plan_sort_key`, parenthesisation rules),
  `pgparser ast.rs ExplainOptions` (1805) + `parser.rs explain`, `session.rs explain`.
- **P1** rule-based transforms (XL, ~2,500): pull-up, qual distribution, AND/OR
  flattening, IN → `= ANY`, `X = X` → `IS NOT NULL`, NullTest reduction on NOT NULL
  columns, constant-false → Result/`One-Time Filter: false` (executor gates one-time
  quals once — `subselect` tattle NOTICE counts), `remove_useless_joins`, self-join
  elimination, sublink → semi/anti join, single-row VALUES → Result, non-materialized CTE
  inlining, immutable folding, alias numbering (`_1.._n`, `"*VALUES*_1"`,
  `unnamed_subquery`), Var qualification iff >1 RTE, join-type suffixes. Files:
  `plan/rewrite.rs`, `plan/bind.rs`, `plan/deparse.rs`. Exit: `predicate.out` exact; every
  `join.out` EXPLAIN with no Hash/Merge/Index/Bitmap/Materialize/Memoize node matches.
- **P2** statistics (XL-XXL, ~2,000 direct + prerequisite for P4): ANALYZE computes
  `compute_scalar_stats` (nullfrac, width, ndistinct, MCV, histogram, correlation —
  deterministic for ≤30k rows given PG's sampling), `pg_statistic`, `pg_stats`,
  `pg_statistic_ext[_data]`, `pg_stats_ext`, extended statistics (ndistinct/dependencies/
  mcv/expressions; today 143 refusals), `pg_restore_*`/`pg_clear_*` import functions with
  WARNINGs (a notice sink on `EvalCtx`), relpages/relallvisible emulation (heap-page
  simulator + `estimate_rel_size`), `selfuncs` port (eqsel/neqsel/scalarltsel/gtsel,
  eqjoinsel, nulltestsel, booltestsel, patternsel with prefix extraction, arraycontsel,
  rangesel, tsmatchsel, networksel, `estimate_num_groups`, `estimate_hash_bucket_stats`).
  Files: NEW `plan/stats.rs`, `plan/selfuncs.rs`, `relstats.rs`, `catalog_rel.rs`, NEW
  `stats_fn.rs`, `session.rs run_maintenance` (~5299), `exec.rs` CREATE STATISTICS arm +
  ALTER COLUMN SET STATISTICS, `parser.rs` CREATE STATISTICS. Exit: `stats_ext` estimated
  column exact for every MCV/ndistinct/dependencies query; `stats_import` exact;
  `reltuples, relpages` for tenk1/onek after test_setup's VACUUM ANALYZE equal PG's.
- **P3** index storage (L-XL, ~700 direct; certify alone — key format rebuild):
  memcomparable secondary-index keys (`crates/pgkv/src/keyenc.rs`; NULLS FIRST/LAST,
  DESC by inversion; C/POSIX first, collation sort keys later with N40c), full index
  catalog (per-key direction/nulls/opclass/collation, predicate, INCLUDE, expressions,
  unique expression indexes, hash-method entries, NULLS NOT DISTINCT, opclass params),
  `pg_index.indkey/indoption/indexprs/indpred/indnkeyatts`, `pg_get_indexdef` INCLUDE/
  WHERE, index rebuild via `local_index_backfill_ops`, ordered index cursor in
  `scanner.rs` (local only; `IndexPlacement::Global` later). Files: `pgkv key.rs 232-257`,
  `keyenc.rs`, `rowenc.rs`, `pgcatalog lib.rs Index/NewIndex` (397) + `serde.rs`,
  `exec.rs index_entries` 11240 / `local_index_backfill_ops` 10060 / CREATE INDEX
  validation 1419-2686, `scanner.rs`, `catalog_rel.rs`, `catalog_fn.rs IndexDef`. Exit:
  zero "not supported" CREATE INDEX errors in the whole schedule; ORDER BY over an
  indexed column served by the index cursor in a unit test.
- **P4** cost-based planner core (XXL, ~11,700; certify alone): `cost.rs` (all
  `cost_*`, PG 18 `disabled_nodes`, GUCs), `paths.rs`, `indexpath.rs` (clause matching
  via `builtin_opclasses.rs`/`builtin_opfamilies.rs`, Index Cond vs Filter, bitmap
  AND/OR, index-only eligibility with a visibility-map analogue → `Heap Fetches`),
  `joinpath.rs` (`join_search_one_level` DP with `join_collapse_limit`, nestloop/hash/
  merge, parameterised inner paths, Materialize/Memoize placement, semi/anti/unique),
  Tid/Tid Range paths, hash join with LIFO bucket order, HashAggregate simplehash
  iteration order + PG hash functions (`hashint4`/`hashtext`/`hash_array`), the
  join-side rule that reproduces `geometry`'s order, `planagg` MIN/MAX → InitPlan + Index
  Only Scan Backward, spilling Sort/Hash/Materialize under `work_mem` (decision 2b),
  `plan/dist.rs` for sharded relations. Files: `plan/{cost,paths,indexpath,joinpath,
  pathkeys,equivclass}.rs`, `plan/exec/{hashjoin,mergejoin,nestloop,material,memoize,
  indexscan,indexonlyscan,bitmap,tidscan,sort}.rs`, `join.rs` bodies, `scanner.rs`,
  `exec.rs try_scan_with_local_index` removed. Exit: EXPLAIN (COSTS OFF) of every
  statement in join/subselect/equivclass/select/limit/tidscan/tidrangescan/aggregates
  (planagg)/create_index (btree) matches upstream node choice on the schedule's data
  under its `enable_*` settings; `join`/`geometry` cross-join order matches;
  `union`/`select_distinct` hashed output order matches.
- **P5** upper relations + sort fidelity (L-XL, ~3,200; certify alone): `pg_qsort`
  (`sort_template.h`) port for every sort site incl. top-N bounded heapsort, one global
  sort per window with `select_active_windows` ordering and `optimize_window_clauses`,
  Run Condition, Incremental Sort (Presorted Key, group counts), Hash/Group/Mixed
  aggregate strategy + Partial/Finalize, GroupAggregate sorted output, DISTINCT
  Unique-vs-HashAggregate, SetOp/HashSetOp, LockRows, ordered-set placement. Files:
  `plan/grouping.rs`, `plan/exec/{agg,windowagg,incsort,unique,setop,sort}.rs`,
  `window.rs execute` (513-555, 1038-1052), `agg.rs` sort sites, `scanner.rs` top-K.
  Exit: `window.out` exact except non-planner roots; groupingsets/incremental_sort
  EXPLAIN blocks match; DISTINCT ON without ORDER BY keeps PG's tie choice.
- **P6** partition-aware planning (XXL, ~8,200; needs N31a, N32, N33a, P4): Append/Merge
  Append over leaves in PartitionDesc order with `_n` aliases (surviving children only),
  `partprune.c` port (static + run-time + InitPlan params, `Subplans Removed`, `(never
  executed)`), `constraint_exclusion` for inheritance CHECKs, partitionwise join/agg,
  EXPLAIN EXECUTE generic plans under `plan_cache_mode`, Update/Delete child lines,
  `inheritance.rs children_of` in OID order (today KV length-first order). Files:
  `plan/partition.rs`, `partition.rs` (479-551 bound order), `inheritance.rs`,
  `plan/exec/append.rs`, `session.rs` prepared statements. Exit: `partition_prune.out`
  and `partition_join.out` exact; `partition_aggregate` exact except parallel blocks.
- **P7 / P7x** parallel (planning M; execution XL): parallel path generation and cost
  (`parallel_setup_cost`, `parallel_tuple_cost`, `min_parallel_*_scan_size`,
  `max_parallel_workers_per_gather`, `parallel_workers` reloption stored via ALTER TABLE
  SET), Gather/Gather Merge/Parallel scans/Parallel Append/Parallel Hash/Partial+Finalize;
  real worker tasks over disjoint scan ranges with per-worker instrumentation (`Worker N:
  Sort Method`, `Workers Launched`). Files: `plan/parallel.rs`, `plan/exec/gather.rs`,
  `cost.rs`, `scanner.rs` range splitting. Exit: `select_parallel.out`,
  `write_parallel.out` exact; `join_hash` exact (needs `ExecChooseHashTableSize` batch
  counts from `work_mem`/`hash_mem_multiplier`, part of P4).
- **P8** index access methods (XXL, optional last): GiST/SP-GiST/BRIN/hash/GIN-generic
  entries and scans with opclass support, KNN Order By, `brin_summarize_*`,
  `gin_clean_pending_list`/`gin_fuzzy_search_limit`. Files: NEW `index_am/{gist,spgist,
  brin,gin}.rs`, `exec.rs index_entries`, `plan/indexpath.rs`, `geometry.rs`.

Parallel lanes inside the planner programme (no overlapping files): A = IR + bind +
createplan + exec skeleton (only lane that edits `exec.rs`); B = stats + selfuncs + cost +
paths + joinpath + indexpath (pure functions, unit-testable); C = index storage (`pgkv`,
`pgcatalog Index`, write-path `index_entries`/backfill — coordinate the `exec.rs` touch
with A); D = EXPLAIN renderer + deparser + parser options; E = partition planning after A;
F = parallel after A, B.

## Workstream catalogue (N-series)

Each entry: size, lines it flips (with listed dependencies), key roots, file set. Full
root records (evidence, oracle facts, fix symbols) are in the preserved triage ledgers;
briefs must be written from those, not from this summary.

- **N07 parser lane 1** (L, ~1,000): FROM/select-list/expression grammar — `x.*` in
  ROW()/args/casts/VALUES, `((subq)) alias`, JOIN USING alias, `(a JOIN b) AS x(cols)`,
  parenthesised set-op operands, empty select list, CTAS AS EXECUTE, WHERE CURRENT OF
  grammar, DROP INDEX lists, `$1.f1`, `name mode type` routine params, BEGIN WORK. Files:
  `parser.rs` (`join_onto` ~12990, `table_factor` 13105, `parse_from` 12960, primary Star
  ~1233/11330/14993, target list ~11897, set-op operands 12514-12660, `create_table_as`
  7001, `begin` 6628), `ast.rs`, `exec.rs` join alias binding + CTAS EXECUTE arm.
- **N09 lexer + syntax-error parity + positions** (M-L, ~800): PG "syntax error at or
  near" wording (`lexer.rs:699 at_or_near`, `parser.rs:13684 syntax_error_at_token`
  adoption), string continuation, LINE/caret from offsets, DETAIL/HINT families,
  `column t1.x does not exist` + HINT, ambiguous table reference. Files: `pgparser
  lexer.rs/error.rs/parser.rs`, `pgexec error.rs`, `session.rs` error emission, `scope.rs`.
- **N10 result column naming** (S-M, ~1,100): FigureColname arms (scalar subquery →
  inner name, ARRAY → `array`, CASE → `case`, ROW → `row`), LATERAL alias pinned before
  substitution (830 of the lines), function-scan naming. Files: `exec.rs named_expr_inner`
  24919 / `BindPass::set_expr` ~14785 / `derived_name` 24850, `routine.rs
  table_function_columns` 2951, `viewdef.rs` 661.
- **N11 LATERAL / correlated binder** (L, ~670) — absorbed by P0a `plan/bind.rs`; run
  standalone only if P0a slips.
- **N12a arrays of any element type + domain over composite** (XL, ~1,850; certify
  alone): `pgtypes datum.rs ElemType` (261) / `from_column_type` (355-446) / `array_of`
  (1118) closed enum → open (record/composite/enum/point/user/domain elements),
  `cast.rs`, `pgcatalog serde.rs` (SCHEMA_VERSION), `usertype.rs create_domain` (98-106),
  `parser.rs` 776 allow-list, `partition/hash.rs hash_array_extended`. Blocks `aggtype[]`,
  `ARRAY[ROW(..)]`, `array_agg(record)`, SEARCH/CYCLE.
- **N12b relation rowtypes + whole-row references** (L, ~1,000): register a composite
  type per relation (`pg_type.typrelid`), `$1.name` postquel, `f.last`↔`last(f)`,
  whole-row args. Files: `usertype.rs`, `routine.rs resolve_type` (468), `scope.rs` 1299,
  `eval.rs whole_row_reference` (55), `exec.rs` 14892, `rowexpr.rs`.
- **N12c row comparison + row-valued subqueries** (M, ~260): ROW `=`/`<>` with NULLs,
  ANY/ALL row subqueries, dissimilar-type messages, IS NULL field-wise. Files:
  `subquery.rs` 640/660, `eval.rs`, `rowexpr.rs`, `pgtypes ops.rs:1039`.
- **N13 SQL-language function executor** (XL, ~1,100): value-bound execution instead of
  inlining only (volatile args once), DML RETURNING final statement, SETOF/record in
  select list, `RETURNS <rowtype>`, CREATE OR REPLACE checks + HINTs, CONTEXT/QUERY lines
  via pgwire `DiagnosticFields`, procedure CALL args, `pg_get_functiondef`. Files:
  `routine.rs` (1825/2211/2272/2347/2377/875/677/1535/468/2628/2841), `session.rs
  drive_scalar_worker` (8027), `plpgsql.rs` seam, `pgwire error.rs`, `catalog_fn.rs`.
- **N14 FunctionScan relation builder** (L, ~1,050): one builder for SQL + PL/pgSQL
  table functions in FROM (subquery/view/join/ROWS FROM/ORDINALITY/coldeflist, OUT-param
  setof record, RETURNS TABLE names, whole-row, scalar builtin in FROM as one-row scan).
  Files: `exec.rs build_table_expr` Function arms 17745-17794 (or `plan/bind`),
  `routine.rs` 2817-3052, `srf.rs from_item` 1185 / `plan` 456 / `user_function_relation`
  1357 / `undefined_function` 2295, `plpgsql.rs` 402-421/1756.
- **N15 named / default / VARIADIC arguments** (M, ~1,500): general `name => value`
  resolution (today a table knowing only `make_interval`), `VARIADIC array[...]`,
  `builtin_procs_*.tsv.zst` regenerated with `proargnames`/`pronargdefaults`/
  `proargdefaults`, one resolution point before the `eval.rs` 369-426 guard chain,
  `viewdef.rs` deparse. Files: `parser.rs func_call` 1901-1950 / `positional_from_named`
  3012, `ast.rs FuncArgs`, `routine.rs resolve_call/bound_args`, `json_fn.rs` 874-899,
  `srf.rs` 321/755, `format_fn.rs`.
- **N16 aggregate completeness** (L, ~1,300): `WITHIN GROUP` ordered-set/hypothetical
  aggregates (parser + `agg.rs` + `useragg.rs`), built-in support functions callable
  (`int4pl`, `int8inc`, `float8_accum`, `numeric_avg_accum`, `ordered_set_transition`, …
  — `useragg.rs lookup` uses user routines only), user-aggregate definition fidelity +
  moving aggregates, overload coverage (`any_value`, `string_agg(bytea)`, `bit_*`,
  `avg(interval)`), outer-level aggregates, functional dependency in GROUP BY, grouping-
  sets semantics, COMMENT ON AGGREGATE. After N15 (shares `func_call`).
- **N17 window frames/specs** (M, ~470): infinite-interval RANGE offsets, `timetz`
  in_range, `unbounded` name precedence, named WINDOW over GROUP BY, moving-aggregate
  execution. Files: `window.rs resolved_frame` (1354), `parser.rs frame_bound` (2138),
  `useragg.rs`.
- **N18 recursive CTE shape + SEARCH/CYCLE + DML-CTE order + CREATE RECURSIVE VIEW** (L,
  ~1,100; needs N12a): `cte.rs` 294-315/329-340/380/533-541/164-169, `exec.rs
  execute_write_parts` (~4362) forward references, `parser.rs create_view` 9259 +
  `parse_with_clause`, `viewdef.rs` 194-230.
- **N19 SRF ProjectSet semantics** (L, ~510): nested SRF args, SRF in GROUP BY/PARTITION
  BY, DISTINCT ON, error contexts, `split_pathtarget_at_srfs` placement. Files: `srf.rs`
  1467/1587/1709/1696.
- **N20 datetime typmod precision** (L, ~1,980; certify alone — SCHEMA_VERSION):
  `ColumnType::{Time,Timetz,Timestamp,Timestamptz,Interval}` carry typmod; `cast_in`/
  `cast_assign_in` round half away from zero and truncate interval field masks;
  `parse_type_name` 651-663 + `interval_literal` 1852-1880; `exec.rs coerce` 12800-12813 +
  `catalog_typmod` 21529; `pgcatalog serde.rs` 355-376/500-524; `builtin_format_type` 2745;
  `viewdef.rs` cast deparse.
- **N21 datetime / geometry / range function fidelity** (M-L, ~880): `pg_sleep`,
  `timestamptz(date,time[tz])`, `date_add/subtract`, `interval_hash`, `avg(interval)`,
  OVERLAPS, multirange literal parser, `to_char` RM/IYYY-BC, `to_timestamp` DETAIL/HINT,
  `generate_series` over timestamps (lazy under LIMIT, infinite step, 4-arg tz), tz
  abbreviations (LMT/MMT/MSK, `to_char TZ`, `SHOW TIME ZONE` posix), `box_in` adjacent
  points, unknown-literal args, cross-type compare, epoch UTC, BETWEEN SYMMETRIC,
  `date_bin` overflow, `'now'` at transaction start (`datetime.rs clock_now` 972).
- **N21x datetime calendar range** (XXL, 247; batch 6, certify alone): PG julian-day
  arithmetic in `pgtypes datetime.rs` replacing jiff civil arithmetic (jiff stays for tz).
- **N22 jsonpath grammar/printer + datetime methods + evaluator** (L, ~2,600): numeric
  literal lexing (`lex_number` 522 → `numeric::parse_finite`), paren canonicalisation,
  unary folding, `$"a"` quoting, `last` scoping, escapes/surrogates + LINE cursor,
  `like_regex` flags, method args, `.datetime(template)` via `datetime.rs` template
  engine std mode (new error texts + field mask), `.decimal`, `_tz` behaviour, `use_tz`
  from `ctx.time_zone`, `TIME(10) precision reduced` warning sink. Files: `jsonpath.rs`
  (401/522/555/610/174/1007-1035/1995-2040/1784-1807/2118-2246), `pgtypes datetime.rs`
  4613-5490, `json_fn.rs` 874-945/1714, `jsontable.rs` 346/385/532.
- **N23 JSON/JSONB function fidelity** (L, ~1,900): SQL/JSON constructors + aggregates
  (NULL ON NULL, WITH UNIQUE KEYS, JSON_ARRAY(subquery)), JSON_VALUE/QUERY returning
  coercion + analysis checks, constraint deparse, index immutability, JSON_TABLE deparse/
  cursor/user-type oid in view, jsonb scalar casts, subscripting polish,
  `populate_record_valid`, `#-`/set-path messages, `pg_column_size`, `json_object` shape,
  `array_to_json` multidim, stack HINT. Files: `json_fn.rs`, `jsontable.rs`, `sqljson*.rs`,
  `viewdef.rs`, `pgtypes jsonb.rs`, `parser.rs` JSON_* clauses, lexer `#-`.
- **N24 XML** (XL, ~1,200 + xmlmap.out ~1,200 per decision 3): xpath (425), xmltable
  (349), xmlelement family (253), SET XML OPTION, `table_to_xml` family, refcursor, second
  libxml error line. Files: `pgtypes xml.rs`, `xml_fn.rs`, `parser.rs` XMLTABLE/PASSING/
  COLUMNS, `session.rs`.
- **N25 text search subsystem** (XXL, ~2,850): default parser port (`ts_parse`/
  `ts_token_type`/`ts_debug`), dictionaries (ispell/hunspell/synonym/thesaurus with the
  sample files from `src/backend/tsearch`), CREATE/ALTER TEXT SEARCH DICTIONARY/
  CONFIGURATION/PARSER/TEMPLATE with mappings (parser today discards the clause),
  `dictinitoption` deparse, headline algorithm, `ts_rank`/`ts_rank_cd` exact numbers,
  `websearch_to_tsquery`, `ts_stat`/`ts_rewrite`/`unnest(tsvector)`/`tsvector_to_array`,
  tsvector/tsquery I/O (backslashes), unknown-literal args, GiST opclass options. Files:
  `pgtypes text_search.rs` (rewrite), `text_search_fn.rs`, `text_search_catalog.rs`,
  `parser.rs` 6534/6579/439, `exec.rs text_search_catalog_rows` 21710.
- **N26 FDW DDL + catalogs** (L-XL, ~1,550): full grammar (ALTER FDW/FOREIGN TABLE/
  SERVER/USER MAPPING, IF NOT EXISTS, TYPE/VERSION, constraints/column OPTIONS/PARTITION
  OF/INHERITS, GRANT ON FOREIGN …, COMMENT ON FOREIGN …), catalog records with owner/
  handler/validator/type/version/acl/oid + per-column options (SCHEMA_VERSION),
  `pg_foreign_*`, `pg_user_mapping[s]`, information_schema foreign_*/user_mapping*,
  `has_server_privilege`, `pg_options_to_table`, dependency tracking, PG messages. Files:
  `parser.rs` 3696-3789/13843-14073/4568/4615/8592, `ast.rs` 955-1032, `pgcatalog lib.rs`
  610-633/268 + `serde.rs` 1973/2313/2342, `catalog_rel.rs`, `catalog_fn.rs`, `exec.rs`
  1826-1952/31868.
- **N27 logical replication DDL + catalogs** (XL, ~1,550; DDL + catalog + psql only, no
  replication): CREATE/ALTER/DROP PUBLICATION (FOR TABLE/TABLES IN SCHEMA/ALL TABLES,
  publish options, row filters, column lists, `pubgencols`), CREATE/ALTER/DROP
  SUBSCRIPTION with `connect = false` and every option, `pg_publication*`,
  `pg_publication_tables` + `pg_get_publication_tables()`, `pg_subscription[_rel]`,
  `pg_stat_subscription_stats`, predefined roles (`pg_create_subscription`), DML-time
  replica-identity checks, DROP COLUMN dependencies, `pg_relation_is_publishable`. Files:
  `parser.rs` 5488 + ALTER dispatch, `ast.rs NON_GOAL_REFUSALS`, NEW `publication.rs`,
  `pgcatalog`, `catalog_rel.rs`, `catalog_fn.rs` 732, `exec.rs` 28370/4361, matrix rows
  145/201/251/153/208/259.
- **N28 rule system** (XXL, ~1,450): CREATE [OR REPLACE] RULE / DROP / ALTER / COMMENT ON
  RULE / ALTER TABLE ENABLE|DISABLE RULE, `pg_rewrite` + `pg_rules` + `pg_get_ruledef`,
  NEW `rewrite_rules.rs` (DO INSTEAD/ALSO, ON SELECT view rules, NEW/OLD, conditional
  rules, RETURNING), `execute_write_parts` hooks. Matrix rows 148/203/254.
- **N29a cumulative statistics system** (XL, ~1,300): NEW `pgstat.rs` (per-relation/
  function/io/database counters incl. `seq_scan`/`idx_scan` from scan leaves,
  `n_tup_ins/upd/del`, snapshot semantics, `pg_stat_force_next_flush`, `pg_stat_reset*`,
  `pg_stat_have_stats`, `pg_stat_get_*`), `pg_stat_*`/`pg_statio_*` views, `pg_stat_io`,
  `track_functions/track_counts/track_io_timing/stats_fetch_consistency`.
- **N29b `pg_locks` / `pg_prepared_statements` / `pg_prepared_xacts` / `pg_database`
  rows** (M, ~320): render `lockmgr.rs` holds (advisory + tuple/relation locks),
  prepared-statement catalog text, `postgres` row.
- **N29c pg_catalog view definitions** (XXL, 1,437; batch 5, decision 5): bootstrap
  `system_views.sql` definitions as stored views over real catalogs, `pg_get_viewdef`
  prints them; retire the `exec.rs` virtual-table registries (19853/22320/22681/20573)
  where a real view can serve. After N30b.
- **N30a `interpt_pp` regression C adapter** (S, 902): `routine.rs RegressionCAdapter`
  (1694-1746, 2041-2063) result-type parameter, `pgtypes geometry.rs
  Lseg::intersection_point`. Un-cascades `select_views`.
- **N30b view storage, deparse, updatable views** (XL, ~1,670): store the analysed query
  (bound view storage, not text) so `FROM schema.view` and rename survive; `pg_get_viewdef`
  layout fidelity (`viewdef.rs write_query` 141/270-292, `write_select` 430-445,
  `expression_text` 243, 1093), view column defaults, view-write rewrite gaps, WITH CHECK
  OPTION cascading, security-barrier qual ordering (NOTICE order is asserted; UPDATE
  through a barrier view must not evaluate the leaky qual on hidden rows), matview refresh
  semantics, view reloptions, temp-view notice, MERGE/row locking through views, PREPARE
  DML on views, ALTER VIEW RENAME COLUMN, VALUES typmod, unknown pseudo-type. Files:
  `viewdef.rs`, `viewwrite.rs`, `exec.rs` CreateView 1095/2152 + `build_base_table`
  17612-17660, `pgcatalog View` + serde, `session.rs` 25126, `rls.rs`.
- **N31a expression partition keys + partition catalog misc** (L, ~1,700 direct; a
  further ~1,700 planner lines behind it for P6): `PartitionKeyElem` keeps Expr/collation/
  opclass (`parser.rs opt_partition_by` 7436-7488, `ast.rs` 2394); `partition.rs` scheme
  (SCHEME_VERSION bump; `key_ordinals` 691, `key_values` 708, `key_description` 769,
  `route` 873, `satisfies` 895, `key_columns` 1114, `partitions_of/leaves_of/descendants`
  479-551 in bound order), `partition/hash.rs` opclass support fn 2 + `hash_array_extended`,
  `exec.rs` 25588/25617/25660/17478/5067/20315/~29912/~30178/29002 column-set check,
  `pg_get_partition_constraintdef`, `pg_partition_tree/root/ancestors`,
  `satisfies_hash_partition` (VARIADIC), MINVALUE/MAXVALUE ordering, `create_table`'s
  atomic DROP failure that leaks `parted`/`list_parted2`/`range_parted2`/`part1..4` into
  `alter_table` (~150), `triggers` (51), `merge`, `inherit`.
- **N31b partitioned foreign keys + FK misc** (L, ~1,050): `reject_partitioned_foreign_key`
  (exec.rs 26475) removed; `fk.rs resolve_foreign_key` 239 / `run_child_check` 1815 /
  `run_parent_check` 1889 / `find_referencing_rows` 2000 see rows in partitions;
  `FkParts::resolve`; FK clones on ATTACH/CREATE PARTITION (`partition_definition` 25300,
  `attach_partition_ops` 29002 — after N31a); `conparentid`/`conenforced`; NOT ENFORCED;
  self-referential FK in CREATE TABLE; TRUNCATE FK on partitioned; violation names the
  root table; `types_are_comparable` 557. `pgcatalog ForeignKey` parent link
  (FOREIGN_KEY_VERSION).
- **N31c cross-partition UPDATE / MERGE / partitioned DML** (L, ~950): row movement
  (`execute_timestamp_update` 12085, `check_partition_constraint` 5047, `timestamp_txn.rs`
  rowid contract), MERGE into partitioned tables, INSERT … SELECT routing 17486, generated
  columns/identity on partitions, ON CONFLICT on partitioned tables, partition trigger
  clone rules (`trigger.rs` 2050/598/2150).
- **N32 inheritance + NOT NULL / CHECK constraint catalog** (XL, ~2,000): NOT NULL as
  first-class constraint records (PG18 `contype n`: `conislocal`, `coninhcount`,
  `connoinherit`, `conenforced`, `convalidated`, names not derived from the column),
  CHECK inheritance/no_inherit/normalised expr, `INHERITS` column merge (NOTICE counts,
  type checks, `attinhcount`), DROP TABLE CASCADE of inheritance children (children
  survive today → 42P07 later; ~330 in `inherit`), ALTER TABLE INHERIT/NO INHERIT (+
  rowsecurity 397 cascade), rename inherited column, `children_of` OID order,
  `Failing row contains` DETAIL. Files: `pgcatalog lib.rs Column.not_null` (170) →
  `NotNullConstraint`, `CheckConstraint` (254) + serde, `exec.rs` 25217-25290 /
  9304-9600 / ~29092-29489, `inheritance.rs`, `catalog_rel.rs` 2547-2604 + `pg_get_expr`,
  `session.rs inheritance_notices` (~9488), `parser.rs alter_table_action` 3997-4190,
  `viewdeps.rs`, `error.rs`.
- **N33a partitioned index tree + index DDL/catalog fidelity** (XL, ~1,150): index parent
  links (`pgcatalog Index` + `put_index_ops`), `pg_inherits` rows for indexes,
  `pg_attribute` rows for indexes (`\d <index>`; 372 lines, 345 in `tablespace`),
  ALTER INDEX ATTACH PARTITION, `indisvalid`/CIC failure state, REINDEX changing
  `relfilenode` of leaf indexes only, ADD CONSTRAINT USING INDEX, ALTER INDEX ALTER COLUMN
  SET STATISTICS, DROP INDEX CONCURRENTLY + lists, `pg_get_indexdef` ON ONLY + expression
  parenthesisation, index autonaming, unique-index-per-partition enforcement, PK on
  partition column, `indisreplident`. Files: `exec.rs` 25438-25529/29002 (index half)/
  20277/20874/9474/9675, `parser.rs alter_index` 3960 / `drop_index` 11081,
  `catalog_fn.rs IndexDef` (128), `catalog_rel.rs`.
- **N33b storage misc** (L, ~1,300; independent files): reloptions storage
  (`pg_class.reloptions`, ALTER … SET/RESET, validation), tablespace semantics (bulk
  moves, defaults on partitions, `pg_global` refusals, ALL IN TABLESPACE, GRANT ON
  TABLESPACE, `test_io_local` leak from `stats`), column compression (`COMPRESSION pglz`,
  `default_toast_compression` HINT `Available values: pglz.`, `pg_column_compression`,
  `\d+` Compression column, LIKE INCLUDING COMPRESSION), `pg_toast` relations,
  page-based TABLESAMPLE SYSTEM/BERNOULLI with REPEATABLE seeds, PG's xoroshiro PRNG
  (`random`, `setseed`, `random_normal`, `random(min,max)`), hash functions
  (`hashint2/4/8`, `hashtext`, … exact values), `pg_indexam_has_property`/
  `pg_index_column_has_property`/`pg_index_has_property`, `make_tuple_indirect`,
  `pg_relation_size`.
- **N33x dropped-column placeholders** (XL, 30 + downstream; batch 6): keep
  `........pg.dropped.N........` attributes with stable attnums across the catalog and
  the row format (SCHEMA_VERSION).
- **N34a operators** (M-L, ~850): PG operator-token lexing rule (`lexer.rs` fixed
  punctuation table → generic operator characters: `~<~ ~<=~ ~>=~ ~>~ ^@ *= *< |@| <<< >>>
  ===`), shell operators + commutator/negator links, ALTER OPERATOR / OPERATOR FAMILY /
  OPERATOR CLASS options, user-operator resolution before builtin comparison
  (`eval.rs apply_binary`), `useroperator.rs` 373, `pgtypes ops.rs:1039` wording.
- **N34b object addressing, dependency graph, role-owned objects, DROP notices** (L,
  ~1,450): `pg_get_object_address`/`pg_identify_object[_as_address]`/`pg_describe_object`
  (all object classes; `catalog_fn.rs CatalogFunc` 68/118, `srf.rs classify` 300 FROM
  forms), `pg_depend` for all object classes + `pg_shdepend` (`catalog_rel.rs
  pg_depend_rows` 1017), DROP OWNED / REASSIGN OWNED / CREATE GROUP, drop-cascade
  NOTICE/DETAIL family in OID order (all object kinds; `exec.rs` 9526-9600), DROP … IF
  EXISTS notices for every object kind, event trigger `ddl_command_end`/`sql_drop` rows,
  role-owned-object refusals (`DROP ROLE` should fail).
- **N34c type / cast / access method / typed table / LIKE DDL** (L, ~1,300): shell type
  autocreate + base-type attributes (`typinput`/`typoutput`, …), CREATE CAST WITH
  FUNCTION, CREATE ACCESS METHOD TYPE TABLE|INDEX with handler + ALTER TABLE SET ACCESS
  METHOD + `pg_am` fidelity (matrix rows 179/230), CREATE TABLE OF type, LIKE INCLUDING …
  (column order with INHERITS), CREATE TABLE misc (unknown type message, CTAS quirks),
  COMMENT ON DOMAIN/TYPE/CAST/AM, domain validation messages, unknown pseudo-type.
- **N34d ALTER TABLE subcommand completeness + `alter_generic`** (XL, ~1,500): SET
  SCHEMA, SET LOGGED/UNLOGGED, ALTER COLUMN SET STATISTICS/STORAGE/(n_distinct)/
  COMPRESSION, SET ACCESS METHOD, OF/NOT OF, CLUSTER ON, SET WITHOUT OIDS, VALIDATE,
  ALTER CONSTRAINT …, ALTER INDEX ALTER COLUMN 0 SET STATISTICS error; ALTER AGGREGATE/
  COLLATION/CONVERSION/FUNCTION/LANGUAGE/OPERATOR CLASS|FAMILY/STATISTICS/TEXT SEARCH …
  RENAME/OWNER/SET SCHEMA. Files: `parser.rs alter_table_action` remainder, `exec.rs`
  ALTER TABLE executor 28900-30300 (`Action::Unsupported` 28981).
- **N35a sequence DDL + column DEFAULT expressions + drop-owned sequences** (L, ~860):
  sequences stop being `CreateIndex` on a fake relation (parser `create_sequence`,
  `SEQUENCE_RELATION`); catalog `Sequence` with data type/owner/owned-by; ALTER SEQUENCE
  (all forms), `lastval`, `nextval/currval/setval` regclass overloads, `pg_sequences`/
  `pg_sequence`, `smallserial`; DROP TABLE drops owned serial/identity sequences
  (`drop_table_ops`/`drop_table_and_dependents_ops` never emit `drop_sequence_ops` — 141
  lines in `fast_default`); column DEFAULT stored as an expression, not a value
  (`ColumnDefault`, `column_from_ast` 2355, `ensure_default_can_be_persisted` 2794:
  `DEFAULT nextval(...)`, `now()`, temporal and user-function defaults; SCHEMA_VERSION);
  sequence privileges. Sequence describe (`\d` type + Owned by) with N42.
- **N35b identity + generated columns completeness** (L, ~900; after N34d): ALTER COLUMN
  ADD/SET/DROP IDENTITY, OVERRIDING SYSTEM|USER VALUE, ALWAYS enforcement, identity on
  partitions/inheritance, SET LOGGED, information_schema `is_generated`/`is_identity`,
  virtual-generated rules and diagnostics, deparse.
- **N36a cursors and portals** (L, ~650): materialise at first FETCH (errors surface at
  FETCH; volatile functions see later inserts), SCROLL/NO SCROLL, WITH HOLD, `pg_cursors`,
  WHERE CURRENT OF (parser with N07; executor uses the cursor's current rowid), FOR
  UPDATE over joins/temp/search_path relations (`execute_read_locking` 23808/23886 →
  LockRows after P0a), row locking through views. Files: `session.rs declare_cursor`
  4589-4635 / FETCH, `cursor.rs`, `exec.rs`, `catalog_rel.rs`.
- **N36b transaction characteristics + implicit blocks** (M, ~450): SET TRANSACTION
  READ ONLY/READ WRITE/DEFERRABLE, SET SESSION CHARACTERISTICS AS TRANSACTION, AND CHAIN
  copies every characteristic (`set_transaction_tail`, `TxnCtx`), multi-statement simple
  queries as an implicit transaction block (`session.rs simple_query`; also
  `psql_pipeline`'s pipeline rollback — `sync()` only clears portals today), CTAS ON
  COMMIT, PREPARE TRANSACTION refusal text, ON COMMIT DROP with inheritance, temp
  namespace objects (`pg_temp` functions), commit of a failed block keeps DDL.
- **N36c MVCC system columns + heap order after UPDATE** (L, ~150 direct; batch 5,
  certify alone): `xmin/xmax/cmin/cmax` (`scope.rs SYSTEM_COLUMNS`), command-id
  visibility (`combocid`), new tuple version placed at the heap end
  (`apply_locked_row_update` 10878-10943, `execute_timestamp_update` 12085,
  `timestamp_txn.rs` rowid contract).
- **N37a privilege model** (XL, ~1,150): column privileges (`attacl`), ALTER DEFAULT
  PRIVILEGES (`pg_default_acl`), role membership model (WITH ADMIN OPTION GRANTED BY,
  INHERIT/SET options, `pg_auth_members` columns, `pg_has_role`), non-relation object
  privileges (schema/function/type/sequence/database/tablespace/parameter/language/FDW),
  role attributes lifecycle (`create_role`, `password_1`: REPLICATION DETAIL, VALID
  UNTIL, CONNECTION LIMIT, RENAME), view privileges, `zeropriv` ACLs, `SET ROLE` for a
  SET-SESSION-AUTHORIZATION superuser (`crabka_pgcatalog::role_can_set` exempts only
  BOOTSTRAP_ROLE), predefined roles, `has_*_privilege` family. Files: `privilege.rs`
  (542/42/1127), `pgcatalog` roles/ACL, `parser.rs` GRANT/REVOKE/ALTER DEFAULT
  PRIVILEGES/CREATE ROLE options, `exec.rs` grant arms + role DDL, `catalog_fn.rs`,
  `catalog_rel.rs`, `session.rs SET ROLE`.
- **N37b large objects** (L, ~880): NEW `largeobject.rs` (`lo_create/open/read/write/
  lseek64/tell64/truncate/unlink/import/export/get/put/from_bytea`, `pg_largeobject` +
  `pg_largeobject_metadata` with owner/ACL, `lo_compat_privileges`); privileges section
  (427) depends on it. Matrix row 138.
- **N37c misc admin functions, sysviews, routine namespaces** (M-L, ~600):
  `num_nonnulls/num_nulls`, `pg_ls_*`, `pg_current_logfile`, `pg_settings_get_flags`,
  `pg_input_is_valid` family, `pg_get_wal_*`, `gen_random_uuid`, …; sysviews
  (`pg_available_extension_versions`, `pg_timezone_abbrevs`, `pg_backend_memory_contexts`,
  `pg_config`, …); schema-qualified routine names (parser `routine_name` refuses non-
  public qualifiers) and `pg_temp` functions.
- **N38 PL/pgSQL fidelity** (L, ~2,000): NEW/OLD field types kept (bpchar `!= ''`
  misfires; 570 lines cascade in `plpgsql`), expression SQL tail (`if count(*) = 0 from
  …`), CONTEXT lines with statement line numbers (`pgparser plpgsql.rs` AST needs line
  numbers), `%ROWTYPE`, RETURN tail forms, composite field assignment on an unassigned
  record, RETURNS SETOF composite in FROM, RAISE diagnostics, CONTEXT for SQL functions
  called from PL/pgSQL. Files: `plpgsql.rs` 3045/288/385/425/1392/402-421/1756,
  `pgparser plpgsql.rs parse_expr_range` 1245, `eval.rs bpchar_to_text_value` 115,
  `routine.rs` 1936.
- **N39a COPY** (M, ~450): COPY FROM fires PL/pgSQL triggers (`session.rs run_copy_in`
  10527 lacks `with_scalar_runtime(.., Some(request_tx))`), ON_ERROR ignore + LOG_VERBOSITY,
  precheck ordering (cannot-copy-to-view after CopyInResponse; duplicate-column check
  before), `client_encoding LATIN1`, TRUNCATE notices, COPY WHERE.
- **N39b DML grammar and semantics** (L, ~1,150): RETURNING OLD/NEW (bare, in subqueries,
  MERGE RETURNING, partitioned targets), ON CONFLICT target grammar + arbiter inference
  (expression/collation/opclass indexes), INSERT target indirection + subscripts
  (`INSERT INTO arrtest (a[1:5], …)`), multi-column SET incl. row-subselect, MERGE grammar
  + clause scope + USING joined source + correlated target + EXCLUDED typing, INSERT … AS
  alias, correlated subqueries in DML. Files: `parser.rs` (insert target/alias/indirection,
  `conflict_target`, `merge` 6863, RETURNING), `exec.rs execute_write` 3308-8300,
  `viewwrite.rs`.
- **N39c constraint / trigger fidelity** (L, ~1,000): PERIOD temporal FKs
  (`reject_temporal_foreign_key` 26489; after N31b), `format_type` for ranges (136 lines
  in `without_overlaps`), full EXCLUDE (`parser.rs` 7825, `enforce_exclusion_constraint`
  10249), REPLICA IDENTITY (`Action::Unsupported` 28981, `relreplident` 20148,
  `indisreplident`), regression C trigger adapter (`trigger_return_old` etc.;
  `routine.rs STATIC_REGRESS_ENTRYPOINTS` 4782), DROP COLUMN dependency tracking (views/
  triggers), `pg_get_triggerdef` + information_schema triggers, `Failing row contains`
  DETAIL (`enforce_not_null` 3240, `enforce_check_constraints` 26688), COMMENT ON
  subobjects, partition trigger clone rules, domain integration gaps.
- **N40a regex engine** (XL, ~500): port `src/backend/regex` (ARE: back-references,
  `\1`, escapes, PG error texts) into `pgtypes/src/regex/`; `regexp_fn.rs compile_pattern`
  472 and `pattern.rs similar_to_regex` 117 switch to it.
- **N40b scalar type fidelity** (L-XL, ~2,600): `name` 63-byte truncation (`datum.rs`
  1002 maps name → Text), `float8` send/recv + `pow` special cases + `erf`/`gamma`,
  numeric typmod overflow/negative scale/`width_bucket`/`to_char`/`to_number`/`lcm`
  scale/`generate_series`, uuid assignment cast (I/O casts TO string types are
  assignment-level: `cast.rs:314`) + `uuidv7`/`uuid_extract_*`, enum (`pg_enum` rows,
  functions, unknown-literal adopts sibling enum type in `coerce_untyped_literal_operands`
  1473, `anyenum`, unsafe-new-value rule), strings (`scs`, bytea input messages,
  `to_bin/oct`, int↔bytea, `unistr`), SQL value functions precision (`current_time` etc.),
  `current_catalog`, typmod casts in views, CASE const-folding, operator on domain,
  `format(*)`, array functions family + `LIKE ANY` + literal DETAIL + `pg_input_*` type
  brackets, `fipshash`/UDF overload with column args (also `brin_multi`, `rowsecurity`
  440 cascade), int→text assignment cast.
- **N40c collations** (L, ~450): CREATE COLLATION (provider builtin/icu/libc, locale,
  deterministic; `collate.utf8.out` target), collation derivation (implicit/explicit,
  `collation mismatch`), `Expr::Collate` (3753) no longer a no-op, `pg_collation`,
  `pg_collation_for`, index collation, EXPLAIN Sort Key `COLLATE`; ordered index text keys
  under non-C collations need collation sort keys (P3 dependency).
- **N40d encoding conversions + regress C functions** (M-L, ~620): conversion tables for
  the encodings the tests exercise (NEW `pgtypes/src/encoding/`), CREATE/DROP CONVERSION
  (`ast.rs` 2156), `pg_conversion`, `convert_to`, regress `test_bytea_to_text` family via
  the C adapter table (shared with N30a/N39c → sequence).
- **N40x SERIALIZABLE** (XXL SSI, ~60 lines; batch 6, decision 5).
- **N41 catalog self-description** (L, ~1,000): `pg_type` full 32 columns + fixture rows,
  `pg_range`, `pg_am`, `pg_shdepend`, catalog PK/index/toast self-description (`type_sanity`/
  `opr_sanity`/`misc_sanity` self-fixture ~350), oid-typed columns (`regclass`/`oid` union
  unification), `proargtypes[0]::regtype` header naming, `regprocedure` quoting,
  `oidvector`, `amvalidate`, `pg_get_catalog_foreign_keys()` SRF (`oidjoins`).
- **N42 psql describe support** (M-L, ~550): `pg_get_function_arguments/identity_arguments/
  result` for builtins (188), sequence describe (with N35a), `\gdesc` describe of utility
  statements, bind-count FATAL, empty SELECT, `BEGIN ATOMIC` in `\df+`, builtin
  SQL-bodied functions, `tableoid`, AUTOCOMMIT-off `\;` batches (87 lines; needs a live
  repro — `relation "foo" already exists` on the first CREATE), `pg_prepared_statements`
  text.

## Verification

Per workstream (in the agent's isolated tree): unit/integration tests in the touched
crates (`cargo nextest run -p <crate>`), `cargo clippy --workspace --all-targets -- -D
warnings`, `cargo +nightly fmt -p <crate>`, `tools/check-pg-compat-matrix.sh`; a focused
run of the owning upstream files (`scripts/gres-pg-regress.sh gres serial --tests …`, or
the snapshot-prefix technique for schedule-dependent files); an A/B against a binary
built from the same base commit; report the per-file delta and "Where the brief was
wrong".

Per batch: commit all slices, then ONE full serial certification of that exact tree
(`GRES_PG_REGRESS_ARTIFACT_DIR=target/pg-regress-runs/<batch>/`), health-check the
artifact (231 result files, none empty, `infrastructure-failures.txt` empty), inspect
every `same-count-different` file, then `scripts/gres-pg-regress-baseline.py update` from
`gres-serial/actual-baseline.json`; commit the ratchet with the artifact name and the
totals in the message. Solo certifications for every "certify alone" workstream before it
joins a batch ratchet. CI's `gres-pg-regress` job (`ci.yml:1210`) must stay green; the
monotone gate fails on an un-ratcheted improvement, so the ratchet rides in the same PR.

Programme-level exit: `scripts/gres-pg-regress.sh gres both` reports 231/231 serial and
parallel with zero infrastructure failures three runs in a row; the baseline file is
empty and deleted; the compat matrix headline is the upstream passed/total; the dated
evidence document is written.

## Triage evidence

The ledgers (799 roots with evidence, oracle facts and fix symbols; 37 verifications; the
EXPLAIN census; the executor study; the synthesis; the compressed `regression.diffs`) are
in `docs/superpowers/notes/2026-08-18-pg-regress-triage/`. Every brief in this programme
is written from those ledgers; this plan is only the index.

## Risks

- `exec.rs` (39k lines) is the serialisation bottleneck: N00 first, then only lane A of
  the planner edits its read path; every other workstream is scoped to named regions.
- Cost fidelity is a page-count problem: relpages emulation can drift after UPDATE/
  DELETE; keep PG's `estimate_rel_size` fallback and measure per file rather than tune.
- Unordered output beyond nested loops (hash-join bucket LIFO order, HashAggregate
  iteration order, UNION dedup) needs PG's hash functions and simplehash growth policy —
  budgeted inside P4, ~1-2k lines stay red until it lands.
- Storage-format changes (P3 keys, N12a arrays, N20 typmod, N35a defaults, N26/N32/N33x
  catalog records) each bump `SCHEMA_VERSION` and are certified alone; local data dirs are
  wiped (greenfield).
- RLS/privilege regressions: `RawScan`/`UnrestrictedTable`/`ReadPermit` remain the sole
  constructors of scan leaves and pushdown paths; the full schedule (not unit tests) is
  what caught the last leak.
- Distributed regressions: the planner prefers `plan/dist.rs` pushdown paths for sharded
  relations until they are costed; the sharded conformance gates stay green per batch.
- The memory-policy change and the sort-order change are observable by the soak/loadtest
  harnesses; both are certified alone and announced.
- Spend/rate limits: batches are dispatched wide; if a run is cut short, results are in
  the journal — resume, do not redo.
