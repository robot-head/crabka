# Verification: planner_join-writable-pg-class

Verdict: root cause CONFIRMED, fix location mostly right (corrected below), attribution
under-counted (744, not 704), dependencies incomplete but the missing ones are the ones the
analyst already lists as "fail longer".

## 1. Root cause (confirmed)

Gres actual `join_hash.out` lines 1-65: `begin;`, the three `set local`, both plpgsql
functions, `create table simple/bigger_than_it_looks as ...`, `alter table ... set
(parallel_workers = 2)`, `alter table ... set (autovacuum_enabled = 'false')` and both
`analyze` all succeed. First changed line of the diff (hunk 1, oracle line 65):

    update pg_class set reltuples = 1000 where relname = 'bigger_than_it_looks';
    +ERROR:  relation "pg_catalog.pg_class" does not exist

The file is one transaction from `begin;` (line 4) to `rollback;` (oracle line 995), so
every statement after the UPDATE answers "current transaction is aborted" (215 lines) or
`savepoint "settings" does not exist` (25 lines). Not a cascade from anything earlier.

Hunks 7-10 (oracle 1027-1165) are two later, independent transactions and are NOT this root:
- hunks 7-9 (38+38+17 = 93 lines): Hash Join vs Nested Loop plan text -> planner-only.
- hunk 10 (6 lines): wrong result. Lateral `join int4_tbl i4 on t1.fivethous = i4.f1+i8.q2`
  returns 8 rows (extra `456|457|1`, `123|124|1`) where PG returns 4. int4_tbl has no
  f1 = 1 in the oracle; separate correctness root (lateral/join predicate or a leaked
  int4_tbl row), not planner, not pg_class.

## 2. Fix location

- `crates/pgexec/src/exec.rs`, `execute_write_body`, `Statement::Update` arm (line 6453).
  Line 6463-6464: `resolve_relation(...)` then `crabka_pgcatalog::get_table(catalog_kv, table)?`.
  `pg_class` resolves to `pg_catalog.pg_class` (virtual, `is_virtual_relation` exec.rs:19786)
  and `get_table` returns `CatalogError::UndefinedTable` -> `relation "pg_catalog.pg_class"
  does not exist` (pgcatalog/src/lib.rs:872). Nothing today parses-and-refuses; it is a plain
  missing-table error. The dispatch branch for virtual relations must be inserted before
  `get_table` here. Correct.
- `catalog_rel.rs` is the WRONG file for the row synthesis. `pg_class` rows are built in
  `crates/pgexec/src/exec.rs::pg_class_rows` (line ~20380) and `PgClassRow::build`
  (line ~20826); `catalog_rel.rs` only holds oid/namespace helpers.
- The persisted store the analyst asks for ALREADY EXISTS for `reltuples`:
  `crates/pgexec/src/relstats.rs` (`set_reltuples_op`, `RelStats`, key prefix
  `catalog_relstats/tuples/`), read by `pg_class_rows` via `relstats::all` (exec.rs:20392,
  20441). ANALYZE writes it (session.rs:5434-5460). The UPDATE only needs to emit
  `set_reltuples_op` for the matched relations.
- `relpages` is NOT stored: `PgClassRow::build` emits `int(0)` for it. Needs a new
  `catalog_relstats/pages/` key + `RelStats.relpages` + `rename_ops` move + `PgClassRow`
  field. Own keyspace, no `SCHEMA_VERSION` (pgcatalog/src/serde.rs:50) bump.
- Also needed in the same branch: match the WHERE (`relname = '...'`, and `oid =
  'x'::regclass` for reloptions.out) against synthesised rows -> map to `RelationName`.
  Simplest seam: run the virtual scan the SELECT path already uses with the UPDATE's filter
  and project relname/relnamespace.
- Superuser gate: privileges.out expects `permission denied for table pg_class` (42501) for a
  non-superuser UPDATE/DELETE on pg_class; today it gets the same "does not exist".

## 3. Attribution (recount)

Whole-block rule, hunks 1-6 (the aborted transaction): 291+139+40+40+40+194 = 744 lines
(241 '+', 503 '-'). Analyst: 704. Within 6 %.
File total 843; remainder 99 = 93 planner + 6 lateral wrong-result.

Composition of the 744 (what the fix alone recovers):
- 241 '+' error lines: recovered.
- 112 '-' plain result rows (`select count(*)` x many, `length(max)`, two FULL JOIN
  outputs of hjtest_matchbits): recovered if joins run (join.rs already hash-indexes
  equijoins, GUCs `hash_mem_multiplier`, `enable_parallel_hash`,
  `parallel_leader_participation` exist in session.rs 1116-1258).
- 281 '-' EXPLAIN plan lines (Hash Join / Parallel Hash / Gather / Finalize Aggregate):
  planner-only. Will be replaced by Gres Nested Loop plans -> still fails.
- 110 '-' `hash_join_batches(...)` rows (22 blocks x 5): need EXPLAIN (ANALYZE, FORMAT JSON)
  to carry a `"Node Type": "Hash"` node with `Original Hash Batches`/`Hash Batches` under a
  work_mem budget. explain.rs has JSON but no Hash node; `find_hash` returns null and the
  rows print blank. Planner + hash-join batch instrumentation.
So ~353 lines recovered by this root alone; ~391 fail longer on planner_join-cost-planner-explain
and planner_join-blocking-memory-budget.

## 4. Dependencies / hidden prerequisites

- Persisted `relpages` key (new) — missed as already-existing? Analyst said "needs a persisted
  override"; reltuples exists, relpages does not.
- Superuser vs non-superuser check on the virtual-relation write path (privileges.out).
- WHERE evaluation over synthesised rows (`relname =`, `oid = ...::regclass`).
- Sibling files with the SAME root, not listed by the analyst: groupingsets (2 lines, no
  cascade, oracle 1968/2163), reloptions (1 error line + 6-line SELECT mismatch; also needs
  stored `reloptions`), privileges (4 lines: 2 UPDATE/DELETE pg_class permission messages),
  database.out (`UPDATE pg_database SET datacl` — different catalog, same seam family).
- Fail longer: planner (Hash Join text), EXPLAIN ANALYZE JSON Hash node with batch counters,
  memory budget. Analyst already lists these.

## 5. Oracle facts

Oracle join_hash.out line 65 and 76-78: the UPDATE prints nothing (pg_regress runs psql -q,
no `UPDATE 1` tag appears in the .out; the analyst's "UPDATE 1" is the tag psql would show
interactively — harmless). No ANALYZE follows the UPDATE in the file, so the override
stands. PG requires superuser only, not allow_system_table_mods, for a row UPDATE on
pg_class — confirmed by the absence of an error in the oracle. Correct.

## Size

M is right for join_hash's two columns (dispatch branch + relstats relpages key + WHERE
match); the reloptions/pg_database columns push the whole family toward L.
