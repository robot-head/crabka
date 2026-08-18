# txn_session cluster — root-cause triage (read-only analysis, 2026-08-17)

Files (16): transactions 262, combocid 101, lock 84, advisory_lock 90, portals 1232,
prepared_xacts_1 286, temp 125, cluster 101, vacuum 185, plancache 76, prepare 81,
sequence 445, identity 528, generated_stored 511, generated_virtual 601, fast_default 353.
Total 5061 changed lines. Planner/EXPLAIN-only: 360 (portals 44, cluster 60, plancache 40,
generated_stored 51, generated_virtual 97, fast_default 68 — of the fast_default 68, ~52 are
EXPLAIN VERBOSE renderer work: `Output:` lines, schema-qualified relation, `t.col`
qualification, `'-1'::integer` literal rendering, BETWEEN expansion; 16 need a Bitmap plan).

Counting: whole-block attribution; per-hunk numbers come from analysis/txn_session/hunks.py.

## Per-file summary

| file | first failing statement | cascade | primary root | other roots (lines) |
|---|---|---|---|---|
| transactions | BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE (0A000) | partial | txn-transaction-characteristics 60 | serializable 28, implicit-block 29, cursor-lazy 28, memory-budget cascade 61, xmin 18, cmdid 11, plpgsql RETURN 9, carets 8, snapshot 4, warning 2, heap-order 2, routine SET clause 2 |
| combocid | SELECT ctid,cmin,* FROM combocidtest (42703 cmin) | yes | txn-mvcc-system-columns 101 | secondary: heap-append ctid moves, command-id semantics |
| lock | ALTER ROLE regress_rol_lock1 SET search_path (parse) | no | txn-pg-locks-view 73 | lock privileges 4, alter-role-set 1, C-lang fn 6 |
| advisory_lock | first SELECT ... FROM pg_locks WHERE locktype='advisory' (0 rows) | no | txn-pg-locks-view 90 (incl. 8 unlock WARNING lines) | |
| portals | DECLARE foo1 SCROLL CURSOR FOR SELECT * FROM tenk1 ORDER BY unique2 (53200 memory budget) | yes (H1 725) | memory-budget 696 | WHERE CURRENT OF 284, pg_cursors 88, planner 44, locking-path resolution 28, SQL-fn utility final stmt 25, FOR UPDATE join 20, toast GUC 19, serializable 17, no-scroll 5, cursor-lazy 4, rules 2 |
| prepared_xacts_1 | SELECT current_setting('max_prepared_transactions') (42704) | yes | txn-prepared-transactions-guc 286 | with the GUC = 0 the script \quit s and the file is byte-exact |
| temp | SELECT * FROM temptest caret (after \c) | partial | pg_temp namespace objects 34 | CTAS ON COMMIT parse 28, PREPARE TRANSACTION refusal 22, ON COMMIT DROP inheritance 17, temp buffer pins 14, ON COMMIT FK 5, fn-error shape 3, carets 2 |
| cluster | hastoast column of pg_class query | no | planner 60 | pg_partition_tree 32, toast 5, heap-order 2, cluster diagnostics 2 |
| vacuum | CREATE INDEX ON vaccluster(wrap_do_analyze(i)) (user fn in index expr) | partial | vacuum/analyze diagnostics 82 | pg_stat_*_tables 74, ddl-expr user fn 6, toast 6, SRF in VALUES 5, failing-row DETAIL 5, SET STORAGE 3, index AM 2, inheritance notice 1, serializable 1 |
| plancache | plpgsql `return f1 from v1;` (parse) | partial | planner 40 | plpgsql RETURN tail 25, plan counters 10, plpgsql notice 1 |
| prepare | EXECUTE q2('postgres') (pg_database lacks postgres row) | partial | pg_prepared_statements catalog 40 | CREATE TABLE AS EXECUTE 28, EXECUTE diagnostics 7, pg_database rows 4, carets 2 |
| sequence | CREATE SEQUENCE sequence_testx INCREMENT BY 0 message shape | partial | txn-seq-full-ddl 387 | seq privileges 38, default expressions 15, harness dbname 4, failing-row DETAIL 1 |
| identity | ALTER TABLE itest3 ALTER COLUMN a ADD GENERATED ALWAYS AS IDENTITY (unsupported) | partial | identity ALTER forms 174 | partitions/inheritance 114, OVERRIDING 95, ALWAYS enforcement 31, SET LOGGED 30, info_schema 26, create validation 19, drop-owned-seq 11, seq DDL 11, inheritance merge 8, typed tables 6, drop view multi 3 |
| generated_stored | information_schema.columns ... is_generated | partial | partition rules 137 | inheritance DDL 84, planner 51, diagnostics 45, table row type 26, carets 20, fn privileges 20, rules 20, info_schema 15, triggers 14, tableoid 12, sublink name 12, alter view 10, qualified names 9, deparse 8, copy where 6, ddl-expr user fn 6, failing-row DETAIL 4, statistics 4, range ctor 3, heap-order 2, typed tables 2, partial index 1 |
| generated_virtual | same as stored | partial | partition rules 139 | planner 97, inheritance DDL 81, diagnostics 53 (incl. virtual-only rules 19), qualified names 30, WHERE CURRENT OF 24, triggers 24, carets 20, rules 20, info_schema 15, table row type 14, sublink name 12, tableoid 12, fn privileges 12, failing-row DETAIL 10, alter view 10, locking resolution 8, deparse 8, copy where 6, ON CONFLICT partitioned 4, typed tables 2 |
| fast_default | ALTER TABLE has_volatile ADD col3 timestamptz DEFAULT current_timestamp ("defaults ... not persisted yet") | partial | drop-table-leaves-owned-sequence 141 | column DEFAULT expressions 100, planner/renderer 68, random named args 19, foreign tables 11, rewrite notice 10, tableoid 4 |

## Load-bearing findings

1. Sequences are encoded as `CreateIndex` on a fake relation (parser.rs::create_sequence,
   SEQUENCE_RELATION); catalog `Sequence` has no data type / owner; `ALTER SEQUENCE` is not
   parsed at all; no lastval, no regclass overloads, no pg_sequences, no smallserial. Also
   DROP TABLE does not drop the owned serial/identity sequence (drop_table_and_dependents_ops /
   drop_table_ops never emit drop_sequence_ops) — 141 lines in fast_default alone.
2. Column DEFAULT is stored as an evaluated value (ColumnDefault::Value | NextVal;
   column_from_ast runs eval_assignment_value at DDL time): DEFAULT nextval('myseq'),
   DEFAULT now(), temporal defaults ("not persisted yet") and user functions in DEFAULT fail.
3. Cursors are materialized at DECLARE (session.rs::declare_cursor -> run_select): a 10k-row
   cursor trips the 20 MiB blocking-query budget (portals H1 = 725 lines), errors surface at
   DECLARE not FETCH, "portal cannot be run" never happens, volatile functions cannot see later
   inserts. WHERE CURRENT OF is not parsed.
4. SET TRANSACTION drops READ ONLY/READ WRITE/DEFERRABLE (set_transaction_tail keeps only the
   isolation level); SET SESSION CHARACTERISTICS AS TRANSACTION goes to the same place; AND
   CHAIN copies only the isolation level (chained_isolation); deferrable is not in TxnCtx.
5. Multi-statement simple queries are not an implicit transaction block (session.rs::simple_query).
6. pg_locks is empty: catalog_rel::rows falls through to information_schema_rows; lockmgr.rs
   holds table + advisory holds but nothing renders them.
7. max_prepared_transactions GUC is missing: adding it (=0) makes prepared_xacts match the _1
   variant — 286 lines for an S.
8. xmin/xmax/cmin/cmax absent (scope.rs SYSTEM_COLUMNS), command-id visibility absent, UPDATE
   keeps rowid order where PG appends the new version at the heap end.
9. Locking read path (exec.rs::execute_read_locking) reports 42P01 for DECLARE ... FOR UPDATE on
   a temp table (portals uctest) and a search_path table (generated_virtual gtest_cursor) while
   the same SELECT without FOR UPDATE works. Needs a runtime repro.

## Brief corrections
- file_stats.json has no prepared_xacts_1 entry.
- The classifier's gres-error counts are far off (portals has 205 +ERROR lines, not 30).
- fast_default's EXPLAIN gap is mostly VERBOSE renderer (Output:, qualification), not plan choice.
- PG_COMPAT_MATRIX ROLLBACK TO SAVEPOINT row (writes cannot be undone) is stale: transactions.out
  shows sub-transaction write rollback works.
- PG_COMPAT_MATRIX SET TRANSACTION/BEGIN rows overstate READ ONLY/DEFERRABLE support: the parser
  carries the modes only for BEGIN.
- The harness connects to database `crab` (scripts/gres-pg-regress.sh GRES_DB), so every printed
  database name differs from the oracle's `regression`.
