# catalog_psql cluster — root-cause triage (read-only)

Files: psql (1197), psql_pipeline (172), type_sanity (329), opr_sanity (363), misc_sanity (217), oidjoins (221), largeobject (457). Total 2956 changed lines. Planner-only lines: 0 in every file (the "explain-ish" 33 lines in psql are `EXPLAIN EXECUTE test \gdesc`, a Describe problem, not a plan).

## Brief corrections
1. Harness: Gres runs with `--dbname=crab` (scripts/gres-pg-regress.sh:28) while the oracle uses `regression`; psql's `\dt regression."no.such.schema"...` family compares the database name against PQdb() client-side, so 132 psql lines are "cross-database references are not implemented" only because of the harness. Gres accepts any startup dbname (pgwire session.rs:1039). One-line fix.
2. server.log in the artifact is 4 lines; no per-statement diagnostics.
3. file_stats error_lines undercount (largeobject ~99, type_sanity 44).
4. 87 psql lines (AUTOCOMMIT off + `\;` batches) begin with `relation "foo" already exists` on the first CREATE TABLE foo of the run; static reading of session.rs did not find the cause — needs live repro.
5. `heap2` in `\dA` comes from the create_am test (CREATE ACCESS METHOD), not from the pg_am fixture.
6. `blocking query exceeded the memory budget` (53200, 20MiB via GRES_PG_REGRESS_BLOCKING_QUERY_MEMORY) appears in 21 result files; here type_sanity 122, opr_sanity 100, psql 48.

## Per-file first failure / cascade
- psql: `SELECT $1, $2 \bind_named ... \g` lacks `LINE 1:` caret (error position). No global cascade; sub-cascades: tableam_display (CREATE ACCESS METHOD), \df+ (BEGIN ATOMIC language), AUTOCOMMIT-off batch, zeropriv (SET LOCAL ROLE).
- psql_pipeline: pipeline BEGIN/INSERT 1/ROLLBACK/BEGIN/INSERT 1/COMMIT → duplicate key: previous pipeline's INSERT 1 was not rolled back because Gres has no implicit transaction block across Execute messages until Sync (pgexec session.rs sync() only clears portals). Cascade until `ROLLBACK;` (expected line 461).
- type_sanity: `NOT t1.typisdefined` → column does not exist (pg_type has 12 of 32 columns, exec.rs:19917).
- opr_sanity: `p1.proargtypes[0]::regtype` column named `regtype` instead of `proargtypes`.
- misc_sanity: pg_shdepend missing; rest is catalog self-description (atttypid/attstorage/reltoastrelid/PK constraints of the catalogs themselves).
- oidjoins: pg_get_catalog_foreign_keys() missing (cascade over the whole DO block).
- largeobject: lo_create missing (cascade; whole LO subsystem).

## Line attribution (whole-block)
psql: error-position 10; pg_prepared_statements text 8; bind-count FATAL 4; param inference in func args 13; describe utility 13; syntax-error message 32; empty SELECT 2; empty query in aborted tx 2; sequence type+Owned by 106; builtin pg_get_function_* 188; CREATE ACCESS METHOD 120; pg_am fidelity 59; format_type 4; plpgsql CONTEXT 12; memory budget 48; SRF in VALUES 6; partitioned-index pg_inherits 68; catalog self-fixture 10; missing catalogs 82; tableoid 18; pg_type columns 12; pg_type fixture 19; builtin SQL-bodied functions 67; obj_description 2; BEGIN ATOMIC 11; update heap order 2; autocommit batch 87; harness dbname 132; role grant options 23; zeropriv ACL 37.
psql_pipeline: implicit tx 142; describe utility 18; bind-count 8; empty SELECT 4.
type_sanity: pg_type columns 111; pg_type fixture 42; pg_range 22; memory 122; self-fixture 27; C entrypoint 5.
opr_sanity: column naming 24; pg_type columns 14; correlated ARRAY subquery 5; regtype/oid unify 5; tableoid 29; regprocedure quoting 14; memory 100; oidvector 31; amvalidate 7; pg_am 10; user opclass family 8; self-fixture 116.
misc_sanity: missing catalogs 7; self-fixture 210.
oidjoins: catalog FKs 221. largeobject: large objects 457.
