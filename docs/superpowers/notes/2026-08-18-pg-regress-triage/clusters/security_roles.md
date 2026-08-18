# Cluster security_roles — root-cause triage (2026-08-17)

Files: privileges 1765 / rowsecurity 2215 / create_role 137 / password_1 71 / guc 164 /
database 10 / namespace 29 / sysviews 130 / misc 231 / misc_functions 767 = 5519 changed lines.

Whole-block attribution per hunk is in `tally.py` (hunk index -> root); totals in `tally.json`.
Planner-only estimate (needs a cost-based planner + PG EXPLAIN renderer): privileges 96,
rowsecurity 66, misc_functions 175. Total ~337 of 5519.

## Per-file summary

| file | lines | first failing statement | cascade? | primary root | planner-only |
|---|---|---|---|---|---|
| privileges | 1765 | `SELECT lo_unlink(oid) FROM pg_largeobject_metadata …`; first substantive: `GRANT … WITH ADMIN OPTION GRANTED BY …` (syntax) | partial | sec-large-objects (427) / sec-column-privileges (230) / sec-default-privileges (157) / sec-role-membership-model (154) / sec-nonrelation-object-privileges (144) | 96 |
| rowsecurity | 2215 | `GRANT EXECUTE ON FUNCTION f_leak(text) TO public;` (syntax); first behavioural: `\dp` relacl empty | partial: fipshash arg-type inference (440 cascade), ALTER TABLE INHERIT (397 cascade) | sec-explain-syntactic-shape (628) | 66 |
| create_role | 137 | `GRANT CREATE ON DATABASE regression …` (syntax); first behavioural: `ALTER ROLE regress_role_limited REPLICATION` DETAIL | partial (RENAME TO fails → tenant objects mis-owned) | sec-role-attributes-lifecycle (131) | 0 |
| password_1 | 71 | `SET password_encryption = 'novalue'` HINT | no | sec-role-attributes-lifecycle (65) | 0 |
| guc | 164 | `SET intervalstyle to 'asd'` HINT | small (SET SESSION AUTHORIZATION error) | sec-guc-registry-and-session-state (101), sec-guc-runtime-scope (48) | 0 |
| database | 10 | `CREATE DATABASE regression_tbd ENCODING …` | yes | sec-database-ddl | 0 |
| namespace | 29 | `CREATE MATERIALIZED VIEW test_maint_mv AS SELECT fn(i)` → unrecognized parameter search_path | yes (block) | sec-guc-runtime-scope (27) | 0 |
| sysviews | 130 | `pg_available_extension_versions` | no | sec-sysviews-catalog-views | 0 |
| misc | 231 | `UPDATE tmp SET stringu1 = reverse_name(…)` | partial (`$1.name` parse → all postquel queries) | sec-sql-function-composite-notation (218) | 0 |
| misc_functions | 767 | `SELECT num_nonnulls(NULL)` | small (`\gset` wal_segment_size) | sec-misc-admin-functions (404) | 175 |

## Brief corrections
1. privileges is not "~0 explain-ish": 96 EXPLAIN-plan lines (leaky-view section), planner-only once `<<<`/`>>>` lex.
2. rowsecurity's ~946 explain-ish lines: ~628 sit in EXPLAIN hunks; only ~66 need a cost-based planner. The rest is deterministic shape (RLS Filter quals, Append, InitPlan, EXECUTE, view expansion).
3. database.diff has no `\c`; it is CREATE/ALTER/DROP DATABASE grammar+lifecycle plus a superuser `UPDATE pg_database`.
4. misc, misc_functions, namespace are only nominally security.
5. privileges COPY data-line echo (`bar true`, `invalid command \.`) is a cascade of the permission failure.
6. rowsecurity `SELECT * FROM t1 FOR SHARE` → relation does not exist: locking-select path ignores search_path (inheritance/locking), not RLS.
7. Gres routines have no schema (parser `routine_name` refuses non-public qualifier); many "does not exist" errors stem from that.

Full root list with fix locations: see StructuredOutput of this agent (also mirrored in tally.py root ids).
