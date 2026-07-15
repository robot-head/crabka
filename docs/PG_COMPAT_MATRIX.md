# PostgreSQL compatibility matrix for Chapter Gres

This matrix is the SQL-Parity Program anti-rot ledger. Every row has an explicit disposition; no row may be left undecided. `Wave-assigned(...)` means the command is intentionally not complete yet and names the parity wave that owns it. M5 is reached when no command row remains wave-assigned.

Published milestone state: [M0, 2026-07-11](superpowers/evidence/2026-07-11-gres-m0.md).

Run:

```bash
tools/check-pg-compat-matrix.sh --self-test
tools/check-pg-compat-matrix.sh
```

## Current G-1/G-2 baseline

The implemented rows reflect the checked-in parser/executor surface after G-1 and the current G-2-era baseline: simple DDL/DML/query statements, transaction open/close aliases, the donor GUC subset, and the implemented FDW DDL from G-6/G-1 vendoring. Runtime-facing G-2 files remain outside this matrix-infrastructure change.

## Owner and process

- **Owner:** Chapter Gres SQL-Parity Program.
- **Update rule:** every wave that changes SQL acceptance or semantics updates this file in the same change.
- **Inventory rule:** [`pg18-command-inventory.json`](pg18-command-inventory.json) is derived solely from hashed `REL_18_0` SGML artifacts: the SQL entity list plus the `ALTER TABLE`, `CREATE TABLE`, and `SELECT` synopses. The deterministic extraction expands PostgreSQL's abbreviated entity filenames and extracts the seven additional syntax-family titles directly from those synopses, yielding exactly 190 unique titles. Major language features remain a separate typed manifest and never count as commands.
- **Anti-rot rule:** parser acceptance is owned by the typed `CommandIdentity` dispatch registry and gated at every public parse boundary. `tools/check-pg-compat-matrix.sh` checks that registry, inventory, command rows, behavior probes, and the separate major-feature manifest in both directions, then runs executable representatives through an in-memory SQL session. Refusals assert exact SQLSTATE plus a stable message fragment.
- **Bounded-refusal rule:** architectural `Non-goal(...)` rows recognize the representative PostgreSQL 18 syntax recorded in typed parser metadata and fail through the ordinary session path with `0A000`; unrecognized or trailing syntax is never swallowed by a prefix-only matcher.
- **Allowed dispositions:** `Implemented`, `Wave-assigned(<wave>)`, `Mapped(<semantics>)`, `Error-with-notice(<SQLSTATE>)`, `Non-goal(<reason>)`.

## PG18 command rows

| Item | Disposition | Notes |
|---|---|---|
| ABORT | Mapped(ROLLBACK alias) | Parser accepts the stock transaction-abort alias. |
| ALTER AGGREGATE | Wave-assigned(P6) | Stretch routine/object support. |
| ALTER COLLATION | Wave-assigned(T8) | Collation support lands with the T8 ICU decision. |
| ALTER CONVERSION | Non-goal(UTF-8-only server) | C-bound conversion objects are excluded by the program design. |
| ALTER DATABASE | Error-with-notice(0A000) | Bounded rename syntax reaches a typed `0A000` session refusal; tenant provisioning owns lifecycle. |
| ALTER DEFAULT PRIVILEGES | Wave-assigned(D8) | Roles and privileges wave. |
| ALTER DOMAIN | Wave-assigned(T5) | Domain type family. |
| ALTER EVENT TRIGGER | Wave-assigned(P4) | Stretch trigger surface; stock-shaped refusal until then. |
| ALTER EXTENSION | Error-with-notice(0A000) | Bounded update syntax reaches a typed `0A000` session refusal; shims are explicit. |
| ALTER FOREIGN DATA WRAPPER | Wave-assigned(P5) | FDW lifecycle completeness bucket. |
| ALTER FOREIGN TABLE | Wave-assigned(P5) | FDW lifecycle completeness bucket. |
| ALTER FUNCTION | Wave-assigned(P2) | SQL routines. |
| ALTER GROUP | Wave-assigned(D8) | Role synonym surface. |
| ALTER INDEX | Wave-assigned(D2) | Secondary index lifecycle. |
| ALTER LANGUAGE | Non-goal(built-in languages only) | C-bound language object lifecycle is excluded. |
| ALTER LARGE OBJECT | Non-goal(bytea storage path) | Large objects are explicitly out of scope. |
| ALTER MATERIALIZED VIEW | Wave-assigned(D5) | Materialized view lifecycle. |
| ALTER OPERATOR | Non-goal(C-bound operator objects) | C-bound object kind excluded. |
| ALTER OPERATOR CLASS | Non-goal(C-bound operator objects) | C-bound object kind excluded. |
| ALTER OPERATOR FAMILY | Non-goal(C-bound operator objects) | C-bound object kind excluded. |
| ALTER POLICY | Wave-assigned(D8) | RLS wave. |
| ALTER PROCEDURE | Wave-assigned(P2) | SQL procedures. |
| ALTER PUBLICATION | Non-goal(physical replication SQL) | Publication/subscription SQL is outside this chapter. |
| ALTER ROLE | Wave-assigned(D8) | Roles and privileges wave. |
| ALTER ROUTINE | Wave-assigned(P2) | Routine lifecycle. |
| ALTER RULE | Non-goal(legacy rewrite system) | RULE is explicitly excluded. |
| ALTER SCHEMA | Wave-assigned(D7) | Schemas and search_path. |
| ALTER SEQUENCE | Wave-assigned(D3) | Sequence lifecycle and sharded allocation. |
| ALTER SERVER | Error-with-notice(0A000) | Parser accepts FDW server options, but the executor currently rejects `ALTER SERVER` as unsupported rather than mutating catalog metadata. |
| ALTER STATISTICS | Wave-assigned(P5) | Utility bucket; stats integration follows planner work. |
| ALTER SUBSCRIPTION | Non-goal(physical replication SQL) | Publication/subscription SQL is outside this chapter. |
| ALTER SYSTEM | Wave-assigned(P5) | Utility bucket; documented refusal or mapping. |
| ALTER TABLE | Implemented | Bounded subset: `ALTER TABLE name RENAME TO new_name` atomically moves table and sharding metadata, rewrites affected index/ACL metadata, and preserves immutable row/index storage IDs; index names are unchanged. Quoted identifiers and duplicate/missing/wrong-object SQLSTATEs are supported. A rename fails clear with `0A000` while any stored view exists because view definitions are stored as SQL text without rewriteable dependency identities. `RENAME COLUMN` is parsed but fails clear with `0A000` for the same unresolved view/index dependency rewrite. |
| ALTER TABLE ATTACH PARTITION | Wave-assigned(D7) | Partitioning is mapped onto native sharding. |
| ALTER TABLE DETACH PARTITION | Wave-assigned(D7) | Partitioning is mapped onto native sharding. |
| ALTER TABLE ENABLE ROW LEVEL SECURITY | Wave-assigned(D8) | RLS wave. |
| ALTER TABLESPACE | Non-goal(chapter storage model) | Tablespaces remain non-goals. |
| ALTER TEXT SEARCH CONFIGURATION | Wave-assigned(T7) | Full-text search DDL. |
| ALTER TEXT SEARCH DICTIONARY | Wave-assigned(T7) | Full-text search DDL. |
| ALTER TEXT SEARCH PARSER | Non-goal(C-bound text search parser) | C-bound object kind excluded. |
| ALTER TEXT SEARCH TEMPLATE | Non-goal(C-bound text search template) | C-bound object kind excluded. |
| ALTER TRIGGER | Wave-assigned(P4) | Trigger lifecycle. |
| ALTER TYPE | Wave-assigned(T5) | Enum/domain/composite/range types. |
| ALTER USER | Wave-assigned(D8) | Role synonym surface. |
| ALTER USER MAPPING | Error-with-notice(0A000) | Parser accepts FDW user mapping options, but the executor currently rejects `ALTER USER MAPPING` as unsupported rather than mutating catalog metadata. |
| ALTER VIEW | Wave-assigned(D5) | View lifecycle. |
| ANALYZE | Wave-assigned(P5) | Mapped to planner statistics when G-9b is ready. |
| BEGIN | Implemented | Transaction begin is parsed and executed in the baseline. |
| CALL | Wave-assigned(P2) | Procedures. |
| CHECKPOINT | Wave-assigned(P5) | Utility bucket; documented mapping/refusal. |
| CLOSE | Wave-assigned(S2) | SQL cursor lifecycle. |
| CLUSTER | Wave-assigned(P5) | Utility bucket; documented mapping/refusal. |
| COMMENT | Wave-assigned(D4) | COMMENT across supported object kinds. |
| COMMIT | Implemented | Transaction commit is parsed and executed in the baseline. |
| COMMIT PREPARED | Error-with-notice(55000) | Typed session refusal with exact `55000`; participant lifecycle remains internal. |
| COPY | Implemented | Q5 starter subset only: `COPY table [(cols...)] FROM STDIN` over pgwire simple query, text format only. Supports tab-delimited rows, `\\N` NULL, common backslash escapes, explicit column lists, defaults, and NOT NULL enforcement. `COPY TO`, file/program sources, binary, CSV, and COPY options beyond `WITH (FORMAT text)` return clear errors. CopyData is buffered and committed as one statement on CopyDone; CopyFail discards buffered rows. |
| CREATE ACCESS METHOD | Non-goal(C-bound access methods) | C-bound object kind excluded. |
| CREATE AGGREGATE | Wave-assigned(P6) | Stretch SQL aggregate support. |
| CREATE CAST | Wave-assigned(P6) | Stretch SQL/PLpgSQL cast support. |
| CREATE COLLATION | Wave-assigned(T8) | Collation support lands with T8. |
| CREATE CONVERSION | Non-goal(UTF-8-only server) | C-bound conversion objects are excluded. |
| CREATE DATABASE | Error-with-notice(0A000) | Typed session refusal with exact `0A000`; tenant provisioning owns lifecycle. |
| CREATE DOMAIN | Wave-assigned(T5) | Domain type family. |
| CREATE EVENT TRIGGER | Wave-assigned(P4) | Stretch trigger surface. |
| CREATE EXTENSION | Wave-assigned(P5) | Statement supported only for built-in shim whitelist. |
| CREATE FOREIGN DATA WRAPPER | Implemented | FDW DDL is parsed in the current baseline. |
| CREATE FOREIGN TABLE | Implemented | FDW foreign table DDL is parsed in the current baseline. |
| CREATE FUNCTION | Wave-assigned(P2) | SQL routines. |
| CREATE GROUP | Wave-assigned(D8) | Role synonym surface. |
| CREATE INDEX | Implemented | Secondary-index catalog metadata is parsed and persisted; ordinary unsharded MVCC tables maintain local secondary-index entries, and local `UNIQUE` indexes enforce committed/live-row uniqueness for DML/COPY with PostgreSQL-default NULL-distinct semantics. Sharded timestamp-table local indexes, global index access/maintenance, and expression/partial indexes remain fail-clear/deferred. |
| DROP INDEX | Implemented | Supports one simple local-index name with optional `IF EXISTS`; removes catalog metadata and local index entries atomically. Constraint-backed primary-key and unique indexes are protected; global indexes, `CONCURRENTLY`, multi-index drops, and options remain unsupported. |
| CREATE LANGUAGE | Non-goal(built-in languages only) | C-bound language object lifecycle is excluded. |
| CREATE MATERIALIZED VIEW | Wave-assigned(D5) | Materialized views. |
| CREATE OPERATOR | Non-goal(C-bound operator objects) | C-bound object kind excluded. |
| CREATE OPERATOR CLASS | Non-goal(C-bound operator objects) | C-bound object kind excluded. |
| CREATE OPERATOR FAMILY | Non-goal(C-bound operator objects) | C-bound object kind excluded. |
| CREATE POLICY | Wave-assigned(D8) | RLS wave. |
| CREATE PROCEDURE | Wave-assigned(P2) | SQL procedures. |
| CREATE PUBLICATION | Non-goal(physical replication SQL) | Publication/subscription SQL is outside this chapter. |
| CREATE ROLE | Implemented | Starter subset: `CREATE ROLE name` persists role metadata with `rolcanlogin = false`; options, membership, ownership, and authz enforcement remain deferred. |
| CREATE RULE | Non-goal(legacy rewrite system) | RULE is explicitly excluded. |
| CREATE SCHEMA | Wave-assigned(D7) | Schemas and search_path. |
| CREATE SEQUENCE | Implemented | D3 starter: `CREATE SEQUENCE name` with `START WITH`, `INCREMENT BY`, `MINVALUE`/`NO MINVALUE`, `MAXVALUE`/`NO MAXVALUE`, `CACHE` (metadata only; no preallocation), and `CYCLE`/`NO CYCLE`. Ownership, `AS type`, `OWNED BY`, `TEMP`, and identity integration remain outside this starter. |
| CREATE SERVER | Implemented | FDW server DDL is parsed in the current baseline. |
| CREATE STATISTICS | Wave-assigned(P5) | Planner statistics utility bucket. |
| CREATE SUBSCRIPTION | Non-goal(physical replication SQL) | Publication/subscription SQL is outside this chapter. |
| CREATE TABLE | Implemented | Baseline table creation plus column `NOT NULL`, constant/simple-expression `DEFAULT` metadata, D3 starter `SERIAL`/`BIGSERIAL` shorthand backed by a standalone sequence default, and local-table `PRIMARY KEY`/`UNIQUE` constraints backed by local unique-index metadata. `CHECK` syntax is parsed for diagnostics but rejected with 0A000 until enforcement lands; sharded-table `PRIMARY KEY`/`UNIQUE` still fail clear until global enforcement exists. SERIAL ownership/drop dependency details are not implemented. The trailing `WITH (storage_parameter [= value] [, ...])` clause is accepted for shape and ignored: storage parameters tune heap/TOAST behavior Crabka's MVCC-over-KV storage has no equivalent of (pgbench -i emits `WITH (fillfactor=100)` on every table). |
| CREATE TABLE AS | Wave-assigned(Q1) | Statement completeness wave. |
| CREATE TABLE INHERITS | Wave-assigned(D7) | Table namespace/storage semantics wave. |
| CREATE TABLE PARTITION BY | Wave-assigned(D7) | Declarative partitioning maps to native sharding. |
| CREATE TABLE PARTITION OF | Wave-assigned(D7) | Declarative partitioning maps to native sharding. |
| CREATE TABLESPACE | Non-goal(chapter storage model) | Tablespaces remain non-goals. |
| CREATE TEXT SEARCH CONFIGURATION | Wave-assigned(T7) | Full-text search DDL. |
| CREATE TEXT SEARCH DICTIONARY | Wave-assigned(T7) | Full-text search DDL. |
| CREATE TEXT SEARCH PARSER | Non-goal(C-bound text search parser) | C-bound object kind excluded. |
| CREATE TEXT SEARCH TEMPLATE | Non-goal(C-bound text search template) | C-bound object kind excluded. |
| CREATE TRANSFORM | Non-goal(C-bound transform objects) | C-bound object kind excluded. |
| CREATE TRIGGER | Wave-assigned(P4) | Trigger support. |
| CREATE TYPE | Wave-assigned(T5) | Enum/domain/composite/range types. |
| CREATE USER | Implemented | Starter subset: `CREATE USER name` persists role metadata with `rolcanlogin = true`; passwords/options and authentication remain deferred. |
| CREATE USER MAPPING | Implemented | FDW user mapping DDL is parsed in the current baseline. |
| CREATE VIEW | Implemented | Bounded D5 subset: `CREATE VIEW name AS SELECT ...` stores the query text and resolved output schema in the catalog. The definition is evaluated against current base-table rows when selected. Supports a single SELECT over direct base tables with the existing projection/filter/group/order semantics; CTEs, set operations, joins (including multi-item `FROM` / implicit comma joins), derived tables, subqueries, parameters, locking reads, and view-on-view definitions fail clear with `0A000`. |
| DEALLOCATE | Wave-assigned(S2) | SQL PREPARE lifecycle. |
| DECLARE | Wave-assigned(S2) | Cursors. |
| DELETE | Implemented | Baseline DML; Q1 starter `RETURNING` supports `*`, direct columns, simple scalar expressions, and aliases on local MVCC tables. |
| DISCARD | Implemented | `DISCARD ALL` resets session GUC state; other DISCARD targets remain outside this row. |
| DO | Wave-assigned(P2) | Routine/procedural wave. |
| DROP ACCESS METHOD | Non-goal(C-bound access methods) | C-bound object kind excluded. |
| DROP AGGREGATE | Wave-assigned(P6) | Stretch SQL aggregate support. |
| DROP CAST | Wave-assigned(P6) | Stretch SQL cast support. |
| DROP COLLATION | Wave-assigned(T8) | Collation support. |
| DROP CONVERSION | Non-goal(UTF-8-only server) | C-bound conversion objects are excluded. |
| DROP DATABASE | Error-with-notice(0A000) | Typed session refusal with exact `0A000`; tenant provisioning owns lifecycle. |
| DROP DOMAIN | Wave-assigned(T5) | Domain type family. |
| DROP EVENT TRIGGER | Wave-assigned(P4) | Stretch trigger surface. |
| DROP EXTENSION | Error-with-notice(0A000) | Bounded syntax reaches a typed `0A000` session refusal. |
| DROP FOREIGN DATA WRAPPER | Implemented | FDW DDL is parsed in the current baseline. |
| DROP FOREIGN TABLE | Implemented | FDW DDL is parsed in the current baseline. |
| DROP FUNCTION | Wave-assigned(P2) | SQL routines. |
| DROP GROUP | Wave-assigned(D8) | Role synonym surface. |
| DROP LANGUAGE | Non-goal(built-in languages only) | C-bound language object lifecycle is excluded. |
| DROP MATERIALIZED VIEW | Wave-assigned(D5) | Materialized views. |
| DROP OPERATOR | Non-goal(C-bound operator objects) | C-bound object kind excluded. |
| DROP OPERATOR CLASS | Non-goal(C-bound operator objects) | C-bound object kind excluded. |
| DROP OPERATOR FAMILY | Non-goal(C-bound operator objects) | C-bound object kind excluded. |
| DROP OWNED | Wave-assigned(D8) | Roles and privileges. |
| DROP POLICY | Wave-assigned(D8) | RLS wave. |
| DROP PROCEDURE | Wave-assigned(P2) | SQL procedures. |
| DROP PUBLICATION | Non-goal(physical replication SQL) | Publication/subscription SQL is outside this chapter. |
| DROP ROLE | Implemented | Starter subset drops persisted role metadata and recorded table ACL rows for that grantee; dependency checks and owned-object handling remain deferred. |
| DROP ROUTINE | Wave-assigned(P2) | Routine lifecycle. |
| DROP RULE | Non-goal(legacy rewrite system) | RULE is explicitly excluded. |
| DROP SCHEMA | Wave-assigned(D7) | Schemas and search_path. |
| DROP SEQUENCE | Implemented | Drops a standalone D3 starter sequence. Dependency-aware `DROP OWNED`, cascade/restrict behavior, and SERIAL ownership cleanup remain unsupported. |
| DROP SERVER | Implemented | FDW server DDL is parsed in the current baseline. |
| DROP STATISTICS | Wave-assigned(P5) | Planner statistics utility bucket. |
| DROP SUBSCRIPTION | Non-goal(physical replication SQL) | Publication/subscription SQL is outside this chapter. |
| DROP TABLE | Implemented | Baseline table drop. |
| DROP TABLESPACE | Non-goal(chapter storage model) | Tablespaces remain non-goals. |
| DROP TEXT SEARCH CONFIGURATION | Wave-assigned(T7) | Full-text search DDL. |
| DROP TEXT SEARCH DICTIONARY | Wave-assigned(T7) | Full-text search DDL. |
| DROP TEXT SEARCH PARSER | Non-goal(C-bound text search parser) | C-bound object kind excluded. |
| DROP TEXT SEARCH TEMPLATE | Non-goal(C-bound text search template) | C-bound object kind excluded. |
| DROP TRANSFORM | Non-goal(C-bound transform objects) | C-bound object kind excluded. |
| DROP TRIGGER | Wave-assigned(P4) | Trigger support. |
| DROP TYPE | Wave-assigned(T5) | Enum/domain/composite/range types. |
| DROP USER | Implemented | Starter synonym for the `DROP ROLE` metadata path. |
| DROP USER MAPPING | Implemented | FDW user mapping DDL is parsed in the current baseline. |
| DROP VIEW | Implemented | Drops a stored view atomically. Missing views return `42P01`; `IF EXISTS` is accepted as a no-op only when the relation is absent. A table named by `DROP VIEW`, including with `IF EXISTS`, returns wrong-object-type `42809`. |
| END | Mapped(COMMIT alias) | Parser accepts the stock commit alias. |
| EXECUTE | Wave-assigned(S2) | SQL PREPARE lifecycle. |
| EXPLAIN | Wave-assigned(S6) | Stable plan surface. |
| FETCH | Wave-assigned(S2) | Cursors. |
| GRANT | Implemented | Starter subset: `GRANT ... ON TABLE ... TO ...` records table ACL metadata for existing roles/tables but does not enforce data access. |
| IMPORT FOREIGN SCHEMA | Implemented | FDW import DDL is parsed in the current baseline. |
| INSERT | Implemented | Baseline DML; column defaults fire for omitted columns and explicit `DEFAULT`, `NOT NULL` is enforced, and Q1 starter `RETURNING` supports `*`, direct columns, simple scalar expressions, and aliases on local MVCC tables. ON CONFLICT remains Q1 breadth. |
| LISTEN | Wave-assigned(S4) | Notification bus. |
| LOAD | Non-goal(C-bound code loading) | Loading code is excluded. |
| LOCK | Wave-assigned(S3) | Lock table and advisory locks. |
| MERGE | Wave-assigned(Q1) | Statement completeness. |
| MOVE | Wave-assigned(S2) | Cursors. |
| NOTIFY | Wave-assigned(S4) | Notification bus. |
| PREPARE | Wave-assigned(S2) | SQL PREPARE lifecycle. |
| PREPARE TRANSACTION | Error-with-notice(55000) | Typed session refusal with exact `55000`; participant lifecycle remains internal. |
| REASSIGN OWNED | Wave-assigned(D8) | Roles and privileges. |
| REFRESH MATERIALIZED VIEW | Wave-assigned(D5) | Materialized view refresh. |
| REINDEX | Wave-assigned(P5) | Utility bucket. |
| RELEASE SAVEPOINT | Wave-assigned(S1) | Savepoint support. |
| RESET | Implemented | F-1 GUC registry supports `RESET name` and `RESET ALL` for common client settings. |
| REVOKE | Implemented | Starter subset: `REVOKE ... ON TABLE ... FROM ...` removes recorded table ACL metadata; no data-access enforcement yet. |
| ROLLBACK | Implemented | Transaction rollback is parsed and executed in the baseline. |
| ROLLBACK PREPARED | Error-with-notice(55000) | Typed session refusal with exact `55000`; participant recovery remains internal. |
| ROLLBACK TO SAVEPOINT | Wave-assigned(S1) | Savepoint support. |
| SAVEPOINT | Wave-assigned(S1) | Savepoint support. |
| SECURITY LABEL | Non-goal(C-bound security labels) | Explicitly excluded. |
| SELECT | Implemented | Baseline query expression; Q waves own breadth. |
| SELECT INTO | Wave-assigned(Q1) | Statement completeness. |
| SET | Implemented | F-1 typed GUC registry supports common client/session settings with PostgreSQL 18 transaction-local semantics; pinned tokio-postgres, SQLx, and psycopg startup/SET captures are validated and replayed directly against Gres in CI. |
| SET CONSTRAINTS | Wave-assigned(D6) | DEFERRABLE constraints. |
| SET ROLE | Implemented | Starter subset: `SET ROLE name` switches `current_user` only to an existing role; `SET ROLE NONE`/`RESET ROLE` restore the initial `public` session user. Membership checks are not implemented. |
| SET SESSION AUTHORIZATION | Wave-assigned(D8) | Roles and privileges. |
| SET TRANSACTION | Implemented | Isolation syntax is accepted and maps to the existing READ COMMITTED/REPEATABLE READ session state; SSI wiring remains non-goal for this row. |
| SHOW | Implemented | F-1 GUC registry supports `SHOW name` and `SHOW ALL` over common client settings. |
| START TRANSACTION | Mapped(BEGIN alias) | Parser accepts the stock begin alias. |
| TABLE | Wave-assigned(Q1) | Standalone TABLE statement. |
| TRUNCATE | Wave-assigned(D4) | Table lifecycle wave. |
| UNLISTEN | Wave-assigned(S4) | Notification bus. |
| UPDATE | Implemented | Baseline DML with `NOT NULL` enforcement on assigned rows; Q1 starter `RETURNING` supports `*`, direct columns, simple scalar expressions, and aliases on local MVCC tables. UPDATE FROM remains Q1 breadth. |
| VACUUM | Wave-assigned(P5) | Mapped to garbage horizon/compact hint. |
| VALUES | Implemented | Baseline query body. |

## Major language-feature rows

| Item | Disposition | Notes |
|---|---|---|
| ARRAY expressions and operators | Wave-assigned(T4) | Includes `ARRAY[]`, `ANY`/`ALL`, subscripts, slices, and `unnest`. |
| COLLATE expression | Wave-assigned(T8) | Depends on the collation implementation decision. |
| Column DEFAULT constraints | Implemented | Stored as catalog metadata for supported scalar datums and D3 starter `nextval` defaults from SERIAL; applied on INSERT omitted/`DEFAULT` values. Date/time, interval, bytea, generated, and general volatile defaults remain unsupported. |
| Column NOT NULL constraints | Implemented | Stored as catalog metadata and enforced on INSERT/UPDATE with SQLSTATE 23502. |
| Extended-protocol parameterized queries | Wave-assigned(F-0) | Parser accepts `$n` placeholders and partial extended-protocol Bind/Execute parameter binding exists; keep this wave-assigned until remaining gaps are tracked through runtime conformance and full parity is verified. |
| `information_schema` starter views | Implemented | Virtual `information_schema.schemata`, `information_schema.tables`, and `information_schema.columns` support common client preambles and simple introspection over the single `postgres` catalog, `public` user/foreign tables, and starter column metadata (`data_type`, `is_nullable`, supported defaults). Broader SQL-standard metadata, privileges, domains, generated columns, collations, and non-public schemas remain deferred to their owning waves. |
| Scalar type `varchar(n)` / `character varying(n)` | Implemented | Stored as text-compatible values with PostgreSQL OID 1043 and `atttypmod = n + 4`; INSERT/UPDATE and explicit casts enforce character length, allowing truncation only when discarded characters are spaces. Unbounded `varchar` is a text-compatible type with typmod -1. |
| Scalar type `char(n)` / `character(n)` | Implemented | Stored as text-compatible values with PostgreSQL OID 1042 and `atttypmod = n + 4`; assignment/casts pad shorter values with spaces and raise SQLSTATE 22001 for non-space overflow. Bare `char` maps to `char(1)`. |
| Scalar type `uuid` | Implemented | Starter support: parser accepts `uuid`, OID 2950 appears in `RowDescription`, `pg_type`, and `pg_attribute`; text input accepts canonical, uppercase, braced, and hyphenless forms and canonicalizes to lowercase hyphenated text with SQLSTATE 22P02 for invalid input. Runtime row storage reuses canonical text rather than a dedicated `Datum` variant until row encoding grows a first-class UUID payload. |
| Scalar type `real` / `float4` | Wave-assigned(T2) | Not implemented in this starter; `float8` remains the only floating scalar. |
| Scalar type `smallint` / `int2` | Wave-assigned(T2) | Not implemented in this starter; `int4`/`int8` remain the supported integer widths. |
| CHECK constraints | Error-with-notice(0A000) | Parsed in `CREATE TABLE`, but rejected rather than persisted silently until check-expression enforcement is complete. |
| GROUPING SETS / ROLLUP / CUBE | Wave-assigned(Q3) | SELECT completeness. |
| JSON_TABLE and SQL/JSON expressions | Wave-assigned(T3) | PG18 SQL/JSON surface. |
| MERGE NOT MATCHED BY SOURCE / RETURNING | Wave-assigned(Q1) | PG17/PG18 MERGE breadth. |
| OLD/NEW RETURNING aliases | Wave-assigned(Q1) | PG18 DML RETURNING feature. |
| Recursive CTE SEARCH / CYCLE | Wave-assigned(Q3) | SELECT completeness. |
| Sequence functions | Implemented | D3 starter supports `nextval('name')`, `currval('name')` with session-local “nextval first” semantics, and `setval('name', value[, is_called])` over persisted sequence state. Regclass resolution is string-name only; permissions, schemas, ownership, and replicated/sharded block allocation remain deferred. |
| SQL identity / generated columns | Wave-assigned(D3) | `GENERATED ... AS IDENTITY` and generated expression columns remain unsupported; only SERIAL/BIGSERIAL shorthand is implemented as a starter. |
| Row locking NOWAIT / SKIP LOCKED / KEY SHARE | Wave-assigned(Q3) | Job-queue and lock semantics. |
| SQL/JSON constructors and aggregates | Wave-assigned(T3) | JSON type/function wave. |
| Table PRIMARY KEY / UNIQUE constraints | Implemented | Local non-sharded `CREATE TABLE` column/table `PRIMARY KEY` and `UNIQUE` constraints create local unique-index catalog metadata, enforce DML/COPY uniqueness through the existing local-index path, use PostgreSQL-default NULL-distinct semantics for UNIQUE, and make primary-key columns `NOT NULL`. Sharded/global enforcement remains fail-clear. |
| WITH ORDINALITY / ROWS FROM | Wave-assigned(Q3) | SRFs-in-FROM breadth. |
