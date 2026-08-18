# Verification: tsfr-fdw-catalogs-psql

Verdict: root cause CONFIRMED, attribution CONFIRMED (my count 617 vs 613),
fix locations MOSTLY RIGHT with three corrections and four additions,
dependencies INCOMPLETE (SET ROLE for non-bootstrap superuser, list_* catalog
scans, CURRENT_USER folding in user mappings, pg_options_to_table SRF).

## 1. Root cause (foreign_data.diff, 1650 changed lines total)

Block segmentation script: fd_blocks.py / fd_classify.py (this directory).

Blocks that fail *directly* on the missing catalogs (not a cascade):

| producer | blocks | lines |
|---|---|---|
| `\dew` (relation pg_catalog.pg_foreign_data_wrapper does not exist) | 1 | 9 |
| `\dew+` (column "fdwoptions" does not exist) | 14 | 124 |
| `\des` (relation pg_catalog.pg_foreign_server) | 4 | 58 |
| `\des+` (column "srvoptions") | 8 | 110 |
| `\deu` (relation pg_catalog.pg_user_mappings) | 4 | 40 |
| `\deu+` (column "umoptions") | 6 | 71 |
| `\det+` (column "ftoptions") | 1 | 7 |
| direct `SELECT … FROM pg_foreign_data_wrapper / pg_foreign_server / pg_user_mapping` | 6 | 32 |
| `information_schema.foreign_* / user_mapping* / usage_privileges / role_usage_grants` | 14 | 126 |
| `has_*_privilege(…, (SELECT oid FROM pg_foreign_*), …)` (relation missing) | 6 | 36 |
| `has_server_privilege('regress_test_role','s8','USAGE')` t vs f | 2 | 4 |
| **total** | 66 | **617** |

The first failing statement of the file is `DROP ROLE IF EXISTS a, b, c` (multi-name
DROP ROLE grammar, 1 line, other root). The first FDW-catalog failure is
`SELECT fdwname, … FROM pg_foreign_data_wrapper` at diff line ~30. It is not a
cascade: nothing before it could have created the relation.

Not attributed here (other roots, correctly excluded by the analyst):
- 364 lines of `\d+ ft1 / ft2 / ft3 / fd_pt2_1 / foreign_table_1` and the
  `Child tables: ft2, FOREIGN` / `Partitions: … FOREIGN` lines: cascade of
  CREATE FOREIGN TABLE grammar (`c1 integer OPTIONS (…)`, `NOT NULL` inside the
  column list, `()` empty column list, `PARTITION OF`, `INHERITS`). Once the
  grammar is fixed those blocks fail longer ON THIS ROOT: psql's describe for
  relkind 'f' queries `pg_foreign_table f, pg_foreign_server s` and
  `pg_options_to_table(attfdwoptions)`.
- 331 grammar lines, 25 "not supported" refusals, 29 SET ROLE lines, ~258
  semantics lines (dependency tracking, owner checks, NOTICE texts, option
  validation), and ~30 misc \d+ lines (SET STATISTICS, CHECK deparse
  `((c1 > 0))`, NO INHERIT).

## 2. Fix locations

Confirmed:
- `crates/pgexec/src/catalog_rel.rs`: `PG_CATALOG_RELATIONS`,
  `INFORMATION_SCHEMA_RELATIONS`, `RELATION_NAMES`, `relation_oid` /
  `system_view_oid`, `columns()`, `rows()`. `exec::virtual_table` already
  falls back to `catalog_rel::catalog_relation`, so no exec.rs registry change.
  `pg_description_rows` must also emit rows for classoid 2328/1417 (COMMENT ON
  FDW / SERVER, once the grammar accepts them).
- `crates/pgcatalog/src/lib.rs`: `ForeignDataWrapper { name, options }`,
  `ForeignServer { name, wrapper, options }`, `UserMapping { user, server,
  options }`, `ForeignTableMeta { server, options }` — no owner, handler,
  validator, acl, srvtype, srvversion, oid. Confirmed.
- `crates/pgexec/src/catalog_fn.rs`: `PRIVILEGE_FUNCTIONS` doc says the
  eleven non-relation ones "still return true unconditionally". Confirmed.

Corrections:
- `serialize_fdw / serialize_server / serialize_user_mapping` live in
  `crates/pgcatalog/src/serde.rs` (lines 1973, 2313, 2342), not lib.rs. The
  records are unversioned (no SCHEMA_VERSION byte), so widening them is a
  storage-format change but needs no SCHEMA_VERSION bump. Column-level FDW
  OPTIONS (for `\d+` "FDW options" column) would go into the table schema
  record (`Column`) and DOES need a SCHEMA_VERSION bump — that storage belongs
  to whichever root owns `c1 integer OPTIONS (…)`.
- `crates/pgexec/src/exec.rs` "pg_class row builder (relkind)": WRONG as a
  reason. `pg_class_rows` (exec.rs:20400-20408) already reports `f` for
  `table.foreign.is_some()`, and `information_schema_tables_rows` reports
  `FOREIGN`. `Child tables: ft2, FOREIGN` / `Partitions: …, FOREIGN` come from
  psql reading that relkind, so they need nothing here beyond the objects
  existing. exec.rs still needs `normalize_mapping_user` (exec.rs:1860-1877)
  removed: it folds `FOR CURRENT_USER` to `"public"` (Gres error
  `object "public@s1" already exists`), so `pg_user_mapping.umuser` /
  `pg_user_mappings.usename` cannot be right until it is.
- Missing location: `crates/pgexec/src/srf.rs::classify` — `pg_options_to_table`
  is absent (grep finds none in crates/). Every `\dew+ / \des+ / \deu+ / \det+`
  and every `\d` of a foreign table calls it.
- Missing location: `crates/pgcatalog/src/lib.rs` has no `list_fdws /
  list_servers / list_user_mappings` (only get_/create_/drop_ by key). Row
  producers need prefix scans over `key::fdw_key / server_key /
  user_mapping_key` (crates/pgkv/src/key.rs:528-544).

## 3. Attribution
617 by whole-block rule vs analyst 613 — within 1%.

## 4. Dependencies (missed)
- SET ROLE by a non-bootstrap superuser: `session.rs::set_role` (4159) →
  `pgcatalog::role_can_set` (5163) bypasses only `BOOTSTRAP_ROLE`, so
  `regress_foreign_data_user` (LOGIN SUPERUSER, via SET SESSION AUTHORIZATION)
  gets "permission denied to set role" 29 times. Every Owner column value other
  than regress_foreign_data_user (t1/t2 → regress_test_role, s1 →
  regress_test_indirect) and the three `\deu+` visibility blocks depend on it.
- `test_fdw_handler` (RETURNS fdw_handler, LANGUAGE C) is created OK in the
  Gres run; `postgresql_fdw_validator(text[], oid)` must exist as a builtin
  pg_proc row for `fdwvalidator::regproc` to print its name (grammar root
  needs it too).
- Owner/handler/validator/acl/type/version values require the grammar root
  (HANDLER, VALIDATOR, TYPE, VERSION, OWNER TO, RENAME, GRANT/REVOKE USAGE ON
  FOREIGN DATA WRAPPER / FOREIGN SERVER, COMMENT ON FDW/SERVER/FOREIGN TABLE)
  and the semantics root (dependency tracking). Fail-longer: with catalogs alone
  most `\dew+`/`\des+` blocks still differ in Validator/FDW options/Owner/ACL/
  Description columns.
- Planner: 0 lines.

## 5. Oracle facts
All header layouts, ACL text, information_schema view set (10) and
`PUBLIC | regression | s4` confirmed against self-check foreign_data.out.
Catalog oids 2328/1417/1418/3118 are PostgreSQL's. One imprecision: the
user-mapping option visibility rule is not "owner sees own, superuser all,
others none". PG's pg_user_mappings shows umoptions when (a) the mapping is
for current_user and current_user has USAGE on the server or owns it, (b) the
mapping is PUBLIC and current_user has the server owner's privileges, or (c)
current_user is superuser. Oracle: server owner regress_test_role sees
`s10 | public | ("user" 'secret')` but NOT `s10 | regress_unprivileged_role`'s
options; superuser sees both.

## Other files touching this root (analyst listed only foreign_data)
- alter_generic (2 blocks, 16 lines: SELECT fdwname FROM pg_foreign_data_wrapper
  / srvname FROM pg_foreign_server)
- psql (2 blocks, 12 lines: `\des "no.such.foreign.server"`, `\dew …`)
- select_parallel (information_schema.foreign_data_wrapper_options; block is
  currently a txn-aborted cascade)
- create_table_like (`\d+ ctl_foreign_table1/2` — cascade of LIKE grammar,
  fails longer on pg_foreign_table + Server: footer)
- rules (pg_user_mappings definition text in the pg_views dump — system-view
  definitions root, but the object is this one)
- oidjoins (NOTICE lines for the four catalogs — whole file is another root)
