# Verification: tsfr-fdw-ddl-grammar (foreign_data)

Verdict: root cause CONFIRMED (grammar is the producer of the largest cascade), fix location
PARTLY confirmed (parser/AST/exec arms exist; the catalog layer and several ordinary ALTER TABLE
subcommands are missing from the list), attribution PLAUSIBLE (811 by my whole-block count vs 758),
dependencies NOT confirmed (analyst lists none; there are five).

## 1. First failing statement / cascade

foreign_data.diff line 8: the FIRST failing statement is `DROP ROLE IF EXISTS a, b, c, ...`
-> `syntax error ... found Comma` (parser.rs:4519 `drop_role` reads one name). 1 line, no cascade.
Second/third: `COMMENT ON FOREIGN DATA WRAPPER` (parser.rs:8596 first-word match lacks Keyword::Foreign)
and `CREATE FOREIGN DATA WRAPPER postgresql VALIDATOR ...` (parser.rs:13887 create_fdw: name + parse_options only).
So the analyst's claim that the FDW grammar is the producer of the big cascade is right; the very first
diff line is a role-DDL gap, not FDW.

Gres error strings quoted in the claim all appear verbatim in the diff (checked: Ident("validator"),
unexpected token after ALTER: Keyword(Foreign) x100, at or near "foreign" x25, Keyword(For)/Keyword(If),
expected RParen found Keyword(Options|Not), expected LParen found Ident("partition"), string literal found
Ident("dbname"|"host"|"username"|"modified"), identifier found RParen / Keyword(User), found Comma).

## 2. Fix locations (read)

- crates/pgparser/src/parser.rs
  - 3696-3789 ALTER dispatch on `Token::Ident("alter")`; fallthrough error at 3785. Add
    `Token::Keyword(Keyword::Foreign)` arm (peek3 Data -> alter_fdw, Table -> alter_foreign_table).
  - 13843 parse_options (ident + string only; no ADD/SET/DROP; `user` is Keyword::User so expect_ident fails).
  - 13865 parse_user_mapping_user (PUBLIC/CURRENT_USER/ident only; no USER, CURRENT_ROLE, SESSION_USER).
  - 13887 create_fdw, 13916 create_server (no IF NOT EXISTS/TYPE/VERSION), 13934 alter_server (options only),
    13960 create_user_mapping (no IF NOT EXISTS), 13977 alter_user_mapping, 13995 drop_user_mapping,
    14014 create_foreign_table (own column loop: name type [collate]; no constraints/OPTIONS/empty list/
    PARTITION OF/INHERITS), 14057 drop_foreign_table (one name), 14073 import_foreign_schema (no OPTIONS).
  - 4568/4615 grant/revoke: after ON only SCHEMA or table list -> at-or-near "foreign".
  - 8592 comment_on: first-word arm lacks Keyword::Foreign (continuation loop already knows DATA/WRAPPER/TABLE).
  - error.rs:141 `reporting_position()` exists; the new rules must opt in for `ALTER FDW foo;`,
    `ALTER SERVER s0;`, `CREATE FOREIGN TABLE ft1 ();`, and the HANDLER x HANDLER y "conflicting or
    redundant options" (parser.rs:15323 shows the message already used elsewhere).
- crates/pgparser/src/ast.rs 955-1032: variants carry name+options only. Confirmed.
- crates/pgexec/src/exec.rs 1826-1952: arms confirmed; 1947 AlterServer|AlterUserMapping -> 0A000
  refusal via RefusalCommand (ast.rs:2049). CreateForeignTable arm (1896) passes only `Column::new(name, ty)`
  -> no constraints, no per-column options.
- MISSED: crates/pgcatalog/src/lib.rs 610-633 ForeignDataWrapper/ForeignServer/UserMapping (no owner,
  handler, validator, type, version, ACL, comment), 268 ForeignTableMeta (server + table options only,
  no per-column options), 6279 create_fdw_ops, 6355 create_server_ops (does not check the wrapper exists),
  6321 drop_fdw_ops (no dependents), 6538 create_foreign_table_ops (no constraints);
  crates/pgcatalog/src/serde.rs 1973/2313/2342 serialize_fdw/server/user_mapping, SCHEMA_VERSION=12 (line 50)
  guards the schema record that carries ForeignTableMeta -> per-column options need a bump.
- MISSED: comment_ops exec.rs:31868 handles "foreign table" but refuses "server"/"foreign data wrapper"
  (needs CommentObject variants in pgcatalog).
- MISSED: ALTER TABLE actions the ALTER FOREIGN TABLE section needs but ordinary ALTER TABLE lacks:
  SET SCHEMA (parser.rs:4152 -> Unsupported), INHERIT parent (no AlterTableAction variant),
  SET STATISTICS / SET STORAGE / SET (n_distinct) (parser.rs:4267 -> Unsupported; the file also shows
  them failing on ordinary fd_pt1: 20 lines), ALTER COLUMN ... OPTIONS (...). ALTER SEQUENCE is not in
  the ALTER dispatch at all (3 lines).

## 3. Attribution (whole-block, script fdw_blocks_cls.json)

Total 1650 changed lines, 499 blocks.
- grammar direct (incl. 0A000 ALTER SERVER/USER MAPPING refusals): 342 lines / 211 blocks
  (ALTER FOREIGN TABLE 114, ALTER FDW 54, ALTER SERVER 40, CREATE FOREIGN TABLE 42, GRANT/REVOKE 33,
  ALTER USER MAPPING 19, CREATE FDW 11, CREATE USER MAPPING 10, CREATE SERVER 7, COMMENT 4, DROP UM 4,
  DROP FT list 2, IMPORT 2)
- cascade from grammar (ft1/ft2/ft3/ct3/foreign_table_1/fd_pt2_1/ft_part* never exist): 469 lines
  (400 of them are `\d+` foreign-table describe output; 39 expected relkind/dependency errors; 30 plain
  does-not-exist errors)
- => 811 attributable to this root under the counting rule (analyst 758: within 7%).
- Not this root: fdw-catalogs-psql 613 (pg_foreign_* / information_schema.foreign_* / \dew+ \des+ \deu+
  \det+ / pg_options_to_table), fdw-privileges 45 (+ has_server_privilege stub returns t), state cascade
  from missing SET ROLE + failed drops 45, fdw-dependencies 38, SET ROLE from non-bootstrap superuser 28
  (pgcatalog lib.rs:5163 role_can_set ignores rolsuper), ALTER TABLE SET STATISTICS/STORAGE 20,
  CHECK deparse `((c1 > 0))` 16, foreign-table relkind refusals 10, REASSIGN/DROP OWNED 7,
  IF EXISTS NOTICE 5, ALTER SEQUENCE 3, misc 8.

## 4. Fail longer

After the grammar lands: 400 cascade lines need psql `\d+` for relkind f (Foreign table header,
FDW options column, Server:/FDW options:/Inherits:/Child tables: ... FOREIGN, Partition of:), i.e.
tsfr-fdw-catalogs-psql; 105 grammar-direct lines need option validation (postgresql_fdw_validator is in
builtin_procs tsv but has no evaluator; "invalid option"/"HINT: Perhaps you meant"/"provided more than
once"/"option not found"/"must return type fdw_handler"/WARNING on handler change); 52 need FDW/server
ownership+ACL enforcement (which itself needs SET ROLE to work: 28-line root); 20 need caret-bearing PG
syntax errors; ~39 need relkind refusals on foreign tables (PK/UNIQUE/FK/index/USING/constraint
trigger/transition tables) and unique-index-vs-foreign-partition checks.

## 5. Oracle facts

All checked against the oracle .out / diff minus side: `ALTER FOREIGN DATA WRAPPER foo;` and
`ALTER SERVER s0;` are `syntax error at or near ";"` with LINE 1 + caret; `CREATE FOREIGN TABLE ft1 ();`
likewise, and `... () SERVER no_server` is `server "no_server" does not exist`; HANDLER x HANDLER y is
`conflicting or redundant options` + LINE/caret; `CREATE FOREIGN TABLE ft2 () INHERITS (fd_pt1) SERVER
s0 OPTIONS (...)` succeeds. Correct.

## 6. Size

L is too small for the scoped unit. Grammar+AST alone is L; making every form execute (catalog record
changes for owner/handler/validator/type/version/column options/constraints, ALTER FDW/SERVER/UM real
updates, ALTER FOREIGN TABLE via the ALTER TABLE path incl. SET SCHEMA/INHERIT/SET STATISTICS/SET STORAGE
/column OPTIONS, PARTITION OF/INHERITS foreign tables, GRANT/REVOKE storage) is XL.
