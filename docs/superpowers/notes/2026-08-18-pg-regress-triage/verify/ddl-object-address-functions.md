# Verification: ddl-object-address-functions

Verdict: root cause CONFIRMED, fix location PARTLY WRONG (incomplete), attribution
REASONABLE (my count 611 primary + 43 secondary for object_address; analyst ~606),
dependencies INCOMPLETE (three hidden prerequisites in the executor itself).

## 1. Root cause (object_address.diff, 691 changed lines)

Block census (whole-block rule, headers excluded):

| block | lines | first failing cause |
|---|---|---|
| CREATE TEXT SEARCH TEMPLATE / PARSER | 2 | "C-bound text search objects are not supported" (ts template/parser root) |
| CREATE AGGREGATE/FUNCTION/PROCEDURE addr_nsp.* | 3 | schema-qualified routine names (ddl-routine-namespace) |
| CREATE TRIGGER t ... addr_nsp.trig() | 4 | cascade of the above |
| ALTER DEFAULT PRIVILEGES x2 | 2 | parser: ALTER DEFAULT (priv-grant-grammar) |
| CREATE TRANSFORM | 1 | parser |
| CREATE PUBLICATION x2, CREATE SUBSCRIPTION | 5 | parser (pubsub) |
| CREATE STATISTICS addr_nsp.x | 1 | parser: qualified statistics name |
| 3 error-case SELECTs | 12 | **pg_get_object_address missing** |
| DO loop over unsupported types | 7 | **pg_get_object_address missing** (also: error must be SQLSTATE 22023 so `WHEN invalid_parameter_value` catches it) |
| 4 misc "select * from pg_get_object_address(...)" | 20 | **missing** (FROM-item form) |
| big DO loop | 408 | **missing** (204 expected WARNINGs, 204 actual) |
| 28 unqualified-name SELECTs | 112 | **missing** |
| identity table | 52 | **missing** (fails longer: see prerequisites) |
| DROP FDW CASCADE notice | 5 | drop-cascade notice for FDW (dependency graph) |
| DROP PUBLICATION x2 / SUBSCRIPTION | 3 | pubsub |
| DROP SCHEMA cascade list | 12 | cascade of missing objects + ordering (dependency graph) |
| DROP OWNED BY | 1 | parser |
| invalid-objects VALUES query | 43 | **`unify_types(int4, regclass)` in eval.rs:5363 fires first** ("types integer and regclass cannot be matched"); then `'pg_ts_parser'::regclass` etc. would 42P01 (no such catalog relations); then the three functions |

Directly attributable: 12+7+20+408+112+52 = **611**; plus 43 secondary = 654.
Analyst: 704 total minus 98 in other files = ~606 for object_address. Within 30%.

The first failing statement of the file is NOT this root (CREATE TEXT SEARCH TEMPLATE),
but nothing about the function blocks cascades from it: the DO loops only produce
"does not exist"/validation errors, and Gres reaches every one of them. Only the
identity table (52) and the invalid-objects block (43) are gated on the DDL above.

## 2. Other files

- alter_operator (135 total): first failing statement is `CREATE OPERATOR === ...
  NEGATOR = !==` -> "negator operator !== does not exist: ... this catalog does not
  create shell operators". Everything after is a cascade of that + `ALTER OPERATOR`
  being unparsed ("unexpected token after ALTER: Ident("operator")"). The seven
  pg_describe_object blocks total 10+9+9+9+10+6+6 = **59** lines by visible symptom
  (analyst 40). Even with the function they still fail: pg_depend_rows in
  catalog_rel.rs:1017 emits only trigger and event-trigger rows, no operator->function
  / operator->schema rows.
- create_am (295): pg_describe_object blocks 11+6+5+6+5 = 33 by symptom (analyst 39);
  all gated on CREATE ACCESS METHOD heap2 (tables), ALTER TABLE SET ACCESS METHOD
  (unsupported subcommand), and pg_depend rows relation->pg_am.
- create_cast (34): one block, -7 +1 = **8** (analyst 8). Gated on CREATE CAST WITH
  FUNCTION ("not supported") and pg_depend rows for casts.
- event_trigger (247): one block, -7 +4 = **11** (analyst 11). This one IS attainable by
  this root alone (pg_event_trigger rows + oids exist). Note Gres types `e.oid` as
  integer, so the signature must accept int4/oid/regclass for classid/objid.

## 3. Fix location

Exists / correct:
- crates/pgexec/src/catalog_fn.rs: enum `CatalogFunc` (line 68) and `fn catalog_func(name)`
  (line 118) — the analyst's symbol name `catalog_func_by_name` is wrong, the function is
  `catalog_func`. Right place for the SCALAR members: pg_describe_object (text) and
  pg_identify_object (record). No `describe_object|identify_object|get_object_address`
  string anywhere in crates/ except a doc comment in pgcatalog/src/lib.rs:1188.
- crates/pgexec/src/catalog_rel.rs: `relation_oid` (242), `pg_depend_rows` (1017),
  `check_constraint_oids` (460), `not_null_constraint_oids` (478) exist.
- crates/pgexec/src/reg_fn.rs: `enum RegKind` (43), `fn resolve` (219), `fn render` (243) exist.

Missing from the analyst's list:
- crates/pgexec/src/srf.rs: `enum Srf` + `fn classify` (line ~300) + `plan`/`rows`.
  Every FROM-position call (`FROM objects, pg_get_object_address(type,name,args) AS addr1,
  pg_identify_object_as_address(...) AS ioa (typ,nms,args)`, `select * from
  pg_get_object_address(...)`, `LATERAL pg_identify_object_as_address(...)`) goes through
  exec.rs:17780 -> srf::from_item, whose `plan` returns 42883 for any name `classify`
  does not know. `pg_input_error_info` (a non-SETOF record function in FROM) is the
  precedent. This is 20 + 52 + 11 lines of the symptom, and `PERFORM pg_get_object_address`
  in the DO loops needs the select-list form too (srf.rs docs: multi-column SRF in the
  select list is 0A000 today; the plpgsql loop catches WHEN OTHERS so the *message* would
  differ).
- crates/pgexec/src/exec.rs `resolve_projection` (24790): `(pg_identify_object(...)).*` —
  `Expr::FieldSelectAll` is unsupported everywhere (eval.rs:567 "(row).* is only supported
  in a SELECT output list", and rowtypes.out shows that message even in an output list).
  Gate for the 52-line identity table.
- crates/pgexec/src/eval.rs `unify_types` (5363): int4 vs regclass/oid must resolve to the
  reg type (PG: int4->regclass implicit, reverse assignment). Gate for the 43-line block.
- Catalog relations that do not exist at all in Gres (grep over crates/pgexec/src):
  pg_foreign_data_wrapper, pg_foreign_server, pg_user_mapping, pg_ts_parser,
  pg_ts_template, pg_transform, pg_default_acl, pg_subscription, pg_largeobject,
  pg_parameter_acl, pg_auth_members. `'pg_ts_parser'::regclass` resolves through
  catalog_fn::relation_oid -> exec::resolve_base_relation and will 42P01. Needed by
  the 43-line block and by pg_identify_object's per-classid dispatch.
- Note pg_roles is mapped to oid 1261 in exec.rs:22683 area (grep) — that is
  pg_auth_members' oid in PostgreSQL; identify_object(1261,...) must say "role membership".

State today: not parsed as anything special; the names simply are not registered, so
42883 at bind time (scalar) or from srf::plan (FROM item).

## 4. Dependencies

Analyst's list is right for the identity table (routine namespace, ts template/parser,
transform, pubsub, statistics, default privileges, dependency graph, access method).
Add: (expr).* projection expansion; unify_types for oid/reg with ints; the eleven
missing catalog relations; CREATE OPERATOR shell/negator forward reference and ALTER
OPERATOR grammar (alter_operator producer); ALTER TABLE SET ACCESS METHOD (create_am);
CREATE CAST WITH FUNCTION (create_cast); pg_depend rows for operators, casts, and
relation->pg_am (dependency graph). No planner, no wire, no storage-format change.

Fail-longer: after the functions exist, alter_operator/create_am/create_cast blocks
still differ (pg_depend content, failed producers); object_address identity table
still differs until all eight DDL roots land; invalid-objects block still differs
until unify_types + catalog names land.

## 5. Oracle facts

All quoted messages verified in self-check-serial/results/object_address.out
(lines 58-329, 457-505, 613-655). Extra ones worth listing: "name list length must be
at least 2/3", "argument list length must be exactly 1", "unrecognized default ACL
object type "i"", "user mapping for user "eins" on server "integer" does not exist",
"access method "addr_nsp" does not exist" (opclass/opfamily), "invalid input syntax for
type oid: "blargh"", "large object 123 does not exist". Identity strings verified.

## Sizing note

559 of the 611 lines are validation / "does not exist" paths that need only the
name-list/arg-list rules and existence probes (a probe against a catalog Gres lacks
still answers "does not exist"). Only 52 + 43 + the four other files need the inverse
(identity/description) direction. XL is fair for the full family; a forward-only
phase is M-L.
