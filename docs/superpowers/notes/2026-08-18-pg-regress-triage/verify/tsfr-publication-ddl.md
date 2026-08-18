# Verification: tsfr-publication-ddl (publication.out)

Verdict: CONFIRMED (root cause, fix locations mostly right, attribution within 1%).

## 1. Root cause
- First failing statement is `CREATE PUBLICATION testpub_default;` -> `ERROR: physical replication SQL is not supported`
  (diff @@ -8, Gres out line 11). Not a cascade from anything earlier: the three
  preceding CREATE ROLE + SET SESSION AUTHORIZATION succeed.
- Only three representative spellings hit the 0A000 refusal:
  `crates/pgparser/src/ast.rs` NON_GOAL_REFUSALS entries
  `(AlterPublication, "ALTER PUBLICATION pub ADD TABLE t")`, `(CreatePublication, "CREATE PUBLICATION pub")`,
  `(DropPublication, "DROP PUBLICATION pub")` matched by `bounded_non_goal_refusal` (parser.rs:15954)
  before the real parser runs. Everything else falls into `create_statement` (parser.rs:5488)
  / the ALTER dispatch (parser.rs ~3650-3790, `unexpected token after ALTER`) where
  "publication" is `Ident` (not a lexer keyword) and produces the syntax errors seen
  (88x "position 7", 165x "position 0", 6x "position 5" for multi-name DROP).
- 59x `column "pubgencols" does not exist`: pg_publication exists as an always-empty
  synthesized catalog (catalog_rel.rs:148-150 always-empty list; column set at
  catalog_rel.rs:1406-1416 has no pubgencols; `rows()` at 774 has no arm for it).
- 7x `relation "pg_publication_tables" does not exist`: no view, no pg_get_publication_tables().
- COMMENT ON PUBLICATION: `comment_ops` exec.rs:31868 -> "COMMENT ON {other} is not supported".
- DROP COLUMN c succeeds because the DropColumn arm (exec.rs:28370) only consults
  `dependent_view_names` and generated columns; then CREATE UNIQUE INDEX (b,c) and
  ALTER c SET NOT NULL fail (2 cascade lines) - publication root.
- DML replica-identity checks: no code; write entry `execute_write_parts` exec.rs:4361 /
  `statement_trigger_targets` exec.rs:4201 already computes (relation, DmlEvent) and is the seam.

## 2. Fix locations
Analyst's list is right for parser.rs / ast.rs / pgcatalog lib.rs / catalog_rel.rs / exec.rs.
Corrections:
- Row-filter validation should not go in eval.rs (evaluator). The existing seam is exec.rs
  `is_immutable_function` (26356) + generated-expression validator (~26300); user-defined
  operator/function/type/collation detection needs catalog lookups (routine.rs / pgcatalog),
  so a new `crates/pgexec/src/publication.rs` next to `policy_ddl.rs` is the natural home.
- Missing: session.rs statement dispatch (6921 handles CompatibilityRefusal; 6973 lists DDL
  variants) must gain the new variants; parser.rs:23197 test asserts NON_GOAL_REFUSALS.len()==27
  and session.rs:21046 iterates them - both change; docs/PG_COMPAT_MATRIX.md rows 145/201/251.
- Missing: COMMENT ON PUBLICATION -> exec.rs `comment_ops` + pgcatalog `CommentObject` (lib.rs:3774)
  + obj_description(oid,'pg_publication') classoid mapping.
- Missing: catalog_fn.rs `pg_relation_is_publishable` (732) is a stub returning true; psql \d
  'Publications:' section for FOR ALL TABLES pubs joins on it - must return false for
  views/system relations.
- pg_publication_rel prattrs must be int2vector (psql casts `prattrs::pg_catalog.int2[]` and
  subscripts it), currently Text; prqual is Text where psql calls pg_get_expr(prqual, oid).
- Permission model: privilege.rs has no database-level ACL; role_is_superuser exists (rls.rs:717).

## 3. Attribution (whole-block rule)
Total changed lines: 1202. Non-publication blocks:
- ALTER TABLE REPLICA IDENTITY FULL/NOTHING/USING INDEX refusals: 29 error lines + 2 for the
  `\d+` index line " REPLICA IDENTITY" suffix = 31 (shared-alter-table-replica-identity)
- SET ROLE 'permission denied to set role': 5 (pgcatalog lib.rs:5163 `role_can_set` ignores
  rolsuper; only BOOTSTRAP_ROLE bypasses) (shared-set-role-superuser)
- GRANT/REVOKE CREATE ON DATABASE parse: 2 (shared-grant-object-kinds)
- ADD PRIMARY KEY USING INDEX parse: 1 (shared-add-pk-using-index)
- DROP ROLE a, b parse: 1
- INSERT ON CONFLICT on partitioned: 2 (shared-insert-on-conflict-partitioned)
- CREATE COLLATION: 1 (shared-create-collation)
- virtual generated column with user-defined function must fail at CREATE TABLE: 4 (+2 cascade)
  -> generated-columns root, NOT publication
=> publication root: 1202 - 47 = 1155 (1153 if the 2 cascade lines go to gencol). Analyst 1144: within 1%.

## 4. Dependencies / fail-longer
All six analyst dependencies confirmed real. Additional:
- generated-columns: only the "generation expression uses user-defined function" virtual-gencol
  restriction is missing; STORED and VIRTUAL generated columns already parse+create (Gres out 391,396,570).
  Analyst note "needs generated columns" overstated.
- pg_publication_tables partition semantics need partition ancestor lookup (partition.rs).
- After parser+catalog land, the DML-check block (~60 lines: 'cannot update table ...' + DETAIL/HINT)
  still fails until REPLICA IDENTITY is stored (Table.relreplident + indisreplident); permission block
  (~10 lines) still fails until SET ROLE-superuser + database CREATE ACL exist.

## 5. Oracle facts
All quoted messages verified in self-check-serial/results/publication.out (e.g. lines 1192-1218,
1458-1464, 1539, 1630, 672-673, 1146-1147). \dRp header, 'Tables:' / 'Tables from schemas:',
'(a, c) WHERE (c <> 1)' all as stated. Also present but not listed: 'cannot add relation' DETAILs
for temporary / unlogged / system tables; 'WHERE clause not allowed for schema';
'argument of PUBLICATION WHERE must be type boolean, not type integer';
'aggregate functions are not allowed in WHERE'; SET EXPRESSION refusal on published virtual gencol.
