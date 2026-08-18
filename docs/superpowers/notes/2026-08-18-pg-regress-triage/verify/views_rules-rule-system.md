# Verification: views_rules-rule-system

Verdict: CONFIRMED (root cause, attribution within 2%, fix locations mostly right with corrections below).

## 1. Root cause

rules.diff: every rule-bearing block fails on its own `create rule` (`syntax error at position 7: expected Keyword(Table), found Ident("rule")`; `CREATE OR REPLACE RULE` fails at `Keyword(Or)`), and the following DML shows the un-rewritten result (rows stay in rtest_t4, rtest_emplog empty, rtest_order2 empty, rtest_nothn1 keeps "don't want this", rules_log empty, hats duplicate-key errors, ruletest1 keeps all 5 rows). DROP RULE / ALTER RULE hit the 0A000 refusal `the legacy rewrite rule system is not supported`. COMMENT ON RULE fails at `ON` (position 29). ALTER TABLE ... DISABLE RULE fails at `expected \`trigger\``. `pg_rules` does not exist. pg_get_ruledef -> NULL (create_view H51 shows a blank cell; note pg_rewrite exists and returns the `_RETURN` row).

Not a single cascade: the file is a sequence of independent blocks. Non-rule producers inside rules.diff:
- H8/H9 (6): physical row order after UPDATE (PG appends the new tuple; Gres keeps rowid order). Same rule action either way.
- H23/H24 (25): SQL-function `$1` binding in `rtest_viewfunc1` (`where a = $1` returns count 8 for every row).
- H25/H26 (10): `int4smaller` missing -> shoe_ready never exists.
- H36 (1437): pg_catalog `pg_views` definitions dump — 1431 lines; only the 6 lines of the `pg_rules` pg_catalog query (pg_settings_n / pg_settings_u + `relation "pg_rules" does not exist`) are rules.
- H42 (43): `create table test_1 (id integer primary key) inherits (id)` -> `column "id" specified more than once` (inheritance column merge); 1 line is the rule syntax error.
- H55-H58 (15): VALUES-view deparse and `alter table rule_v1 rename column` on a view.
- H66/H67/H68 (32): pg_get_functiondef SET params, pg_get_viewdef(0), pg_get_function_arg_default.
- H73/H74 (73): MERGE ... INSERT ... OVERRIDING USER VALUE in a SQL-body function + \sf deparse layout.

updatable_views.diff rule hunks: H14 (14 of 17), H18 (1), H22-H28 (76), H29 (5 of 35; the other 30 are EXPLAIN plans with Nested Loop / Index Scan / Bitmap and `relation "rw_view2" does not exist` from EXPLAIN-on-view-DML), H83 (20 of 26), H84 (3), H91 (2), H96 (1 of 20; 19 are Merge Join / Sort plan lines), H109 (1), H111 (2), H112 (59; DO ALSO doubles rows — needs view column defaults too).
create_view.diff: H51 (12: pg_get_ruledef of `_RETURN`) + 1 (updlog create rule; the `\d+ tt15v` Rules: block is a cascade of `row(i.*::int8_tbl)` syntax).

## 2. Fix locations (what exists today)

- parser.rs: `bounded_non_goal_refusal` (15954) token-shape matches NON_GOAL_REFUSALS; only `CREATE RULE r AS ON SELECT TO t DO INSTEAD NOTHING` shape is refused, so real CREATE RULE falls to `create_statement` (5488) and dies at `rule`. DROP RULE r ON t / ALTER RULE r ON t RENAME TO r2 shapes are refused (Statement::CompatibilityRefusal). Corrections:
  - ast.rs: variants are `NonGoalCommand::{AlterRule 2082, CreateRule 2092, DropRule 2104}` inside `RefusalCommand::NonGoal`, plus `NON_GOAL_REFUSALS` rows (2224/2245/2268); `command.rs` CommandIdentity rows 36/89/139; parser test `NON_GOAL_REFUSALS.len() == 27` (parser.rs:23197); docs/PG_COMPAT_MATRIX.md rows 148/203/254.
  - ALTER TABLE ENABLE/DISABLE [REPLICA|ALWAYS] RULE: parser.rs ~4008 (`SetTriggerMode` branch expects `trigger`), needs a new AlterTableAction + executor.
  - COMMENT ON RULE r ON t: `comment_on` (parser.rs:8592) has no `ON <relation>` tail at all.
- pgcatalog: no rule store. Model on crates/pgcatalog/src/trigger.rs (own key family + own version byte) — no SCHEMA_VERSION bump needed. Not "next to View in lib.rs".
- exec.rs: rewriter hook = `execute_write_body` (6183) dispatch (before the `is_view_ref` arm) and `execute_view_dml` (5882) / `rewrite_view_write` (5299) which calls `viewwrite::resolve` (677). `resolve_write_subqueries` (4523) is a subquery-folding pass, not the dispatch. `session_replication_role` GUC exists (session.rs:1325; trigger.rs:1379 reads it).
- viewwrite.rs `query_refusal` (480) / `resolve` (677): correct place for the "conditional DO INSTEAD" DETAIL and unconditional-rule bypass.
- catalog_fn.rs:139 `pg_get_ruledef` NullDef: confirmed.
- catalog_rel.rs: `pg_rewrite_rows` (2292) projects only `_RETURN` per view — must gain user rules; information_schema is_updatable/is_insertable_into (2980/3379). `\d+ Rules:` is psql's own query over pg_rewrite + pg_get_ruledef(r.oid,true) gated by pg_class.relhasrules — that flag is built in exec.rs (~20142, ~20422, ~20466). No Gres-side "\d+ section" exists.
- viewdef.rs: SELECT/VALUES deparser only (write_query 141). INSERT/UPDATE/DELETE/NOTIFY/multi-action deparse is NEW code (ruleutils get_insert_query_def / get_update_query_def / get_delete_query_def, `SET (a,b) = (SELECT ...)`, `AS trgt` aliases, WITH-CTE DML). Shared with the SQL-body function deparse gap (rules H74).
- explain.rs `plan_statement` (77): pure AST walk, no rewrite, no `Conflict Resolution/Arbiter Indexes/Conflict Filter`, no `CTE data`/`CTE Scan` — needed for rules H63/H65 (20 lines).
- privilege.rs `require_write` (765): exists; ordering (main relation before rule-action relation; security_invoker view's rule action under view owner) is decided in the rewriter.

## 3. Attribution recount

rules: 818 (of 2452). updatable_views: 184 (of 1452). create_view: 13 (of 1633). Total 1015 vs analyst 1002 (+1.3%).
Planner-only lines I kept OUT of the root: updatable_views H29 30, H96 19 (EXPLAIN plans through rule-rewritten views).

## 4. Hidden prerequisites / fail-longer

- DML deparser (new) for pg_rules / pg_get_ruledef / \d+ (~150 lines of expected text depend on byte-exact layout).
- pg_class.relhasrules + pg_rewrite user rows + psql query path for `\d+`.
- ALTER TABLE ... RULE grammar and ev_enabled state; COMMENT ON RULE grammar; pg_description for rules.
- explain.rs: run the rewriter before planning; ON CONFLICT lines; MATERIALIZED CTE nodes.
- System rules pg_settings_n / pg_settings_u must appear in pg_rules (rules H36 tail).
- Fail-longer after rules exist: DELETE ... WHERE EXISTS (... = shoelace.sl_name) correlated to the target (H32, `missing FROM-clause entry`); ALTER VIEW ALTER COLUMN SET DEFAULT (view defaults; updatable_views H106-H112); timestamptz DEFAULT persistence (base_tbl_hist, updatable_views H18/H19); inheritance column merge (rules H42); int4smaller + SQL-function `$1` (rules H23-H26); `\d+ tt15v` needs `row(i.*::type)` parse (create_view).

## 5. Oracle facts

All quoted messages verified in the diffs: rule-not-found / already exists / renaming ON SELECT (rules H54), COMMENT ON RULE not-found (H1), `_RETURN` drop refusal + HINT (H40), `cannot have ON SELECT rules` DETAIL tables/partitioned tables (H41), `relation "old" in FOR UPDATE clause not found in FROM clause` (H45), ON CONFLICT refusal (H32/H38), MERGE refusal (H71; updatable_views H14/H29), fire order r1..r4 (H19), session_replication_role (H75), permission ordering (H76/H77 + oracle rules.out 3873-3890), pg_rules definition wrap (H59-H63), `\d+ Rules:` layout (H50/H51/H54), pg_get_ruledef `_RETURN` (create_view H51). One PG behaviour the analyst omitted: unqualified `f1` in a rule action -> `column "f1" does not exist` + DETAIL/HINT (H33).
