# views_rules cluster — root-cause triage (read-only)

Files: rules (2452), create_view (1633), updatable_views (1452), select_views (1036), matview (173). Total 6746 changed lines.
Counting: whole-block attribution; splits inside mixed hunks are estimates; sums reconcile to the file totals.

## Per-file summary
| file | lines | first failing statement | cascade | primary root | planner-only est. | exact w/o planner |
|---|---|---|---|---|---|---|
| rules | 2452 | `create rule rtest_v1_ins as on insert to rtest_v1 do instead ...` -> `syntax error at position 7: expected Keyword(Table), found Ident("rule")` | partial | rule-system (805) + system-catalog-view-definitions (1437) | 5 | yes (marginal) |
| create_view | 1633 | `CREATE VIEW iexit AS ... interpt_pp(...)` -> `cannot execute function interpt_pp(path,path): Gres has no c interpreter` | no (chains) | parser-gaps (457), view-bound-storage (396), viewdef-deparse (344) | 39 | no |
| updatable_views | 1452 | `CREATE VIEW ro_view19 AS SELECT * FROM uv_seq` -> `relation "uv_seq" does not exist` | no | view-write-rewrite-gaps (264), rule-system (183), view-column-defaults (173) | 405 | no |
| select_views | 1036 | `SELECT name, #thepath FROM iexit ...` -> `relation "iexit" does not exist` | yes (901/1036) | create-view-no-eval (902) | 113 | no |
| matview | 173 | `EXPLAIN (costs off) CREATE MATERIALIZED VIEW mvtest_tm ...` -> `Result` | no | planner (52), view-bound-storage (25), unknown type (23) | 52 | no |

## Roots (lines across the 5 files)
R1 rule-system (XXL) 1002 — rules 805, updatable_views 183, create_view 14
R2 view-bound-storage (XL) 421 — create_view 396, matview 25
R3 viewdef-deparse-fidelity (L) 356 — create_view 344, rules 12
R4 system-catalog-view-definitions (XL) 1437 — rules
R5 parser-gaps (M) 521 — create_view 457, updatable_views 64
R6 view-column-defaults (M) 173 — updatable_views
R7 view-privileges (L) 98 — updatable_views
R8 view-write-rewrite-gaps (L) 264 — updatable_views
R9 merge-into-views (M) 22; R10 row-locking-through-views (M) 34; R11 matview-refresh-semantics (M) 12; R12 view-reloptions-catalog (S) 42; R13 temp-view-notice (S) 17; R14 replace-view-type-checks (S) 2; R15 view-pkey-dependency (S) 3; R16 security-barrier-qual-ordering (L) 26; R17 sql-function-inline-capture (M) 37; R18 relation-rowtypes-as-types (M) 117; R19 sequence-as-relation (S) 25; R20 drop-if-exists-notice (S) 2; R21 dependency-cascade-order (M) 73; R22 storage-order-after-update (L) 12; R23 dml-correlated-subqueries (M) 9; R24 values-typmod-resolution (S) 24; R25 unknown-pseudo-type (S) 45; R27 misc-catalog-functions (S) 74; R29 restrict_nonsystem_relation_kind (S) 6; R30 alter-set-schema (M) 17; R31 inherit-merge-column (foreign) 43; R32 union-type-resolution (foreign) 21; R33 column-default-persistence (foreign) 9; R34 harness-dbname (S) 8; R35 returning-old-new-wholerow (M) 94; R36 assignment-io-cast (foreign) 3; R37 check-option-detail-row-order (S) 2; R38 merge-partitioned (foreign) 8; R40 create-view-no-eval (S) 902; R41 pg-depend-catalog (foreign) 45; R43 explain-create-matview (S) 14; R44 alter-view-rename-column (S) 10; R45 identifier-truncation-notice (foreign) 16; R46 operator-error-detail (foreign) 4; R48 merge-sql-body-parse-deparse (foreign) 73; INDEX (foreign) 5; ROLES (foreign) 2; LEFTOVERS (foreign) 2; PLANNER 614.

Deparse vs behaviour: pg_get_viewdef/pg_get_ruledef fidelity proper = R3 356 + rule display inside R1 (~120) + R4 1437 (if done by deparse). All the rest is behaviour; R2 (421) is the storage prerequisite for both.

## Fix locations — see the structured output (same content).

## Brief corrections
- select_views is 87% a cascade of create_view's `CREATE VIEW iexit` failing because Gres calls the C function `interpt_pp` while deriving view column types (routine.rs `callable()` inside `inline_scalar_call`).
- updatable_views' "~340 explain-ish" is ~405 planner-only; 264 lines are auto-updatable rewrite defects, not rules.
- rules is 58% one block: the pg_catalog `pg_views` definition dump (1437 lines), unrelated to CREATE RULE.
- pgcatalog's "security_barrier is inert for a structural reason" is wrong in observable terms: NOTICE ordering is asserted by the tests, and UPDATE through a barrier view evaluates the leaky qual on hidden rows.
- matview's `FROM mvtest_mvschema.mvtest_tvm` needs bound view storage (R2), not only ALTER ... SET SCHEMA.
