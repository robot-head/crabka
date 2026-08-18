# Verify: views_rules-system-catalog-view-definitions

Verdict: root cause CONFIRMED, count CONFIRMED (1437 exact), fix locations partly right, hidden prerequisites found.

## Evidence

- rules.diff hunk `@@ -1286,1445 +1284,10 @@` (diff lines 947-2393): 1436 `-` lines, 1 `+` line = 1437 changed lines.
  - 1431 lines = 80 `viewname| SELECT ...` blocks (pg_aios ... pg_wait_events), all from the pg_views SELECT.
  - 6 lines = 5 removed pg_rules rows (pg_settings_n 2 lines, pg_settings_u 3 lines) + `+ERROR:  relation "pg_rules" does not exist`.
- The removed text is byte-identical to oracle self-check-serial/results/rules.out lines 1289-2727 (diff'd, no differences).
- Gres actual rules.out 1284-1289: pg_views SELECT returns 0 rows (next statement follows immediately, no error); pg_rules SELECT errors.
- Not a cascade: filter is `schemaname='pg_catalog'`; nothing earlier in rules.sql can change it. Zero planner lines.

## What exists today

- `crates/pgexec/src/catalog_rel.rs:2902 pg_views_rows` -> `crabka_pgcatalog::list_views(kv)` (crates/pgcatalog/src/lib.rs:3230) -> user views only, definition via `catalog_fn::view_definition_text` -> viewdef.rs `write_query` (line 141). System views never appear.
- Two virtual-relation registries: exec.rs `virtual_table()` (19853), `virtual_table_names()` (22320), `virtual_relation_oid` (22681), `virtual_pg_class_properties` (20573) for pg_settings/pg_roles/pg_user/pg_prepared_statements/...; catalog_rel.rs RELATION_NAMES/system_view_oid for pg_indexes/pg_locks/pg_matviews/pg_policies/pg_replication_slots/pg_shmem_allocations_numa/pg_stat_activity/pg_tables/pg_views. Only 13 names are reported as relkind 'v'.
- 67 of the 80 oracle view names do not exist as relations in Gres at all (pg_stats, pg_shadow, pg_group, pg_rules, pg_cursors, pg_prepared_xacts, pg_seclabels, pg_sequences, pg_stat_* (all), pg_statio_*, pg_timezone_*, pg_user_mappings, pg_wait_events, pg_config, pg_file_settings, pg_hba_file_rules, ...). Other tests hit these directly: stats_import (pg_stats x30), stats (pg_stat_io x21, pg_stat_all_tables, pg_stat_user_functions, pg_stat_slru, pg_stat_database), portals (pg_cursors x9), prepared_xacts (pg_prepared_xacts x9), publication (pg_publication_tables x7), vacuum (pg_stat_user_tables x6), sysviews (pg_timezone_abbrevs, pg_backend_memory_contexts).
- No `pg_rules` anywhere in crates (grep). CREATE RULE is not parsed at all: `syntax error at position 7: expected Keyword(Table), found Ident("rule")` (Gres rules.out 1814); `CREATE OR REPLACE RULE` -> `expected Keyword(Table), found Keyword(Or)`.

## Corrections / additions to the claim

1. Fix location: catalog_rel.rs pg_views_rows is right for the row source, but the "register as real views" route also has to retire/redirect the exec.rs registry entries (`virtual_table`, `virtual_table_names`, `virtual_pg_class_properties`, `virtual_relation_oid`) for pg_settings/pg_roles/pg_user/pg_prepared_statements, otherwise the names collide. pg_rules needs a new relation (catalog_rel.rs RELATION_NAMES + system_view_oid + columns + rows) plus a `pg_get_ruledef` renderer (catalog_fn.rs), which does not exist.
2. Size: the honest route (80 views bootstrapped from system_views.sql, each executable) is XXL not XL: it needs dozens of set-returning functions (pg_get_aios, pg_show_all_settings, pg_stat_get_activity, pg_stat_get_*, pg_cursor, pg_prepared_statement, pg_prepared_xact, pg_hba_file_rules, pg_get_shmem_allocations, pg_timezone_names, pg_get_wait_events, pg_available_extensions, ...) and catalogs (pg_statistic, pg_seclabel, pg_shseclabel, pg_user_mapping, pg_foreign_server, pg_auth_members, ...). Canned-text route (a `&[(&str, &str)]` table of the 80 definitions + owner) is M and reaches byte-exactness on its own, but leaves 67 pg_views rows pointing at relations that cannot be queried, and does not help the other tests that query those views.
3. Missed prerequisites:
   - `ORDER BY viewname` result is C/byte order (pg_stat_xact_user_tables < pg_statio_all_indexes < pg_stats). Gres text sort must be bytewise (it is today for C-collation; note only).
   - Deparse route needs: function-in-FROM with column-alias list (`pg_get_aios() pg_get_aios(pid, ...)`), `'v'::"char"` casts, parenthesised boolean targets `(x.extname IS NOT NULL) AS installed`, nested `(a LEFT JOIN b ON ((...)))` layout, CASE, LATERAL, ARRAY subscripts, `::regclass`, `ANY (ARRAY[...])`.
   - pg_rules rows for pg_settings need two rule objects attached to a virtual relation (pg_settings), rule storage in pg_rewrite (catalog_rel.rs already registers pg_rewrite), and ruledef text with `ON UPDATE TO pg_catalog.pg_settings` (schema-qualified), `DO  SELECT` (two spaces), `'f'` printed as `false`, and `AS set_config` output alias.
4. Dependencies: rule system (parser + executor + pg_rewrite rows + pg_get_ruledef) is a hard dependency for the 6 pg_rules lines only; the 1431 pg_views lines do not depend on it. viewdef fidelity is only a dependency for the deparse route.
5. Unblocked statements fail longer: none within this hunk (both SELECTs are terminal). Downstream, once pg_rules exists the `SELECT ... FROM pg_rules WHERE tablename='hats'` blocks (Gres rules.out 1816, 1830, 1844, 1864, 1895) still fail until CREATE RULE works (rule-system root).

## Oracle facts

Confirmed against rules.out: `\a\t` unaligned tuples-only; 80 rows; rule text exact as quoted in the claim.
