# Cluster textsearch_fdw_replication — root-cause triage (2026-08-18)

Files: tsearch (1388), tsdicts (599), tstypes (489), foreign_data (1650), publication (1202), subscription (388). Total 5716 changed lines, all attributed (whole-block rule). Scripts: `classify.py`, `run_*.py` in this directory; block dumps in `*_blocks.json`.

## Per-file summary

| file | lines | first failing statement | cascade | planner-only | exact w/o planner |
|---|---|---|---|---|---|
| tsearch | 1388 | `SELECT oid, prsname FROM pg_ts_parser WHERE ...` -> `relation "pg_ts_parser" does not exist` | no (independent blocks; local cascade in the GiST siglen block) | 80 (10 EXPLAIN blocks: Index Scan / Bitmap Heap+Index Scan on wowidx/wowidx2 vs Seq Scan) | no |
| tsdicts | 599 | `CREATE TEXT SEARCH DICTIONARY ispell (Template=ispell, DictFile=ispell_sample, AffFile=ispell_sample)` -> `text search template "ispell" does not exist` | yes (every ts_lexize/config test on ispell/hunspell/synonym/thesaurus) | 0 | yes |
| tstypes | 489 | `SELECT $$'\\as' ab\c ab\\c AB\\\c ab\\\\c$$::tsvector` (backslash handling in tsvector I/O) | no | 0 | yes |
| foreign_data | 1650 | `DROP ROLE IF EXISTS a, b, ...` (parse) then `COMMENT ON FOREIGN DATA WRAPPER dummy IS 'useless'` (parse) then `CREATE FOREIGN DATA WRAPPER postgresql VALIDATOR postgresql_fdw_validator` (parse) | partial: ft1/ft2/foreign_schema.foreign_table_1 never get created (grammar) so ~440 lines are cascade | 0 | yes |
| publication | 1202 | `CREATE PUBLICATION testpub_default;` -> 0A000 `physical replication SQL is not supported` | yes (whole file) | 0 | yes |
| subscription | 388 | `CREATE ROLE regress_subscription_user3 IN ROLE pg_create_subscription` (predefined role missing) then `CREATE SUBSCRIPTION regress_testsub CONNECTION 'foo'` (parse) | yes (whole file) | 0 | yes |

## Attribution tally (lines)

tsearch: default-parser 389, headline 299, unknown-literal-tsargs 244, aux-functions 163, match-exec 96, PLANNER 80, websearch 70, rank 28, gist-opclass-options 19.
tsdicts: dictionaries 594, config-mapping-ddl 5.
tstypes: unknown-literal-tsargs 282, aux-functions 95, tsvector-tsquery-io 80, match-exec 32.
foreign_data: fdw-ddl-grammar 758 (318 direct parse errors + 440 cascade), fdw-catalogs-psql 613, fdw-object-semantics 159, foreign-table-relkind 36, set-role 28, set-statistics/storage 24, check-deparse 18, reassign/drop owned 7, alter-sequence 3, notnull-constraint-name 3, drop-role-multi 1.
publication: publication-ddl 1144 (539 catalog/psql, 390 parse, 154 semantics, 61 refusal), replica-identity 33, create-collation 12, set-role 5, on-conflict-partitioned 4, grant-on-database 2, pk-using-index 1, drop-role-multi 1.
subscription: subscription-ddl 374, predefined-roles 9, grant-on-database 4, set-role 1.

## Roots (full records in the StructuredOutput)

Text search: the whole subsystem is a placeholder. `crates/pgtypes/src/text_search.rs` (739 lines) is a simplified tsvector/tsquery with its own tokenizer, precedence printer and matcher; `crates/pgexec/src/text_search_fn.rs` splits on non-alphanumerics (`words()`), a hard-coded English stop list, `rust_stemmers`; `crates/pgexec/src/text_search_catalog.rs` stores only `name -> base` for configs/dictionaries; the parser (`parser.rs::alter_text_search`) skips every token of an `ALTER TEXT SEARCH ...` after the name. No default parser, no dictionaries beyond simple/snowball(english), no headline/rank algorithms, no ts_parse/ts_token_type/ts_debug/ts_stat/ts_rewrite/unnest(tsvector)/tsvector_to_array.

FDW: parser handles only the SP40 kafka_fdw subset (`create_fdw` OPTIONS only, `create_server` no TYPE/VERSION/IF NOT EXISTS, `create_user_mapping` no IF NOT EXISTS/USER, `create_foreign_table` no constraints/column OPTIONS/PARTITION OF/INHERITS/empty list, no `ALTER FOREIGN DATA WRAPPER`, no `ALTER FOREIGN TABLE`, ALTER SERVER/USER MAPPING refused 0A000, `GRANT ... ON FOREIGN DATA WRAPPER|FOREIGN SERVER` not parsed, `COMMENT ON FOREIGN ...` not parsed). Catalog structs (`crabka_pgcatalog::ForeignDataWrapper/ForeignServer/UserMapping`) hold name+options only (no owner/handler/validator/acl/type/version/oid); no pg_foreign_* relations in `catalog_rel.rs`; no dependency tracking; generic `object "x" already exists`/`does not exist` messages.

Publication/subscription: entirely absent; the parser recognizes only the bounded representative spellings in `NON_GOAL_REFUSALS` (`bounded_non_goal_refusal`) and refuses 0A000; anything else is a raw syntax error. `pg_publication*` relations exist in `catalog_rel.rs` but lack `pubgencols`; `pg_publication_tables`, `pg_subscription`, `pg_subscription_rel`, `pg_stat_subscription_stats` absent.

Shared defects surfaced here (owned by other clusters, sized for completeness): SET ROLE denied to a SET-SESSION-AUTHORIZATION superuser (`crabka_pgcatalog::role_can_set` only exempts BOOTSTRAP_ROLE); DROP ROLE name lists; ALTER SEQUENCE not parsed at all; REASSIGN OWNED/DROP OWNED not parsed; ALTER COLUMN SET STATISTICS/STORAGE/(n_distinct) refused; CHECK constraint deparse `CHECK ((c1 > 0))` w/o `::text` casts and w/o NO INHERIT; not-null constraint names derived (`{table}_{col}_not_null`) so RENAME COLUMN renames them; ALTER TABLE REPLICA IDENTITY refused; ADD PRIMARY KEY USING INDEX not parsed; CREATE COLLATION not parsed; GRANT ON DATABASE not parsed; INSERT ... ON CONFLICT on partitioned tables refused; no predefined roles (pg_create_subscription etc.); scalar built-in functions cannot be used in FROM (`srf::plan` closed registry).

## Brief corrections
- tsdicts "20 gres-error lines" and tstypes "30" in the brief are wrong; actual +ERROR counts are 96 and 60 (file_stats.json agrees).
- tsearch is not "~0 explain-ish": 10 EXPLAIN blocks (80 changed lines) expect Index/Bitmap scans on GiST/GIN (file_stats.json explain_lines 78).
- ALTER SERVER / ALTER USER MAPPING are matrix `Error-with-notice(0A000)`, not non-goals; CREATE/DROP FDW/SERVER/USER MAPPING/FOREIGN TABLE/IMPORT are `Implemented` (subset). ALTER FOREIGN DATA WRAPPER / ALTER FOREIGN TABLE are `Wave-assigned(P5)` and are not parsed at all.
- foreign_data is not a regress.so handler problem: `CREATE FUNCTION test_fdw_handler() ... LANGUAGE C` already succeeds in Gres.
- Only ~318 of foreign_data's 1650 lines are direct parse errors; ~440 are cascades of failed CREATE/ALTER FOREIGN TABLE and ~613 are missing catalogs/psql listings.
- publication asserts DML-time replica-identity checks, row-filter/column-list validation and DROP COLUMN dependencies (~150 lines), beyond DDL + psql listing.
- Several tsdicts statements silently "succeed" today (`ALTER TEXT SEARCH CONFIGURATION ... ALTER MAPPING`, `DROP MAPPING FOR not_a_token`) because the parser discards the clause.
