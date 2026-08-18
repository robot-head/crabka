# indexes_storage cluster — root-cause triage (read-only)

20 files, 5530 changed lines (counting rule: every in-hunk +/- line except the two file headers).
Tally per file/root: `tally.json` (produced by `classify.py`, whole-block attribution, then hand-checked).

## Per-file summary

| file | lines | first failing statement | cascade | primary root | planner-only est. |
|---|---|---|---|---|---|
| create_index | 1489 | `CREATE INDEX IF NOT EXISTS ON onek USING btree(unique1 int4_ops);` (PG: syntax error, Gres accepts) | no (sections cascade locally) | PLANNER (833+28) | 861 |
| create_index_spgist | 799 | `SELECT count(*) FROM radix_text_tbl WHERE t ~<~ 'Aztec…'` (lexer) | no | PLANNER (546) | 546 |
| index_including | 270 | `CREATE INDEX tbl_include_reg_idx ON tbl_include_reg (c1, c2) INCLUDE (c3, c4);` | yes | include-columns (256) | ~20 hidden |
| index_including_gist | 83 | `CREATE INDEX tbl_gist_idx ON tbl_gist using gist (c4) INCLUDE (c1,c2,c3);` | yes | include-columns (83) | ~12 hidden |
| btree_index | 281 | `CREATE INDEX bt_name_index ON bt_name_heap USING btree (seqno name_ops);` | no | PLANNER (266) | 266 |
| brin | 148 | `INSERT INTO brintest SELECT … FROM tenk1 ORDER BY unique2 LIMIT 100;` (memory budget) then `CREATE INDEX brinidx … USING brin` | yes | brin-am (117) + brin functions (25) | ~20 hidden (+DO-loop WARNINGs) |
| brin_multi | 280 | `INSERT INTO brintest_multi SELECT … fipshash(unique1::text) …` (UDF overload) | yes | brin functions (132) + brin-am (108) | ~100 hidden |
| brin_bloom | 121 | `INSERT INTO brintest_bloom SELECT … LIMIT 100;` (memory budget) | yes | brin-am (96) | ~10 hidden |
| gin | 134 | `create index gin_test_idx on gin_test_tbl using gin (i) with (fastupdate = on, …)` | yes | PLANNER (84+20) / gin generic opclasses (30) | 104 |
| gist | 138 | `drop index gist_pointidx2, gist_pointidx3, gist_pointidx4;` | no | PLANNER (101) | 101 |
| spgist | 12 | `explain (costs off) select * from spgist_domain_tbl where f1 = 'fo';` | no | PLANNER | 12 |
| hash_index | 13 | `CREATE INDEX hash_name_index ON hash_name_heap USING hash (random name_ops);` | no | partial-index (10) | 8 |
| hash_func | 184 | `SELECT v as value, hashint2(v)::bit(32) …` | no | hash functions (184) | 0 |
| amutils | 180 | `select prop, pg_indexam_has_property(a.oid, prop) …` | no | index property functions (178) | 0 |
| reloptions | 104 | `SELECT reloptions FROM pg_class WHERE oid = 'reloptions_test'::regclass;` | no | reloptions storage (96) | 0 |
| tablespace | 527 | `ALTER TABLESPACE regress_tblspace RESET (random_page_cost = 2.0);` | no | index pg_attribute rows (345) | 0 |
| indirect_toast | 75 | `SET default_toast_compression = 'pglz';` then `make_tuple_indirect(indtoasttest)` | partial (trigger) | make_tuple_indirect (73) | 0 |
| compression_1 | 245 | `SET default_toast_compression = 'pglz';` then `CREATE TABLE cmdata(f1 text COMPRESSION pglz);` | yes | column compression (230) | 0 |
| tablesample | 178 | `SELECT t.id FROM test_tablesample AS t TABLESAMPLE SYSTEM (50) REPEATABLE (0);` | no | tablesample (178) | ~10 |
| random | 269 | `SELECT ks_test_uniform_random() OR …` | no | random PRNG (263) | 0 |

## Root totals (whole-block attribution across the 20 files)

```
1841 planner (+36 rowset-order rows that only an index-scan execution order reproduces, +20 gin recheck table) => 1897
 372 cat-index-pg-attribute-rows      (\d <index> prints no column rows: pg_attribute has no rows for user indexes)
 344 idx-include-columns
 323 idx-brin-am                        + 169 fn-brin-summarize (brin_summarize_new_values/_range, brin_desummarize_range)
 282 fn-random-pg-prng                  (+6 KS-test failures, cause not established)
 233 guc-toast-compression-and-column-compression
 184 fn-hash-functions
 181 lex-pattern-and-startswith-ops     (~<~ ~<=~ ~>=~ ~>~ ^@)
 178 fn-index-property-functions
 178 exec-tablesample-pg-faithful
 177 explain-typed-deparse-and-nodes    (cross-cluster: typed Const deparse, OR flattening, IN->=ANY, SubPlan/InitPlan/WindowAgg/Output:/Function Scan alias)
 153 cat-pg-partition-tree-and-partition-fns
  98 cat-reloptions-storage
  87 ddl-alter-table-add-constraint-using-index
  79 ddl-tablespace-semantics
  73 fn-make-tuple-indirect
  69 idx-reindex-effects
  55 idx-gin-generic-opclasses (30 after moving the recheck table to planner)
  48 cat-pg-describe-object-and-depend
  44 idx-concurrently-txn-block-and-invalid
  42 idx-partial-index
  37 idx-partitioned-index-cascade (+8 ALTER INDEX ATTACH PARTITION)
  31 idx-nulls-not-distinct
  25 type-name-opclass-binary-coercible
  24 exec-udf-overload-opaque-args
  15 ddl-alter-index-alter-column
  23 idx-opclass-params-syntax (22 + 1 opclass-on-expression-key)
  17 ddl-drop-index-multi-and-concurrently
  12 cat-pg-statistic
   9 ddl-toast-relations-and-pg-toast
  small: expression-index deparse 8, insert-select sort budget 7, index comment 6, replica identity 6, drop-cascade report 6,
  syscol refusal 5, DESC/NULLS FIRST keys 5, unique expression index 4, pg_my_temp_schema 4, IF NOT EXISTS unnamed 3, textcat 3,
  expression index w/ user fn 3, plpgsql FOR implicit lateral 3, timestamp/date range 3, point-in-polygon 2, extended stats 2,
  CASE/subquery column name 2, not-null DETAIL 2, pg_relation_size 2, UPDATE pg_class 1
```

## Notes on cascades ("unblocked statements fail longer")

* index_including / index_including_gist: once INCLUDE parses and persists, the SELECTs need `pg_index.indnkeyatts`, `indclass` (1978), `pg_get_indexdef`/`pg_get_constraintdef` INCLUDE text, `\d` "Key?" column (needs pg_attribute rows for indexes), unique enforcement on the key columns only, ALTER TABLE ADD PRIMARY KEY/UNIQUE ... INCLUDE, EXCLUDE ... INCLUDE, and the AM refusals (`access method "brin" does not support included columns`, `NOTICE: substituting access method "gist" for obsolete method "rtree"`). EXPLAIN blocks then need the planner (Index Only Scan using covering / Bitmap Index Scan on covering with `Index Cond: (ROW(c1, c2) <= ROW(2, 5))`).
* brin*: after `USING brin` is accepted the DO blocks run; they compare `EXPLAIN` text to `Bitmap Heap Scan` and RAISE WARNING otherwise — with a syntactic EXPLAIN this produces one WARNING line per operator (hundreds of hidden lines) until the planner picks BRIN bitmap scans. brin_multi additionally needs `fipshash(unique1::text)` resolved (UDF overload with column args) before its rows exist.
* gin: after array/jsonb GIN opclasses exist, the queries need `gin_clean_pending_list`, `gin_fuzzy_search_limit`, and the planner (Bitmap Heap Scan + `Rows Removed by Index Recheck` parsed by execute_text_query_index).
* create_index REINDEX section: after `pg_partition_tree` and `ALTER INDEX ... ATTACH PARTITION` work, `compare_relfilenode_part` needs REINDEX to change `pg_class.relfilenode` of leaf indexes only.
* compression_1: after `COMPRESSION` parses, statements need `pg_column_compression`, `\d+` Compression column, `ALTER ... SET COMPRESSION`, `LIKE INCLUDING COMPRESSION`, inheritance conflict NOTICE/ERROR, `default_toast_compression` GUC with `HINT: Available values: pglz.` (the *_1 variant is the no-lz4 build; Gres may target either variant, the no-lz4 one is cheaper).
* tablespace: `\d <index>` blocks (345 lines) are all the pg_attribute-for-indexes gap; the rest is REINDEX (TABLESPACE) moves + relfilenode, partition index cascade, default_tablespace on partitions, pg_global refusals, ALTER ... ALL IN TABLESPACE, GRANT ON TABLESPACE, and one leaked TEMP table (`test_io_local` from stats.sql, moved into regress_tblspace) that makes every "relname in tablespace" query and the final DROP TABLESPACE wrong.

(Full root list with fix locations, oracle facts and sizes is in the StructuredOutput returned to the orchestrator.)
