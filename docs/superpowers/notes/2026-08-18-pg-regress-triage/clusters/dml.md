# DML cluster triage (insert, insert_conflict, update, merge, returning, copy, copy2, copydml, copyencoding, truncate)

Total changed lines: 3,955 (insert 630, insert_conflict 424, update 538, merge 984, returning 608, copy 45, copy2 506, copydml 58, copyencoding 4, truncate 158). Counting rule: every in-hunk +/- line except the two file headers; whole change blocks attributed to the producing root; verified per file with hunks.py/blocks.py in this directory.

Planner-only estimate: insert 0, insert_conflict 5, update 49, merge ~158 (58 inside EXPLAIN MERGE blocks, 14 FULL JOIN row order, ~6 trigger-notice order, ~80 inside explain_merge() ANALYZE blocks once the SRF-in-select-list cascade is unblocked), returning ~95, copy* 0, truncate 0.

## Per-file first failing statement / cascade
- insert: `insert into inserttest (col1, col2, col3) values (DEFAULT, DEFAULT, DEFAULT);` -> missing DETAIL "Failing row contains (null, null, testing)."; partial cascade: `partition by range (a, (b+0))` refusal poisons ~354 lines; composite arrays refusal poisons the composite section.
- insert_conflict: `create unique index both_index_expr_key on insertconflicttest(key, lower(fruit) collate "C" text_pattern_ops);`; partial cascade.
- update: `UPDATE update_test t SET t.b = t.b + 10 WHERE t.a = 10;` (LINE/HINT missing); partial cascade: `PARTITION BY range (abs(d))` refusal empties range_parted for the row-movement section (241 lines).
- merge: `DROP TABLE IF EXISTS target;` (NOTICE missing); partial cascade: NOT MATCHED INSERT `VALUES (t.tid, s.delta)` accepted -> persistent (NULL,40) row contaminates ~40 later blocks; part1..part4 leaked from create_table.
- returning: `UPDATE foo SET f3 = f3*2 FROM int4_tbl i ... RETURNING` then row order (updated row not moved to end); partial cascade: CREATE RULE refusal poisons 147 lines.
- copy: `copy parted_copytest from :'filename';` -> trigger function requires a session executor.
- copy2: `COPY x (a, b, c, d, e) from stdin;` -> same; table x stays empty (118 lines).
- copydml: `create rule qqq ...` (52/58 lines RULE).
- copyencoding: `SET client_encoding TO LATIN1;`.
- truncate: `TRUNCATE TABLE truncate_a CASCADE;` NOTICE missing; partial cascade: CREATE TABLE with nextval() default refused (68 lines); FK on partitioned table (65).

## Largest roots (lines across the ten files)
partition expression keys 623; MERGE into partitioned tables 240; RULEs 223; RETURNING OLD/NEW 195; EXPLAIN MERGE 145 (87 non-planner) + EXPLAIN ON CONFLICT 129 (124 non-planner) + EXPLAIN VERBOSE DML / temp-view lookup 86; COPY FROM triggers 146; SRF in SELECT list 136; MERGE clause scope 135; CREATE TABLE default evaluated at DDL time 128; pg_get_partition_constraintdef 112; multi-column SET 110; COPY ON_ERROR 106; ON CONFLICT target grammar 102; INSERT ... AS alias 86; MERGE grammar 86; INSERT target indirection 68; whole-row x.* 73; partitioned UPDATE column-order refusal 67; FK on partitioned tables 65; SQL-function body deparse 62; error cursor 58. Full list with fix locations is in the structured output returned to the orchestrator.

## Brief corrections
1. insert_conflict/merge "explain-ish" lines are mostly missing EXPLAIN *renderer* output (Conflict Resolution/Arbiter Indexes/Filter, "Merge on t", VERBOSE Output, JSON ModifyTable), not planner choice; planner-only ~5 and ~58.
2. update "~0 explain-ish": 49 lines are EXPLAIN and planner-only.
3. copy2 "40 gres-error lines": 102 +ERROR lines; largest cost (132) is one root (COPY FROM cannot fire plpgsql triggers).
4. insert: 354/630 lines are one root (expression partition keys), which also leaks into merge (part1..part4 already exist).
5. Gres already supports RETURNING old.*/new.* for single-table INSERT/DELETE; the gap is bare old/new, old/new in subqueries, MERGE RETURNING, partitioned targets.
6. session.rs precheck_copy_from claims "cannot copy to view" must precede CopyInResponse; PG raises it in CopyFrom() after ReceiveCopyBegin (expected file shows no leaked "test1"), whereas the duplicate-column check must precede it and does not.
