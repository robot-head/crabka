# Partitioning cluster triage (2026-08-17, certified main run)

Files: partition_join 4199, partition_prune 4035, partition_aggregate 1201, partition_info 312,
hash_part 92, indexing 703, inherit 2157 = 12,699 changed lines.

Method: every diff was split into change blocks (runs of +/- lines), each block tied to the
statement that produced it and attributed whole to one root. Blocks whose expected side is an
EXPLAIN plan were marked cost-dependent when the expected plan holds a node only a cost-based
planner chooses (Index/Bitmap scans, Hash/Merge/Nested Loop choice, Memoize, Materialize,
Gather/Parallel, Partial/Finalize, Merge Append, HashAggregate vs GroupAggregate). Scripts:
blocks_lib.py, attrib_*.py in this directory.

## Headline numbers per file

| file | total | planner-only (cost) | deterministic partition-plan-shape | producer cascades | other |
|---|---:|---:|---:|---:|---:|
| partition_join | 4199 | 4002 (3611 direct + 391 hidden behind prt1_e/prt1_m/plt1_e/pht1_e) | 25 | 561 expr-key cascade (170 rows/DDL) | 2 |
| partition_prune | 4035 | 1008 (519 direct + 365 behind SRF-in-select-list + 70 expr keys + 54 MERGE parse) | 2347 | 589 expr keys, 365 SRF, 97 MERGE-USING-JOIN | ~130 (hash opclass 40, prepare-on-view 18, === lexing 14, INHERITS merge 14, hash array 10, scan order 6) |
| partition_aggregate | 1201 | 1130 (1070 + 60 hidden) | 26 | 99 expr keys | 6 |
| partition_info | 312 | 0 | 0 | 0 | pg_partition_tree/root 282; ALTER INDEX ATTACH + index ancestors 30 |
| hash_part | 92 | 0 | 0 | 0 | satisfies_hash_partition 74; VARIADIC args 18 |
| indexing | 703 | 36 | 0 | small | partitioned-index tree ~370, NOT NULL catalog 62, pg_get_indexdef LATERAL 55, error DETAIL 33, dropped-column placeholders 30, partial index 19, index naming 19, misc |
| inherit | 2157 | 741 (491 direct + 250 hidden) | 288 | drop-cascade orphans ~330, expr keys 220, INHERITS merge 112, ALTER INHERIT 73 | NOT NULL catalog 224, CHECK inheritance ~225, misc ~150 |

Cost-based planner lines: 6,917 of 12,699 (54%). Deterministic plan-shape lines needing a
catalog-aware plan tree with partition pruning but no cost model: 2,686 (21%). Everything else
(~3,100 lines, 25%) is DDL/DML/catalog/parser work.

## First failing statement per file
- partition_join: hunk 1 EXPLAIN join (Append of Hash Joins expected). Not a cascade. First error: CREATE TABLE prt1_e PARTITION BY RANGE(((a + b)/2)) 0A000 -> 561-line cascade.
- partition_prune: hunk 0 `explain (costs off) select * from lp;` (Append expected). Not a whole-file cascade. First error: create table mc3p ... (a, abs(b), c) -> 589-line cascade (mc3p x2, iboolpart, coll_pruning_multi).
- partition_aggregate: hunk 1 partitionwise aggregate plan. First error pagg_tab_m expr key (99 lines).
- partition_info: SELECT * FROM pg_partition_tree(NULL) -> 42883.
- hash_part: SELECT satisfies_hash_partition(0,4,0,NULL) -> function does not exist.
- indexing: pg_indexes indexdef lacks ON ONLY. Not a cascade.
- inherit: hunk 1 merge NOTICE count; cascade producer = `drop table some_tab cascade` (children survive -> 42P07 later).

## Brief corrections
1. partition_join / partition_prune are NOT cascades (86% / 71% are EXPLAIN over existing tables).
2. tableoid works; inherit is gated on DROP CASCADE of inheritance children, NOT NULL/CHECK catalog modelling, INHERITS column merge.
3. children_of = KV (length-first) order; partitions_of = name order; PG = OID order / bound order.
4. list_parted and range_parted pre-exist when inherit runs (left by another file): 16 lines not fixable here.
5. gres-error counts in the brief were capped at 100 (real: 123/63/238/148/…).
6. hp/hp_prefix_test hash rows depend on the test_setup custom hash opclasses; Gres drops the opclass at CREATE.

## Roots: see the StructuredOutput object (ids prefixed part-).
