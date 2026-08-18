# Verification: dml-partition-expression-keys

Verdict: root cause CONFIRMED, attribution CONFIRMED (633 vs 623), fix location PARTLY WRONG (error.rs not needed; parser, catalog_fn, bound-coercion, DROP/RENAME COLUMN guards, MINVALUE validation missing), dependencies INCOMPLETE.

## 1. Root cause

Refusal lives at `crates/pgexec/src/partition.rs:1156-1162` (`expression_key_error`), called from `key_columns` (1114-1140) when `PartitionKeyElem.column` is `None`. The parser (`crates/pgparser/src/parser.rs:7451-7461`, `opt_partition_by`) parses the expression with `self.expr(0)` and DISCARDS the AST, keeping only `text`.

First failing statement per block:
- insert.diff: 302 `(a, (b+0))`, 354 `lower(a)`, 691 `((b+0))` (mlparted1), 851 `((a+0))` (key_desc), 887 `(a, abs(b), c)` (mcrparted). insert runs before create_table (test 62 vs 68) so no cross-test cascade.
- update.diff: 271 `abs(d)` (part_c_100_200) -> part_d_1_15/part_d_15_20 never exist -> `:init_range_parted` fails "no partition of relation part_b_10_b_20 found for row (c) = (105)" -> range_parted empty for the rest of the section. BUT the section is doubly blocked: the column-order refusal ("UPDATE over a partitioned table is not supported when a partition declares its columns in a different order") fires independently at diff lines 366, 385, 396, 458 (part_c_1_100 / part_b_20_b_30 are attached with permuted columns and that succeeds).
- copy.diff: 84 `LIST((id % 2))`.
- merge.diff: no refusal in merge itself. part1..part4 "already exists" (950-959, 1114-1123) plus "child table is missing column tid" (1127-1133) and INSERT no-partition (966-967, 1136-1137) come from create_table.out line 796: `DROP TABLE parted, list_parted, range_parted, list_parted2, range_parted2, range_parted3;` -> `ERROR: table "range_parted3" does not exist` (range_parted3 = `PARTITION BY RANGE (a, (b+1))` refused). The DROP is atomic in Gres, so range_parted2 (part0..part4, range2_default), list_parted2, parted, list_parted, range_parted all leak. Blast radius beyond merge: alter_table.diff 3430 (list_parted2 already exists), 3461/3478/3493 (part1..3), 3695 (range_parted2); triggers.diff 1285 (parted already exists, ~51 lines); inherit.diff 3394 (range_parted already exists).

## 2. Fix location

Correct: partition.rs `Scheme` (128), `key_columns` (1114), `key_values` (708), `key_description` (769), `route` (873), `satisfies` (895), `serialize_scheme`/`deserialize_scheme` (258-281, SCHEME_VERSION 2 -> 3), `key_column_type` (1166).
exec.rs: `route_row_to_leaf` (17476) and `check_partition_constraint` (5047) only delegate; they need an EvalCtx/scope threaded through so `key_values` can evaluate expressions. `describe_row` (4988) is NOT involved (whole-row DETAIL already exact). `may_describe_key` (5013) says "no column-level grants in this engine" -- stale: privilege.rs:42/1127 store column grants; needs SELECT-on-every-key-column check for the key_desc block.
error.rs 1494-1512: NO change needed; wording already exact ("Partition key of the failing row contains {key}.", "Failing row contains {row}.").

Missing:
- crates/pgparser/src/ast.rs `PartitionKeyElem` (2394) must carry the `Expr`; parser.rs 7459 must keep it.
- exec.rs `resolve_partition_bound` (~25655-25720) `key_type` closure: bound coercion needs `eval::infer_type(expr, scope_of_parent_columns)` (eval.rs:4154) for expression keys.
- catalog_fn.rs `part_key_def` (2008-2040): pg_get_partkeydef must deparse expressions with PG's looks_like_function paren rule ("LIST (lower(a))", "RANGE (a, (b + 0))"); reuse viewdef.rs `expr_text` (1093).
- exec.rs DROP COLUMN guard (~29912-29928) and RENAME COLUMN rewrite (~30170-30185): match keys by name; must inspect column refs inside expressions (alter_table: "cannot drop column b because it is part of the partition key").
- exec.rs `pg_partitioned_table_rows` (~20315): partattrs 0 + partexprs.
- exec.rs `reject_incomplete_partitioned_key` (25610): PG "unsupported PRIMARY KEY constraint with partition key definition" DETAIL "... cannot be used when partition keys include expressions." (indexing).
- MINVALUE/MAXVALUE ordering check absent (no "must also be MINVALUE" anywhere in crates/); belongs near partition.rs `check_bound_shape`/`check_range_not_empty`; the LINE caret needs source positions for `RangeBoundValue` (ast.rs 2449 has none) -> parser positions or session.rs attach_* re-lex pattern (session.rs ~14980-15200).
- Key-expression classification for create_table messages (set-returning, aggregate, window, subquery, constant, pseudo-type record/unknown, non-IMMUTABLE function).

## 3. Attribution (whole-block rule)

- insert: 354 (ranges 297-607, 650-667, 690-701, 709-723, 726-742, 750-760, 766-768, 776, 784-794, 817-827, 834-844, 850-884, 886-990 of insert.diff).
- update: 250 expression-key direct+cascade; separately 28 column-order, 30 EXPLAIN (planner), 8 upview correlated-subquery, 14 pg_get_partition_constraintdef, 3 row-type.
- copy: 12. merge: 17.
- Total 633 vs analyst 623 (within 2%).
- Outside the cluster (same root): create_table 78 (16 validation + 18 range_parted3 + 43 range_parted4 + 1 DROP), triggers ~114 (51 leak + 63 refusals), alter_table ~40 (leak + (a+b+1)), inherit leak, indexing 4, generated_stored/virtual, partition_join/prune/aggregate.

## 4. Dependencies / fail longer

Not planner. Prereqs missed: parser AST change; SCHEME_VERSION bump; EvalCtx on the routing path; infer_type for bounds; column-level grant read in may_describe_key; deparse; MINVALUE validation + caret; DROP/RENAME COLUMN reference tracking.
Fail longer: update -> column-order root blocks nearly all of the 250 lines; \d+ of partitions -> pg_get_partition_constraintdef with expression rendering ("(abs(a) IS NOT NULL)"); insert mlparted -> target-first check order + BR-trigger row; key_desc -> column-privilege rule; merge -> MERGE-into-partitioned root; hash_parted -> hash function mismatch (hpart3 11).

## 5. Oracle facts

All verified in insert.out / update.out: 23514 wording, DETAIL forms "(a, (b + 0)) = (a, 11)", "((b + 0)) = (5)", "(a, abs(b), c) = (null, null, null)"; MINVALUE/MAXVALUE errors with LINE caret at the first offending value; "\d+ list_parted" -> "Partition key: LIST (lower(a))" and ", PARTITIONED" suffixes; suppression rule matches ExecBuildSlotPartitionKeyDescription (table SELECT, else every key column must be a plain column with column SELECT).
