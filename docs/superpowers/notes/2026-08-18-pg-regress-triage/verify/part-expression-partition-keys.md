# Verification: part-expression-partition-keys

Verdict: root cause CONFIRMED, fix locations CONFIRMED but INCOMPLETE, attribution
CONFIRMED (my count 1629 vs analyst 1475, +10%), dependency list WRONG
(the named dependency is not needed; several real prerequisites are missing).

## 1. Root cause

Every block starts with `CREATE TABLE ... PARTITION BY <strategy> (<expr>)` and Gres's
0A000 from `partition::expression_key_error` (crates/pgexec/src/partition.rs:1156, called
from `key_columns` at :1127, called from `exec::partition_scheme_from_ast` at exec.rs:25602).
All following statements on the table are 42P01.

First failing statement in each block (diff line -> SQL):
- partition_prune 859 mc3p `range (a, abs(b), c)`; 1506 iboolpart `list ((not a))`;
  2167 coll_pruning_multi `range (substr(a, 1) collate "POSIX", substr(a, 1) collate "C")`;
  3823 mc3p again (`range (a, abs(b), c)`).
- partition_join 723 prt1_e `RANGE(((a + b)/2))`, 741 prt2_e, 1486 prt1_m `RANGE(a, ((a + b)/2))`,
  1498 prt2_m, 1569 plt1_e `LIST(ltrim(c, 'A'))`, 1744 pht1_e `HASH(ltrim(c, 'A'))`.
- inherit 2800 mcrparted `range (a, abs(b), c)`.
- partition_aggregate 870 pagg_tab_m `RANGE(a, ((a+b)/2))`.
- indexing 841/845 idxpart `range ((b + a))` with PK / UNIQUE.
Not a cascade from anything earlier: plain multi-column range keys (mc2p) create fine in
the same file (gres partition_prune.out:455-462).

Cascade tail worth noting: partition_prune oracle line 1895
`drop table lp, coll_pruning, rlp, mc3p, mc2p, ...` fails in Gres (`table "mc3p" does not
exist`), so 12 other tables survive; no later 42P07 results from that in this file (1 line).

## 2. Fix locations

All named symbols exist at the stated lines:
- partition.rs: `SCHEME_VERSION` (:55, =2), `Scheme { keys: Vec<String> }` (:128),
  `serialize_scheme` (:258), `key_ordinals` (:691), `key_values` (:708),
  `key_description` (:769), `key_columns` (:1114), `expression_key_error` (:1156),
  `key_column_type` (:1166), `route` (:873), `satisfies` (:895).
- exec.rs: `pg_partitioned_table_rows` (:20315), `partition_scheme_from_ast` (:25588),
  `resolve_partition_bound` (:25660).
- catalog_fn.rs: `part_key_def` (:2019) for `pg_get_partkeydef`.

State today: the PARSER ACCEPTS the syntax (parser.rs:7436 `opt_partition_by`); it parses
the expression with `self.expr(0)` and DISCARDS the AST, keeping only `PartitionKeyElem.text`
(ast.rs:2394). The executor refuses with 0A000. So: parsed, refused, never executed.

Locations the analyst missed (real decision points):
- crates/pgparser/src/parser.rs `opt_partition_by`: `substr(a, 1) collate "POSIX"` is
  swallowed by `self.expr(0)` (the error text proves it), so `collation` is None and the
  COLLATE ends up inside the expression. PG's grammar (`part_elem`) parses func_expr /
  '(' a_expr ')' then opt_collate then opt opclass; partcollation is separate. Needed for
  pg_get_partkeydef ("substr(a, 1) COLLATE \"POSIX\"") and pruning matching.
- exec.rs `reject_partition_key_column` (~:29912) tests `scheme.keys.iter().any(|k| k == column)`;
  must use `expression_reads_column` (exec.rs ~:30329 pattern) so DROP COLUMN / ALTER TYPE
  on a column inside an expression key is refused (alter_table oracle expects it for (a+b+1)).
- exec.rs RENAME COLUMN rewrite (~:30178) rewrites key names only; expression sources need
  `rewrite_identifier_tokens` (index-expression pattern at :30201).
- exec.rs `reject_incomplete_partitioned_key` (:25617): with an expression key + PK/UNIQUE the
  oracle expects 0A000 "unsupported PRIMARY KEY constraint with partition key definition" +
  DETAIL "PRIMARY KEY constraints cannot be used when partition keys include expressions."
  (indexing 6 lines).
- viewdef.rs `expression_text` (:243) is the PG-faithful deparser (pg_get_expr tests exist);
  pg_get_partkeydef must reuse it: non-function expressions get an extra paren pair
  `RANGE (((a + 1)), substr(b, 1, 5))` (create_table oracle :323), function calls bare
  `LIST (lower(a))` (insert oracle :569); DETAIL line uses single parens `(a, (b + 0))`
  (insert oracle :306). Opclass printed only when non-default (`a oid_ops`), collation only when
  non-default (`d COLLATE "C"`, `c collate "default"` dropped) (create_table :314).
- eval.rs `infer_type` (:4154) gives the expression result type the refusal says is missing.
- `key_values`/`route`/`satisfies`/`key_description` need an EvalCtx + Table (not `columns`)
  to evaluate the expression per row (`cluster_sort_key` at exec.rs:9109 is the template:
  `parse_expression(source)` + `Scope::single(table, ...)` + `eval::eval`).
- Validation rules PG applies at parse analysis, all exercised by create_table (not in this
  cluster's 5 files but same root): SRF, aggregate, subquery, constant expression (42P17),
  pseudo-type record/unknown, IMMUTABLE functions, generated column, system column,
  whole-row Var `((partitioned))` treated as a plain Var.
- Storage: SCHEME_VERSION 2 -> 3 (greenfield: no shim). Suggested shape:
  `keys: Vec<PartitionKey { source: Column(String) | Expression(String), collation, opclass }>`.

## 3. Attribution (whole-block, oracle-line ranges via count_by_oracle.py)

| file | ranges (oracle lines) | lines |
|---|---|---:|
| partition_prune | 693-964, 1645-1721, 1895-1896, 3459-3517, 1279-1447, 1808-1846 | 586 |
| partition_join | 754-964, 1075-1381, 1462-1525, 1543-1604, 1687-1742, 2381-2430 | 720 (692 without the two "memory budget" statements, 28) |
| inherit | 3142-3242, 3272-3370, 3508-3544 | 218 |
| partition_aggregate | 854-959 | 99 |
| indexing | 2 blocks x 3 | 6 |
| total | | 1629 |

Analyst: 1475. Difference is partition_join (561 vs 720): their block splitter assigns the
minus-side rows of a SELECT that follows an EXPLAIN to the EXPLAIN block, so error blocks
were undercounted. Within 30%.

Same root outside this cluster (not counted by the analyst; other clusters should charge it):
create_table 14 CREATEs (validation rules + \d output), insert 5 tables (~335 lines:
range_parted (b+0), list_parted lower(a), mlparted1 (b+0), key_desc (a+0), mcrparted abs(b)),
update part_c_100_200 abs(d) (~388 lines cascade), triggers 4 CREATEs, generated_stored /
generated_virtual 4 each (expected error is "cannot use generated column in partition key"),
copy (id % 2), alter_table (a+b+1).

## 4. Dependencies

- `part-hash-opclass-support-function`: NOT needed. The only hash expression key in these
  files is pht1_e `HASH(ltrim(c, 'A'))` -> text; partition/hash.rs already hashes
  `Datum::Text` with hash_bytes_extended (:92) exactly as pht1 `HASH(c)` does. That root is
  about test_setup custom opclasses on hp/hp_prefix_test, unrelated.
- Fail longer after the fix:
  * every EXPLAIN in the unblocked blocks -> part-explain-append-prune (Gres prints
    `Seq Scan on mc2p` with no Append even for plain keys; pruning must also match quals
    against the key EXPRESSION, e.g. `abs(b) < 1`);
  * join order / qual order in `mc2p t1, lateral (... mc3p ...)` (Aggregate before Append,
    `(a = t1.b) AND (c = 1) AND (abs(b) = 1)` reordering) -> planner;
  * partition_join partitionwise joins, inherit Merge Append + Index Scans,
    partition_aggregate partial aggregates -> planner;
  * plt1_e/pht1_e 3-way GROUP BY joins -> 53200 memory budget (scanner.rs:1136). Note the
    EXPLAIN of the same query says 42P01 while the SELECT says 53200: relation resolution
    happens after scanning starts (plt1 x plt2 materialised before plt1_e is opened).

## 5. Oracle facts

Correct: DDL accepted silently; bounds coerced to expression type; hash uses the type's
hash opclass; `Partition key: RANGE (a, abs(b), c)` for function-call keys. Refinements: none
of the 5 files prints `\d` on an expression-keyed table (checked); the deparse rules
(double parens for operator expressions, COLLATE/opclass printing) come from create_table
and insert.
