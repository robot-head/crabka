# Verification: sec-explain-syntactic-shape (rowsecurity)

Materials: diffs/rowsecurity.diff (124 hunks, 2215 changed lines), oracle
self-check-serial/results/rowsecurity.out, gres-serial/results/rowsecurity.out,
crates/pgexec/src/{explain.rs,session.rs,exec.rs,query.rs,rls.rs,privilege.rs}.
Per-statement counts: rowsec-stmt-counts.txt (script count_rowsec.py).

## 1. Root cause

Confirmed. `explain.rs::plan_statement` (line 77) is a pure AST walk (module
doc, lines 1-13). `session.rs::explain` (5176) calls
`exec::describe_statement` first, then `explain::plan_statement`, and never
consults policies, inheritance, partitions, views, prepared statements, or
privileges. Subqueries are stubbed as text: `Expr::ScalarSubquery => "(SubPlan)"`,
`InSubquery => "(x IN SubPlan)"` (explain.rs 806-820). `ExecuteStatement` falls
into `utility_node_type` -> "Result" (114-119).

Not a cascade. Each EXPLAIN block is an independent statement. Two producer
defects do feed some blocks:
- `ALTER TABLE t3 INHERIT t1;` -> `ALTER TABLE subcommand is not supported`
  (hunk 11; exec.rs 28979 `Action::Unsupported`). Every Append-over-t1 block
  expects three children (t1_1, t2_1, t3_1); with the fix to explain.rs alone
  Gres would print two. 228 lines depend on this.
- `WHERE CURRENT OF` (parser, hunks 104/105, 19 lines) and the `<<<` operator
  (lexer/CREATE OPERATOR, hunk 117, 7 lines) block three EXPLAINs entirely.

Three EXPLAINs error before explain.rs is reached, inside
`exec::describe_statement` (exec.rs 25126):
- `EXPLAIN SELECT (SELECT x FROM s1 LIMIT 1) xx, * FROM s2 ...` -> `column "x"
  does not exist` (query.rs 108: `resolve_types_in_projection_with_ctes` sees no
  outer scope) — 14 lines.
- `EXPLAIN UPDATE t2 t2_1 ... FROM t2 t2_2 ... RETURNING *, t2_1, t2_2` ->
  `column "t2_1" does not exist` (exec.rs 25185 `Scope::single(&table,
  &table.name.name)`: bare table name, no alias, no FROM/USING) — 12 + 26 lines.
- `EXPLAIN UPDATE/DELETE bv1` -> `relation "regress_rls_schema.bv1" does not
  exist` (exec.rs 25168 `get_table` on a view) — 16 lines.
The statements themselves execute correctly; only Describe/EXPLAIN fail.

## 2. Fix location

explain.rs symbols exist at the named lines (plan_statement 77, plan_select 162,
plan_from 225, scan_node 298, render_text_node 871). session.rs explain 5176
exists. rls.rs: the enforcement fold `combine_policy_quals` (796) builds
`((FALSE OR p1) OR p2) AND r1 AND r2`; PostgreSQL prints restrictive quals first
(sorted by name, each a separate AND conjunct), then ONE permissive OR whose
members are in REVERSE name order (relcache lcons order: `((a % 4) = 0) OR
((a % 3) = 0) OR ((a % 2) = 0)` for p1,p2,p3), no FALSE seed. So the deparse
cannot literally reuse the fold; it needs `applicable_policies` (743) with
names+permissive flags exposed. Additional fix locations the analyst missed:
exec.rs describe_statement/describe_returning (25126/25159), subquery.rs
resolve_types_in_projection_with_ctes, pgparser AlterTableAction + exec.rs
28979 for `ALTER TABLE ... INHERIT`.

## 3. Attribution (whole-block rule)

All EXPLAIN-statement changed lines in rowsecurity: ~1082.
- Pure shape, this root, no other dependency: 463
  (9a 6, 15 7, 30 15, 36 36, 42c 6, 43a 13, 45a 6, 48 28, 60-63 z1+plancache_test
  60, 64-80 rls_view 179, 84/85 16, 86 12, 89 27, 90 11, 92 7, 97/98 18, 120 16)
- Shape but needs `ALTER TABLE INHERIT` for the third child: 228
  (12 29, 13 14, 14 45, 17 15, 19 17, 37 14, 39 15, 40 14, 41 14, 42a/b 33, 45b 18)
- Blocks that can ONLY be matched with a planner (Index Scan in InitPlan on
  never-analyzed uaccount, Hash Join, Materialize, EC-derived quals, join
  order): 365 (6 28, 7 28, 9b 11, 21/23/24/25/27 80, 43b/c 39, 43d 12, 44 26,
  plancache_test2/3 112, 87 29). Most of the text in these blocks is still
  produced by this root; the analyst's ~66 is a per-line count of the residue.
- Blocked by parser/lexer producers: 26 (104 9, 105 10, 117 7).
This root: 691 (analyst 628, +10%). Note the analyst's hunk numbers appear
0-based relative to the diff (their #59-79 = rls_view hunks 60-80).

## 4. Dependencies missed
- ALTER TABLE INHERIT (228 lines).
- PostgreSQL qual ordering (order_qual_clauses): security level via
  contain_leaked_vars + per-clause cost (procost * cpu_operator_cost), stable
  insertion sort. Evidence: `Filter: (f_leak(b) AND (a = 1))` (84),
  `(f_leak('abc'::text) AND (RLS OR))` (86), `((a > 0) AND (a = 4) AND ((a % 2)
  = 0) AND f_leak(b))` (48), `((a <= 2) AND ((a % 2) = 0))` (37). Needs
  leakproof flags for built-in operators; routine cost/leakproof exist
  (pgcatalog routine.rs 262-263).
- Plan-time partition pruning by constant RLS qual (`cid < 55` -> only
  part_document_fiction, printed as `Seq Scan on part_document_fiction
  part_document`) hunks 24/25 (26 lines).
- Security-barrier subquery rules: never pulled up; leakproof outer quals pushed
  down; trivial Subquery Scan removed -> `Seq Scan on y1` (84/85) vs `Subquery
  Scan on bv1` kept when the outer qual is f_leak (48).
- Const folding: default-deny FALSE -> `Result / One-Time Filter: false`;
  `ROW(1,1,1)` -> `'(1,1,1)'::record`; alias renumbering (t1_1, t1_1_1, s2_1,
  rls_tbl_1); Result over Append only for projecting UPDATE (`SET b = b || b`);
  SubPlan numbering inner-first, `hashed` rule; InitPlan for uncorrelated.
- EXPLAIN permission checks incl. relations inside policy quals under the
  view owner's rights (z1_blacklist).
Fail-longer: after this root, 365 lines wait for a planner; 228 wait for
INHERIT; SELECT-result blocks around them still fail on `public.fipshash(x::text)`
-> "function fipshash(unknown) is not unique" (separate root).

## 5. Oracle facts
All stated PG behaviours confirmed in the oracle .out, with two nuances:
permissive policies are OR-ed in reverse name order (not name order); a
security_barrier view prints Subquery Scan only when the outer qual cannot be
pushed down. `SELECT * FROM t1 FOR SHARE` -> `relation "t1" does not exist`
(hunk 14, 25 lines) is a locking-read defect (exec.rs execute_read_locking),
not EXPLAIN; cause not traced.

Size: XL rather than L — this is the resolved-plan renderer foundation every
EXPLAIN-heavy file needs, plus three describe-path fixes and ~8 rule families.
