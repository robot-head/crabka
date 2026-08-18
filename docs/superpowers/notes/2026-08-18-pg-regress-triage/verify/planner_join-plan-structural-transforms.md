# Verification: planner_join-plan-structural-transforms

Verdict: root cause CONFIRMED, fix location PARTLY WRONG (missing prerequisites), attribution PLAUSIBLE (my strict count 611, range 611-1016), size UNDERESTIMATED unless a bound-tree IR is delivered elsewhere.

## 1. Root cause (confirmed with evidence)

- explain.rs is a syntactic AST walk: session.rs:5203-5206 calls `crate::explain::plan_statement(statement)` on the raw `Statement`; there is no planner directory (`ls crates/pgexec/src/planner` -> none). Execution goes through `query.rs::query_to_relation` -> `exec::select_to_relation_with_ctes` (direct interpretation).
- explain.rs:225-249 `plan_from`: for `many` FROM items, builds left-deep `Nested Loop` and puts the ENTIRE WHERE on the top node as `Join Filter` (line 244). Confirmed by join hunk 197/198 (`Join Filter: (((t1.a = t2.a) AND (t1.b = 1)) AND (t2.b = 2))`).
- explain.rs:271-281: every `TableExpr::Derived` prints `Subquery Scan on <alias>` (union h35: `Subquery Scan on c / Filter: (t = 2) / Append` vs oracle `Seq Scan on tenk1 b`).
- explain.rs:494-508 `deparse_with`: `Binary` prints `(l op r)` recursively -> nested AND parens, no flattening.
- explain.rs:619-622: `Expr::Column` prints the qualifier only if written (`qualify` flag) -> `ff = x1` where PG prints `ec1.ff = ec2.x1` (equivclass h1) and `Filter: (ctid <= ctid)` for a lateral outer ref (tidrangescan h12).
- explain.rs:735 `Expr::InList` prints `IN (1, 2)`; PG's parse analysis makes ScalarArrayOp `= ANY ('{1,2}'::integer[])` (incremental_sort diff line 315/317). NOTE: this transform is parse-analysis (transformAExprIn), not planner.
- predicate h1-h4: `Filter: (a IS NULL)` vs `Result / One-Time Filter: false`; `Filter: (a IS NOT NULL)` vs bare `Seq Scan on pred_tab t`. pgcatalog `Column.not_null` exists (pgcatalog/src/lib.rs:170), so the NOT NULL fact is available.
- join h166 (analyst "165"): SJE `Seq Scan on sj q / Filter: ((a IS NOT NULL) AND (b = (a - 1)))` vs Gres 2-way Nested Loop. join h147/h134/h154: LEFT JOIN removal to unique inner (`Seq Scan on parent p` alone). join h149/h150: `p.k = 1 and p.k = 2` -> `Result / One-Time Filter: false` (EC contradiction; NOT in the analyst's list). join h113/h136/h137/h195/h226: `ON false` reduction. join h167/h196: EXISTS -> semi join (Gres prints `Filter: (EXISTS SubPlan)`).
- equivclass h10: `x = x` -> `x IS NOT NULL` confirmed (6 lines).
- No cascade for the cited blocks: predicate's first hunk is the first EXPLAIN; join/equivclass/union cited blocks are self-contained EXPLAINs. equivclass h12 (8 lines) IS a cascade from a missing `information_schema.sql_identifier` type -> not this root.

## 2. Fix location

- `crates/pgexec/src/planner/` does not exist; explain.rs has no bound tree to work on. The claim's symbol names are PG's, fine as a target, but the REAL prerequisite is a bound query representation (range table + Var(varno, attno) + type + nullability + join tree). Gres has only `scope.rs` (flat ColumnBinding list per relation) and `bind.rs` (rewrites `Expr::Column` into `$pos.N` positional refs for evaluation) -- no Query/RTE/Var IR that both explain and executor share. Neither the claim nor its one dependency names this IR as a deliverable.
- explain.rs:282-290 `TableExpr::Join { left, right, .. }` IGNORES `kind` and `constraint` (ast.rs:3345-3350 has both). Result: no `Left Join`/`Full Join`/`Semi Join`/`Anti Join` suffix ever prints and every ON qual is silently dropped (predicate h5: oracle `Nested Loop Left Join`, Gres `Nested Loop` with no filter; join h218: ON `a.q2 = ss.q1` vanishes). 196 minus-lines in join, 18 in predicate, 27 in subselect carry an outer-join node name. This is a hidden prerequisite for nearly every outer-join block in the claim's files and is not covered by the claim text.
- Correction: IN-list -> ScalarArrayOp and AND-list flattening are parse-analysis facts in PG (transformAExprIn / transformBoolExpr); they can be done in the deparser or a bind pass, no planner needed.
- Executor side: subselect h37 shows the pulled-up constant qual `tattle(9, 8)` is evaluated ONCE by PG (One-Time Filter; 1 NOTICE) and 6 times by Gres (6 NOTICEs) -> the transforms are observable outside EXPLAIN; the executor must mirror at least the const-qual gating (exec.rs select path), else NOTICE lines fail after EXPLAIN matches.

## 3. Attribution (whole-block rule)

Total changed lines in the 10 files: 12,789. Crude classifier (expected side has no Hash/Merge/Index/Bitmap/Materialize/Memoize/Sort/Incremental/Tid/Gather node) -> 1,121 candidate lines: join 847, subselect 135, predicate 60, union 35, select_distinct 23, select_distinct_on 15, equivclass 6, tidrangescan 0, incremental_sort 0, tuplesort 0.

Strict (block fixed by this root's transforms + trivial rendering only):
- join: h40 12, h88 21, h113 19, h120 22, h121 24, h134 14, h136 23, h137 23, h147 10, h149 12, h150 14, h154 13, h164 10, h166 12, h167 8, h170 13, h171 38, h173 22, h176 10, h177 12, h179 12, h180 18, h183 42, h195 18, h197 10, h198 10, h199 61 = 503
- predicate h1-h4 = 30 (h11/h12 = 30 more need inheritance Append expansion)
- union h35 12 + h43 15 = 27
- select_distinct h8-h10 = 17, select_distinct_on h2 = 15 (DISTINCT-on-const -> Limit; rule-based, arguable)
- equivclass h10 = 6
- subselect h29 = 13 (VALUES alias numbering + SubPlan render, mixed)
- tidrangescan 0 (h12's 13 lines also need Tid Range Scan), incremental_sort 0 (every block needs Incremental Sort/Presorted Key), tuplesort 0 (Index Scan for ORDER BY LIMIT, memory-budget errors, EXPLAIN DECLARE CURSOR -> `Result`).
Strict total ~611. Mixed blocks whose dominant defect is a transform but which also need VERBOSE Output/InitPlan/PHV rendering, LockRows, Function Scan naming, or inheritance: join h82/86/87/89/152/201/208/218/219/221/224/226/228/232 = 344, predicate h11/12 = 30, subselect h43 = 18, tidrangescan h12 = 13 -> upper bound ~1,016. Analyst's 900 is inside the range; strict number 611 (analyst high by ~47% on the strict reading, ~10% low on the generous reading). Also many PLANNER-tagged blocks (e.g. join h196 needs Index Scan AND EXISTS pull-up; every equivclass h1-h9 block needs Index Cond AND qual distribution) require these transforms as well but are correctly not counted here.

## 4. Dependencies missed
1. Bound query tree IR (RTE list, Var identity, nullability, join tree) -- does not exist; prerequisite for every transform and for the Var-qualification rule.
2. Join-kind suffix + ON-qual rendering in explain.rs Join arm (kind/constraint dropped).
3. Equivalence-class deduction (derive const quals, detect contradictions `p.k=1 AND p.k=2`, `ff = x1 AND ff = 42` -> `x1 = 42`) -- appears in join h149/150 and equivclass h1-h9, not in the claim's transform list.
4. Unique-index/PK proofs for join removal and SJE (catalog has indexes; need a rel_is_unique helper).
5. Executor gating for One-Time Filter / const-false so NOTICE counts match (subselect h37).
6. Fail-longer: after the transforms, unblocked blocks still need Materialize/Memoize/Index/Merge placement (predicate h5-h10,h13,h14 = 148 lines all have `Materialize` or `Merge Full Join`), VERBOSE Output/InitPlan rendering, inheritance/partition Append expansion (predicate h11/12, join h164, subselect h22).

## 5. Oracle facts
All stated facts check out against the .out/diff text: `Filter:` on the scan for single-rel quals; `Join Filter:` for multi-rel quals (and for outer-join ON quals that cannot be pushed, e.g. `Join Filter: (t1.a = 1)` predicate h7); flat `(a AND b AND c)`; `Result / One-Time Filter: false`; `Nested Loop Left Join / Join Filter: false / -> Result / One-Time Filter: false` (predicate diff lines 128-133); `_1.._n` suffixes (`ec1_1..ec1_6`, `pred_parent_1/2`, `t_1`, `"*VALUES*_1"`).

## Brief corrections
- files_affected: tuplesort contributes 0 lines to this root; incremental_sort 0 standalone (all blocks planner-bound); tidrangescan at most 13 mixed lines.
- The analyst's hunk numbers are off by one vs. a plain `@@` count (their "join hunk 165" is my 166, "union hunk 34" is my 35).
- Size: XL is only defensible if the bound-tree IR and join-kind rendering come from another root; standalone this is XXL (PG's prepjointree.c + initsplan.c + analyzejoins.c + equivclass.c + clauses.c subset).
- Shared-scratchpad hazard hit again: analysis/verify/hunks.py was overwritten by another agent mid-run; I moved my scripts to analysis/verify/pjst/.
