# Verification: agg_window-recursive-cte-shape-search-cycle

File checked: with (diff = 1755 changed lines, 105 hunks).

## Verdict

Root cause CONFIRMED. Fix locations MOSTLY confirmed, with two corrections
(DML forward references live in exec.rs::execute_write_parts, not only cte.rs;
SEARCH/CYCLE viewdef printing needs viewdef.rs). Attribution reasonable:
my whole-block count is 922 (R 349 + SEARCH/CYCLE 573) vs the analyst's 883.
The 39 line gap is the y-state consequence blocks after the DML forward-ref
failure (hunk 77 second block 19, hunks 97/98 16), which the analyst left out.

## Hunk attribution (whole-block rule)

R = recursive shape / lazy LIMIT / validation text / DML forward-ref /
CREATE RECURSIVE VIEW. SC = SEARCH/CYCLE.

| hunks | lines | root |
|---|---|---|
| 0 | 23 | R: CREATE [OR REPLACE] RECURSIVE VIEW (first failing statement of the file, not a cascade) |
| 1 | 30 | R: infinite UNION ALL / UNION + LIMIT 10 -> 'stack depth limit exceeded' (cte.rs:380 MAX_RECURSION_ITERATIONS) |
| 2 | 12 | R: nested WITH inside parenthesised recursive term (outermost/innermost1..6) |
| 3-9 | 152 | self-referential FK `REFERENCES department` fails at CREATE TABLE -> cascade (NOT this root); includes vsubdepartment view + 2 pg_get_viewdef (57) |
| 10 | 6 | viewdef alias t_1 (viewdef root) |
| 11,12 | 66 | R: `union all (with x as (select * from q) select * from x)` + LIMIT 24/32 (infinite -> also needs lazy LIMIT); DOUBLE-BLOCKED by department FK |
| 13,14,16 | 41 | tree self-ref FK cascade |
| 15 | 7 | parser `count(t2.*)` (+ tree cascade) |
| 17,18 | 46 | planner (Index Only Scan, Hash/Merge Semi Join, CTE node) |
| 19-32 | 166 | SC SEARCH (19,22,25,27 = 84 lines are EXPLAIN VERBOSE -> planner-only) |
| 33,34 | 4 | SC validation text ('right side of the UNION must be a SELECT', 'must be at the top level of its right-hand SELECT') |
| 35 | 35 | SC v_search view + pg_get_viewdef (needs viewdef.rs SEARCH printing) |
| 36-38 | 90 | arrays of record (ElemType has no Record) -> arrays root, NOT this root |
| 39-48 | 228 | SC CYCLE (39,42 = 39 lines EXPLAIN -> planner-only) |
| 49-58 | 35 | SC CYCLE syntax errors + v_cycle1 create |
| 59 | 105 | SC v_cycle2 + 2 viewdefs + 2 selects |
| 60 | 16 | R 4 ('must not appear within its non-recursive term'); LINE-cursor 12 |
| 61 | 16 | R 16 (within a subquery 4; non-recursive-term 4; ORDER BY (SELECT n FROM x) 4; WITH-prefixed DELETE inside CTE body = parser 4) |
| 62 | 26 | R 10 (ORDER BY / OFFSET / FOR UPDATE texts); LINE-cursor 16 |
| 63,65,66 | 12 | R ('more than once', 'within EXCEPT') |
| 64 | 2 | LINE-cursor |
| 67 | 10 | R 5 (numeric(3,0) typmod in message); LINE/HINT 3; rules 2 |
| 68 | 16 | parser `((select ...)) q` 6; ?column? naming + int4_tbl extra row 10 |
| 69 | 5 | outer-level aggregate / nested CTE |
| 70 | 15 | planner |
| 71 | 60 | R: WITH-prefixed body (`WITH RECURSIVE s ... SELECT i FROM s UNION ALL SELECT j+1 FROM t`) |
| 72 | 4 | R: DETAIL/HINT on 42P01 'There is a WITH item named "outermost"...' |
| 73 | 11 | R (7 rows + 'within a subquery' 4) |
| 74 | 2 | column naming `array` |
| 75,76 | 24 | R: iter (nested WITH with window fn inside recursive term) |
| 77 | 73 | R 21 (forward ref 'relation "t2" does not exist') + R 19 (y state consequence; will fail longer on heap physical order) ; rules 33 |
| 78,79,80 | 46 | rules |
| 81,83 | 23 | planner |
| 82,84 | 20 | unused scalar-subquery not evaluated by PG (t_cte 'more than one row') |
| 85-88 | 134 | y state cascade (rules DO INSTEAD + forward-ref) |
| 89,90 | 8 | DML-CTE semantics (ON CONFLICT dup row, EXCLUDED.*) |
| 91 | 5 | parser `UPDATE SET (k,v) = (SELECT ...)` |
| 92-95 | 99 | MERGE with CTE |
| 96,97,98 | 22 | R: DML forward ref t1->t2 + y/yy state consequences |
| 99,100 | 4 | inheritance ordering |
| 101 | 41 | planner |
| 102 | 6 | R: 'must not contain data-modifying statements' vs 'relation "t" does not exist' |
| 103 | 48 | R 4; DML-CTE message 4; rules 36; LINE 4 |
| 104 | 2 | LINE |

Totals: R 349, SC 573, sum 922. Whole file: 1755 (checks).

## Fix-location findings

- cte.rs:304-306 `split_recursive_terms` rejects `query.with`; :307-315 requires
  a top-level SetOp::Union; `check_recursive_term`:533-541 requires the recursive
  term to be a plain SELECT (rejects Nested/parenthesised set-op). Confirmed.
- cte.rs:329-333 `evaluate_recursive_cte` ORDER BY/LIMIT/OFFSET text; :336-340
  and `appended_columns`:521-526 refuse SEARCH/CYCLE with 0A000; parser already
  parses both clauses into `Cte.search` / `Cte.cycle` (parser.rs:12768-12822).
- cte.rs:380 MAX_RECURSION_ITERATIONS=100_000 -> ExecError::StackDepthExceeded is
  the 'stack depth limit exceeded'. Evaluation is fully materialised
  (`evaluate_with_clause` -> Relation with all rows), so LIMIT can never stop
  the fixpoint. query.rs::query_to_relation_with_ctes evaluates the WITH list
  before it looks at q.limit.
- DML forward references: cte.rs:164-169 `cte_references` returns false for
  CteBody::Dml, AND the data-modifying path is exec.rs::execute_write_parts
  (~4362) which iterates `with.ctes` in list order and never calls
  `evaluation_order`. Analyst named only cte.rs. Also 'recursive query "t" must
  not contain data-modifying statements' must be raised there.
- CREATE RECURSIVE VIEW: parser.rs create_statement dispatch (~5558) and
  create_view (9259) have no RECURSIVE arm; PG rewrites to
  `WITH RECURSIVE name(cols) AS (query) SELECT cols FROM name`.
- WITH-prefixed DML inside a CTE body: parser.rs parse_with_clause ~12735
  `starts_dml_statement()` only peeks the first token; top-level `statement()`
  (3488) already handles WITH+DML, the CTE-body branch does not.
- viewdef.rs ~194-230 prints CTE list without SEARCH/CYCLE -> needed for
  pg_get_viewdef('v_search'/'v_cycle1'/'v_cycle2') (about 61 lines of SC).
- arrays of record: pgtypes datum.rs:430 `ElemType::from_column_type` returns
  None for ColumnType::Record -> SEARCH DEPTH FIRST seq (record[]) and CYCLE
  path (record[]) are blocked; BREADTH FIRST seq is a single record (supported).

## Oracle facts

All quoted messages verified in with.out. LIMIT+OFFSET case reports OFFSET first
(checked). FOR UPDATE case: 'FOR UPDATE/SHARE in a recursive query is not
implemented' (Gres: 'FOR UPDATE is not allowed with UNION/INTERSECT/EXCEPT').

## Fail-longer after the fix

- Blocks whose only remaining diff is `LINE n: ... ^` cursor lines (~50 in file).
- y-state SELECTs without ORDER BY (hunk 77 second block, 85-88) reproduce PG
  heap physical order (0..6,11,7,12,...) -> unmatched without heap emulation.
- SC EXPLAIN blocks (123 lines) need the planner + EXPLAIN VERBOSE renderer.
- Hunks 11/12 also need self-referential FK CREATE TABLE.
- Hunk 67 numeric(3,0) needs typmod preserved on the seed scope type name.
