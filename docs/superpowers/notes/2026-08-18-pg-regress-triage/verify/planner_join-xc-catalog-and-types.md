# Verification: planner_join-xc-catalog-and-types

Verdict: root causes confirmed as symptoms, but the claim is a grab-bag of nine
unrelated roots. Three fix locations are wrong (relation rowtypes, pg_temp
functions, current_timestamp precision), one is incomplete (composite oids
in setops), and the line count is overstated (~390 whole-block vs 700 claimed;
only ~100 flip to pass without the planner).

## Per-item findings (evidence: diffs + source)

### 1. information_schema domains (cardinal_number, sql_identifier)
- join.diff 4231-4268 (38) + 4276-4280 (5); equivclass 480 (1) + 483-489 (7,
  cascade "relation overview does not exist"); subselect 1344-1353 (10) +
  1359-1364 (6). Total 67.
- Origin: pgparser parser.rs:612 `parse_type_name` resolves a schema-qualified
  type through `crabka_pgtypes::usertype::column_type_for_name_in(schema,name)`
  (process-wide registry hydrated from `crabka_pgcatalog::list_user_types`,
  pgcatalog/src/lib.rs:6220). Nothing seeds the five information_schema
  domains. Fix: seed them at catalog bootstrap (pgcatalog lib.rs bootstrap +
  usertype registry), not catalog_fn.rs/catalog_rel.rs.
- Fail longer: join 38-line block and subselect 10-line block are EXPLAINs
  needing Hash Join/Index Scan (planner); equivclass EXPLAIN needs view
  inlining + sort elimination + `'foo'::name` deparse. Only join 5-line and
  subselect 6-line result blocks flip.

### 2. information_schema.column_udt_usage
- join.diff 4646-4653 (8). catalog_rel.rs INFORMATION_SCHEMA_RELATIONS
  (line ~165) lacks it; views are Rust row generators. Fail longer: expected
  plan `Result / One-Time Filter: false` (planner constant folding of `on null`).
- Hidden: select_parallel line 1399 `information_schema.foreign_data_wrapper_options`
  currently masked by an aborted transaction (4 more lines when unblocked).

### 3. Table row types as types (mki8/mki4)  -- FIX LOCATION WRONG
- join.diff 4926-4972: 39 lines (analyst said 68).
- routine.rs:468 `resolve_type` ALREADY accepts a relation name for RETURNS
  (get_table/get_view lookup). The failure is the SQL body
  `$$select row($1,$2)::int8_tbl$$`, validated at CREATE (check_function_bodies)
  and parsed by parser.rs:612 `parse_type_name`, which only knows the user-type
  registry; relation composite types are never registered (scope.rs:1299,
  eval.rs:4199, exec.rs:14892 all say "relation's composite type is not
  registered in pg_type"). rowtypes.out:405 and create_view.out show the same
  error. This is a systemic root: relation rowtypes as first-class types
  (register on CREATE/ALTER/DROP TABLE|VIEW, pg_type row with typrelid, parser
  resolution). Size L, not part of a "catalog grab-bag".
- Fail longer: the two EXPLAIN blocks want `Function Scan on mki8 / Function
  Call: '(1,2)'::int8_tbl` = SQL-function inlining + const-folding (planner
  territory) and Gres prints a bare `Function Scan`.

### 4. varbit / composite oids in EXPLAIN (union)  -- LOCATION RIGHT BUT INCOMPLETE
- union.diff: 270-280 (11), 284-294 (11) oid 1562; 573-583 (11), 585-592 (8)
  oid 300236 (ct1). 41 lines. exec.rs:24973 `column_type_from_oid` has no
  BIT/VARBIT arm (trivial) and cannot resolve a user composite oid because it
  is oid-only with no catalog; caller is exec.rs:19566
  `build_table_expr_schema_with_ctes` (Derived table schema path) which
  round-trips through FieldDescription. Fix: add BIT/VARBIT arms and use
  `crabka_pgtypes::usertype::column_type_for_oid` fallback (registry) for
  user types.
- Fail longer: 3 EXPLAIN blocks want `Unique -> Sort -> Append -> Values Scan on
  "*VALUES*_1"` (planner: sort-based dedupe for non-hashable type; Gres prints
  HashAggregate + Subquery Scan). Only the 8-line ct1 SELECT flips.

### 5. arrays of varbit (union 371-381, 383-390 = 19)
- pgtypes datum.rs:261 `ElemType` has no Bit/VarBit variant
  (`from_column_type` returns None at datum.rs:427). Needs new ElemType
  variants + `code()`/`from_code` (datum.rs:563/594) = row-encoding change.
  Fail longer for the EXPLAIN block (planner Sort/Unique); 8-line SELECT flips.

### 6. jsonb #- (explain 479-645, 167 lines)
- lexer.rs:926 lexes `#>`/`#>>`/`##` but not `#-` -> falls to `#` + unary `-`.
  `jsonb_delete_path` already exists (json_fn.rs:1925). Fix: lexer token +
  ast BinaryOp + parser.rs:1092 precedence + eval dispatch. Size S.
- Fail longer massively: the block is `explain (analyze, verbose, buffers,
  format json) select * from tenk1 order by tenthous` under
  max_parallel_workers_per_gather=4 => Gather Merge/Sort/Parallel Seq Scan
  JSON with Buffers/Planning/Triggers sections. Also explain_filter_to_json is a
  plpgsql loop over EXECUTE of EXPLAIN. `#-` owns 2 lines of the 167.

### 7. pg_temp functions (explain 651 = 1, + 653-660 = 8)  -- LOCATION WRONG
- parser.rs:14319 `routine_name` rejects any qualifier but `public` with
  3F000 at parse time; routines have no schema in the catalog (Routine.name is
  a bare String). Fix = schema-qualified routines (parser + pgcatalog Routine
  identity + routine.rs lookup via search_path + EXPLAIN verbose printing
  `pg_temp.mysin(t1.f1)`). Size L. The 8-line follow-on block also needs SRF in
  select list (explain_filter) and `Seq Scan on pg_temp.t1` schema
  qualification in EXPLAIN verbose.

### 8. current_timestamp(0) (join 7898-7903 = 6, 8031-8035 = 5)  -- LOCATION WRONG
- Not func.rs: datetime_fn.rs:66 maps current_timestamp to
  DtFunc::TransactionTimestamp and :127/:~200 `require_arity(fc, n == 0)`.
  Fix: accept optional precision (0..=6) for current_timestamp/current_time/
  localtimestamp/localtime and round. Size S.
- The two EXPLAIN blocks (12+12) are planner (self-join elimination
  `Seq Scan on sj j2 Filter: ((b IS NOT NULL) ...)`) + deparse
  (`EXTRACT(dow FROM CURRENT_TIMESTAMP(0)) / '15'::numeric`); Gres printed a
  plan there, so they are NOT this root.

### 9. pg_stats / pg_stat_database / pg_stat_force_next_flush
- join 8925-8928 (4): pg_stats missing (catalog_rel.rs has pg_stat_activity
  only). Expected output is itself an ERROR about column atts.relid, so an
  empty pg_stats with PG's column list suffices.
- select_parallel 8-15 (8), 20 (1), 1567-1574 (8), 1579-1584 (6) = 23.
  pg_stat_force_next_flush: no builtin. pg_stat_database: no relation.
  Fail longer: the last block expects `t | t` (parallel_workers_launched
  grew) => needs parallel execution or a fake counter tied to Gather nodes.

### 10. int -> text assignment cast (subselect 335, 338 = 2)
- exec.rs:12839 catch-all: comment says "int <-> text keeps erroring with
  42804". PG: pg_cast has int4->text castcontext 'a' (I/O conversion casts to
  string types are assignment-level). exec.rs:12763 already routes
  Varchar/Char through cast_assign_in but not Text. Fix in pgtypes cast.rs:314
  `assignment_cast_allowed` (any -> string type) or the exec.rs arm. Size S.
- The shipped_view result blocks are a CREATE RULE cascade, not this.

## Recount (whole-block)
join 105, equivclass 8, subselect 18, union 60, select_parallel 23,
explain 176 => ~390 (analyst 700, +79%). Lines that flip to pass with only
these fixes and no planner: ~100 (join 5+24+11+4, equivclass 0, subselect 8,
union 16, select_parallel 17, explain 0).

## Brief corrections
- Claim says the mki8 hunk costs 68 lines; it is 39.
- "routine.rs return-type resolution" is not the fix: RETURNS int8_tbl already
  resolves; the parser's `::int8_tbl` cast does not.
- func.rs is not where current_timestamp lives; datetime_fn.rs is.
- pg_temp functions is a schema-qualified-routines subsystem, not a schema
  lookup.
