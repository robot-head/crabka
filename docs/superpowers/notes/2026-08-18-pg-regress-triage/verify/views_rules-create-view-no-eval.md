# Verification: views_rules-create-view-no-eval

Verdict: root cause CONFIRMED, attribution CONFIRMED (902 exact), fix location PARTLY WRONG,
dependency list WRONG (GEOMETRY already present; the real prerequisite is a generalised
regression-C adapter seam in routine.rs).

## 1. Root cause

- create_view.diff hunk 1 (line 33): first hunk of the file. Everything before matches, including
  `CREATE FUNCTION interpt_pp(path, path) RETURNS point AS :'regresslib' LANGUAGE C STRICT`
  (validate_c_routine accepts regress.so symbols from a static list; routine.rs:417).
- Gres output: `ERROR: cannot execute function interpt_pp(path,path): Gres has no c interpreter ...`
  raised by routine.rs `callable()` (2272) via `inline_scalar_call` (2589,2606), reached from
  `inline_scalar` (1825) called by subquery.rs `resolve_types_in_expr` (852), called by
  `crate::query::describe_query_expr` in the CreateView arm of exec.rs (1095). Not a cascade.
- select_views.diff hunk 1 (@@ -341,906 +341,7): `SELECT name, #thepath FROM iexit ORDER BY name
  COLLATE "C", 2;` -> `ERROR: relation "iexit" does not exist`. Pure cascade of the CREATE VIEW
  failure. The `street` view (path ?# path in WHERE) created and returned 333 rows that matched, so
  `?#` on path/path already works.

## 2. Fix location

- routine.rs `callable`/`inline_scalar_call`: exists, is where the CREATE VIEW error is decided.
  BUT deferring the refusal alone is not enough: the view body is expanded in exec.rs
  `build_base_table` (~17612-17660) via `crate::query::query_to_relation(&body_ctx, query)` which
  evaluates every target column for every row; there is no projection pruning through views. So
  the SELECT would then fail with the same "cannot execute" error at scan time.
- The named symbol `build_from_schema_of_select` (exec.rs 19360) is the describe path (schema only),
  not where the view projection is evaluated. Wrong symbol.
- Precedent that makes this S: routine.rs already has a pinned regression-C adapter seam
  (`RegressionCAdapter` 1694, `regression_c_adapter` 1715, `eval_regression_c_adapter` 1746) for
  test_pglz_compress/decompress. `inline_scalar` returns None for adapters (1848),
  `plpgsql_declared_call_type` returns the declared type for describe (1883-1900), and
  `eval_plpgsql_scalar_with` runs the adapter (2041-2063). Adding `InterptPp` there fixes both the
  CREATE VIEW (describe returns `point`) and the SELECT (evaluated per row).
- Hidden prerequisite: `has_exact_regression_c_signature` (1699) hardcodes the result type to
  Bytea; it must take the expected result type (Point).
- Geometry primitives exist: pgtypes/src/geometry.rs `Lseg::intersects` (207) and
  `Lseg::intersection_point` (213) are exactly lseg_intersect/lseg_interpt; regress.c interpt_pp
  (postgresql-18.4 src/test/regress/regress.c:94-130) is a two-loop over consecutive point pairs,
  first intersecting pair wins, NULL if none.
- `#path` (UnaryOp::NPoints, eval.rs 1139) exists; text sorts by byte value = COLLATE "C"
  (exec.rs 33058 comment), so `ORDER BY name COLLATE "C", 2` will match.

## 3. Attribution

create_view H1 = 1 line; select_views H1 = 901 lines (grep count of +/- lines in the hunk).
Total 902. Analyst number is exact.

## 4. Dependencies

- "GEOMETRY" is not required: path ?# path, #path, Lseg intersect/interpt all exist.
- Real dependency: generalise the regression-C adapter signature helper (result type param).
- Fail-longer: none expected for the SELECT itself if the adapter route is taken. If only the
  refusal is deferred (analyst's routine.rs-only fix), the SELECT still fails at view scan.

## 5. Oracle facts

Confirmed from oracle .out: CREATE VIEW iexit succeeds silently; the SELECT returns 896 rows
sorted by name (C) then npoints.
