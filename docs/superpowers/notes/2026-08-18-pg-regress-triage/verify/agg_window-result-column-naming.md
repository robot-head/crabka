# Verification: agg_window-result-column-naming

Verdict: root cause CONFIRMED as a symptom family, but the mechanism and the
fix location are wrong for ~78% of the lines. Attribution number is fine
(analyst 1057, my whole-block recount 1066). Size S is optimistic: three
separate seams -> S-M.

## 1. Root cause, per file (whole-block recount)

Three distinct mechanisms produce the `?column?`/wrong-name headers:

A. LATERAL outer-reference substitution (830 lines, 78%)
   groupingsets: 144 + 204 + 348 = 696 (hunks @802, @1404, @1764: cube/grouping
   sets lateral `select v.a, four, ten, count(*)` and bug_16784 `select a, i, j`)
   rangefuncs: 58 + 76 = 134 (hunk @1196: `LATERAL (SELECT r1, * FROM ...)`)
   Mechanism: exec.rs `LateralBinder::bind` -> `BindPass::expr` (exec.rs
   ~14810) rewrites every outer column ref inside the lateral item into
   `Expr::Const { value, ty }` BEFORE the derived table is built
   (`lateral_join` exec.rs 13745 -> `build_table_expr(&specialized)`), so the
   ordinary projection naming (`derived_name` at exec.rs 24850) sees a Const
   and returns `?column?`. `named_expr_inner` already returns the name for
   `Expr::Column`; adding arms there cannot fix this. Values are already
   correct, so the fix recovers these blocks completely.
   Fix: `BindPass::set_expr` (exec.rs ~14785): for each
   `SelectItem::Expr { expr, alias: None }` pin `alias = Some(derived_name(expr))`
   before `self.expr(expr, &inner)` substitutes. Precedent for exactly this pin
   already exists at exec.rs 16476-16480 (CorrelatedRowExprs marker). Same
   walk serves `lateral_schema_item` (exec.rs 19326) so Describe agrees.

B. `named_expr_inner` missing arms (~140 lines)
   groupingsets 34 (scalar subquery `grouping`/`count` 10, CASE 24)
   aggregates 38 (ARRAY(subq) 4, `min` scalar subq header 2, `count` 16, CASE 16)
   tsrf ~20 (scalar subquery whose target is generate_series, 2 blocks)
   with 6 (`foo`, `column1`, `array` headers)
   rowtypes 42 (FieldSelect `(q).c1`, `(row(1,2.0)).f1`, `(d).a`; `ROW()`;
   `(1.1,2.2)::complex` and `cast(row(..) as text)` -> `row`)
   Fix: exec.rs `named_expr_inner` (line 24919). Needed arms:
   ScalarSubquery -> name of the single inner target (recursive: inner
   select item alias / derived_name; VALUES -> column1); ArraySubquery ->
   "array"; Exists -> "exists"; Case -> name of ELSE result if strength 2 else
   "case"; Row -> ("row", 2) (strength 2 so it survives a cast);
   FieldSelect -> last field name (2); ArrayLiteral -> "array" (2).
   viewdef.rs 661 uses derived_name too, so `AS "row"` in pg_get_viewdef
   follows for free.

C. Function-scan (SQL-language function in FROM) column naming (~96 lines,
   all rangefuncs): `rngfunct(...) z` 10, `rngfunc_sql(...)` 16+16+24=56,
   getrngfunc3/4/5 4+4+4, getrngfunc1/2 `AS t1` 8+10.
   Mechanism: routine.rs `table_function_columns` (line 2951):
   `RoutineResult::Type {..} => Some(vec![routine.name.clone()])` renames only
   the FIRST body column to the function name and leaves the rest with the
   body's derived names; for a composite return (setof rngfunc2 /
   rngfunc_rescan_t) PG uses the composite's attribute names. And exec.rs
   17771-17780 (`crate::routine::expands_as_table` branch) does
   `column_aliases.clone().or(Some(names))` and never lets a bare table alias
   (`AS t1`) name a scalar function's single column. srf.rs
   `user_function_relation`/`qualify_columns` is only reached for plpgsql and
   already handles the alias -> not the fix location. `RoutineType.column` is
   None for a composite (pgcatalog/src/routine.rs 145), so the fix needs a
   catalog lookup of the composite's columns (needs `kv`).
   Also: the Describe/schema path (exec.rs 19576-19603) does not handle SQL
   functions at all (falls to srf::from_item_schema -> "function ... does not
   exist"), a separate root the naming fix must be mirrored into.

NOT naming (analyst included in the family, I exclude):
   - rangefuncs testrngfunc blocks (5 x `select * from testrngfunc()`, 42
     lines): body `select 7.136178319899999964, 7.136178319899999964` must be
     coerced to `rngfunc_type` (typmod numeric(35,6)/(35,2)); values are
     wrong too. Root: SQL function composite-return coercion. Naming alone
     leaves them failing.
   - rangefuncs `dup` (OUT/INOUT params): separate root.
   - tsrf `q1 | ?column?` @172: expected an ERROR (SRF in CASE), not a name.

## 2. Hidden prerequisite / fail-longer
   - select_into.out: `INSERT INTO int4_tbl SELECT 1 INTO f;` (and `CREATE VIEW
     foo AS SELECT 1 INTO int4_tbl;`) execute in Gres instead of erroring, so
     int4_tbl carries an extra row `1` for the rest of the schedule. The
     `with` @1723/@1738 and aggregates @758 blocks show 6 rows instead of 5;
     after the naming fix those blocks still fail until the SELECT INTO
     rejection defect is fixed.
   - rangefuncs `SELECT * FROM getrngfunc1(1) AS t1` gain is muted while
     `CREATE VIEW ... getrngfunc1(1)` still fails on the schema path (the
     current diff reuses Gres's `getrngfunc1` block as context for the
     expected view output).

## 3. Oracle facts check
   Confirmed from oracle .out: `count`, `grouping`, `array`, `case`, `row`
   (incl. `(1.1,2.2)::complex` -> row, `cast(row(..) as text)` -> row,
   `ROW()` -> row), `a` for lateral `v.a`, `t1` for `getrngfunc1(1) AS t1`,
   `rngfuncid | f2` for setof rngfunc2, `f1 | f2` for rngfunc_type,
   `column1` for VALUES-in-scalar-subquery, `generate_series` for SRF in
   scalar subquery, `c1`/`f1`/`a` for field selection. Correction to the
   analyst: CASE takes the ELSE expression's name when that is a column or a
   function name; "case" is the fallback (PG FigureColnameInternal
   T_CaseExpr). EXISTS -> "exists" is from PG source, not observed here.

## 4. Size
   S for A + B together (two edits + tests). C needs a composite-attribute
   lookup and mirroring on the schema path: another S. Overall S-M.
