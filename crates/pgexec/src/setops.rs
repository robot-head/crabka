//! SP38: set operations. UNION / INTERSECT / EXCEPT [ALL].
//!
//! A set operation folds the outputs of two or more SELECT branches. The
//! existing `exec::select_to_relation` evaluates each leaf to a `Relation`. This
//! module then resolves the combined output columns with PostgreSQL
//! `select_common_type` semantics, including `unknown`-literal resolution. It
//! coerces every branch's rows to those common types and applies the duplicate
//! semantics. Duplicate matching reuses `Datum`'s grouping `Eq`/`Hash`
//! (NULL = NULL), which is exactly PG's "not distinct" rule.

use std::collections::{HashMap, HashSet};

use crabka_pgkv::Kv;
use crabka_pgparser::ast::{Expr, QueryBody, SetExpr, SetOp};
use crabka_pgtypes::{ColumnType, Datum};

use crate::{
    clock::EvalCtx,
    error::ExecError,
    scope::{ColumnBinding, Scope},
};

/// Defense-in-depth recursion bound for the `SetExpr` tree walks (`fold` and
/// `resolve_set_columns`). It mirrors `eval`'s `MAX_EVAL_DEPTH`. The parser
/// already caps a parsed set-op tree at `pgparser`'s `MAX_DEPTH`, well under
/// this, so this fires only for a `SetExpr` built programmatically deeper than
/// the parser allows.
const MAX_SETOP_DEPTH: usize = 150;

/// One resolved output column of a set-op query: its `name`, from the leftmost
/// branch, its resolved `ty`, and whether it is still `unknown`. A column is
/// still `unknown` when every contributing branch column was a bare untyped
/// literal (`NULL` or a string literal), which PostgreSQL leaves as the
/// `unknown` pseudo-type. An unknown column takes whatever a typed branch
/// resolves to. If it stays unknown across every branch it becomes `text`,
/// which is PG's final unknown-to-text rule.
pub(crate) struct ResolvedCol {
    name: String,
    pub(crate) ty: ColumnType,
    pub(crate) unknown: bool,
}

/// A bare untyped literal, which is `NULL` or a string literal, is PostgreSQL's
/// `unknown` pseudo-type in set-operation type resolution. It takes the type of
/// the other branch and forces no clash. An explicit cast (`'x'::text`), a
/// column reference, or any function or expression result is a CONCRETE type and
/// is NOT unknown, so `1 UNION 'x'::text` is still a 42804 mismatch, like PG.
fn is_unknown_literal(e: &Expr) -> bool {
    matches!(e, Expr::NullLiteral | Expr::StringLiteral(_))
}

/// Unknown-aware pairwise column unification (PG `select_common_type`). An
/// `unknown` operand yields the other operand's type. Two `unknown`s stay
/// `unknown`. Two concrete types fold through `eval::unify_types`, which takes
/// the numeric tower or an identical type, and is 42804 otherwise.
/// `unify_types` is the LUB, so a pairwise fold across a branch list equals a
/// resolution of the whole list at once.
fn unify_col(
    lt: ColumnType,
    lunk: bool,
    rt: ColumnType,
    runk: bool,
) -> Result<(ColumnType, bool), ExecError> {
    Ok(match (lunk, runk) {
        // both unknown -> stay unknown (`lt` is the text placeholder from infer_type)
        (true, true) => (lt, true),
        // unknown ∪ concrete -> the concrete type
        (true, false) => (rt, false),
        (false, true) => (lt, false),
        (false, false) => (crate::eval::unify_types(lt, rt)?, false),
    })
}

/// Resolve a set-op subtree's output columns (name, type and unknown-ness),
/// schema-only, with no rows. Names come from the LEFT branch. Types are the
/// unknown-aware unification across branches. A column-count mismatch raises
/// 42601 with the offending operator. `describe_set_expr` and
/// `set_expr_to_relation` share this.
fn resolve_set_columns(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    e: &SetExpr,
    ctes: &crate::cte::CteContext,
    depth: usize,
) -> Result<Vec<ResolvedCol>, ExecError> {
    // Defense-in-depth: the parser caps set-op tree depth at MAX_DEPTH (50), so this
    // recursion is bounded for any parser-produced tree; the guard catches any
    // programmatically-built `SetExpr` deeper than `MAX_SETOP_DEPTH`. Returns 54001.
    if depth > MAX_SETOP_DEPTH {
        return Err(ExecError::StackDepthExceeded);
    }
    match e {
        SetExpr::Query(QueryBody::Select(s)) => {
            crate::exec::reject_nested_relation_locking(s)?;
            let scope = if s.from.is_empty() {
                Scope::empty()
            } else {
                crate::exec::build_from_schema_with_ctes(catalog_kv, resolution, &s.from, ctes)?
                    .scope
            };
            // Run the SP34 scalar-subquery type pass (so a subquery column's OID is
            // known without executing), then resolve names + types + unknown-ness.
            // The type pass has no outer scope of its own, so a sub-select whose
            // FROM reads this branch's row is given that row as NULLs first —
            // enough to describe it, which is all this pass needs.
            let describable = crate::exec::describable_projection(
                catalog_kv,
                resolution,
                ctes,
                &s.projection,
                &scope,
            )?;
            let projection = crate::subquery::resolve_types_in_projection_with_ctes(
                catalog_kv,
                resolution,
                &describable,
                ctes,
            )?;
            // A branch's window results are synthetic columns of its own row, so
            // its column types resolve against the widened scope.
            let scope = crate::window::describe_scope(s, &scope)?;
            let (fields, exprs, tys) = crate::exec::resolve_projection(&projection, &scope)?;
            Ok(fields
                .into_iter()
                .zip(tys)
                .zip(exprs)
                .map(|((f, ty), e)| ResolvedCol {
                    name: f.name,
                    ty,
                    unknown: is_unknown_literal(&e),
                })
                .collect())
        }
        SetExpr::Query(QueryBody::Values(v)) => {
            let rel =
                crate::values::values_schema_relation_with_ctes(catalog_kv, resolution, v, ctes)?;
            Ok(rel
                .scope
                .columns
                .into_iter()
                .map(|c| ResolvedCol {
                    name: c.name,
                    ty: c.ty,
                    unknown: false,
                })
                .collect())
        }
        SetExpr::Query(QueryBody::Nested(nested)) => {
            crate::query::describe_query_expr_with_ctes(catalog_kv, resolution, nested, ctes)?
                .into_iter()
                .map(|f| {
                    Ok(ResolvedCol {
                        name: f.name,
                        ty: crate::exec::column_type_from_oid(f.type_oid)?,
                        unknown: false,
                    })
                })
                .collect()
        }
        SetExpr::SetOp {
            op, left, right, ..
        } => {
            let l = resolve_set_columns(catalog_kv, resolution, left, ctes, depth + 1)?;
            let r = resolve_set_columns(catalog_kv, resolution, right, ctes, depth + 1)?;
            if l.len() != r.len() {
                return Err(ExecError::SetOpColumnCount {
                    op: *op,
                    left: l.len(),
                    right: r.len(),
                });
            }
            l.into_iter()
                .zip(r)
                .map(|(lc, rc)| {
                    let (ty, unknown) = unify_col(lc.ty, lc.unknown, rc.ty, rc.unknown)?;
                    Ok(ResolvedCol {
                        name: lc.name,
                        ty,
                        unknown,
                    })
                })
                .collect()
        }
    }
}

/// The final wire type of an output column. An unresolved `unknown` column
/// becomes `text`, which is PG's final unknown-to-text rule.
fn output_type(c: &ResolvedCol) -> ColumnType {
    if c.unknown { ColumnType::Text } else { c.ty }
}

/// The output columns of a set-op subtree, schema-only, with `PostgreSQL`'s
/// `unknown` flag still attached.
///
/// The `WITH RECURSIVE` type check needs the flag. An `unknown` recursive-term
/// column adopts the non-recursive term's type and does not clash with it, so it
/// must not collapse to `text` first the way [`output_type`] does.
pub(crate) fn set_expr_columns(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    body: &SetExpr,
    ctes: &crate::cte::CteContext,
) -> Result<Vec<ResolvedCol>, ExecError> {
    resolve_set_columns(catalog_kv, resolution, body, ctes, 0)
}

pub(crate) fn describe_set_expr_with_ctes(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    body: &SetExpr,
    ctes: &crate::cte::CteContext,
) -> Result<Vec<crabka_pgwire::engine::FieldDescription>, ExecError> {
    let cols = resolve_set_columns(catalog_kv, resolution, body, ctes, 0)?;
    Ok(cols
        .iter()
        .map(|c| crate::exec::field(&c.name, output_type(c)))
        .collect())
}

/// One branch of a set-operation tree evaluated on its own, with no result-level
/// tail. The `WITH RECURSIVE` fixpoint uses this. It evaluates the non-recursive
/// and recursive terms separately, and the tail belongs to the whole recursion,
/// not to either term.
pub(crate) fn set_expr_relation(
    ctx: &crate::subquery::SubCtx<'_>,
    body: &SetExpr,
) -> Result<crate::join::Relation, ExecError> {
    let cols = resolve_set_columns(ctx.catalog_kv, ctx.fctx.resolution, body, ctx.ctes, 0)?;
    let out_tys: Vec<ColumnType> = cols.iter().map(output_type).collect();
    let rows = fold(ctx, body, &out_tys, 0)?;
    let scope = Scope {
        columns: cols
            .iter()
            .map(|c| ColumnBinding {
                qualifier: None,
                name: c.name.clone(),
                ty: output_type(c),
            })
            .collect(),
    };
    Ok(crate::join::Relation { scope, rows })
}

pub(crate) fn set_expr_to_relation(
    ctx: &crate::subquery::SubCtx<'_>,
    body: &SetExpr,
    order_by: &[crabka_pgparser::ast::OrderItem],
    window: crate::exec::RowWindow,
) -> Result<crate::join::Relation, ExecError> {
    let cols = resolve_set_columns(ctx.catalog_kv, ctx.fctx.resolution, body, ctx.ctes, 0)?;
    let out_tys: Vec<ColumnType> = cols.iter().map(output_type).collect();
    let scope = Scope {
        columns: cols
            .iter()
            .map(|c| ColumnBinding {
                qualifier: None,
                name: c.name.clone(),
                ty: output_type(c),
            })
            .collect(),
    };
    for item in order_by {
        let ty = if let Some(index) = crate::sql92::output_position(
            &item.expr,
            scope.width(),
            crate::sql92::Sql92Clause::OrderBy,
        )? {
            scope.ty_at(index)
        } else {
            crate::eval::infer_type(&item.expr, &scope)?
        };
        crate::eval::require_ordering_operator(ty)?;
    }
    let rows = fold(ctx, body, &out_tys, 0)?;

    let mut keyed: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::with_capacity(rows.len());
    for row in rows {
        let mut keys = Vec::with_capacity(order_by.len());
        for item in order_by {
            keys.push(order_key(&item.expr, &scope, &row, ctx.eval_ctx)?);
        }
        keyed.push((keys, row));
    }
    if !order_by.is_empty() {
        keyed.sort_by(|a, b| crate::exec::order_cmp(&a.0, &b.0, order_by));
    }
    let rows = crate::exec::apply_row_window(keyed, window, order_by);

    Ok(crate::join::Relation { scope, rows })
}

/// One ORDER BY key for the set-op output. An integer literal is a 1-based
/// position. Anything else evaluates against the output scope, which holds the
/// output column names and expressions.
fn order_key(expr: &Expr, scope: &Scope, row: &[Datum], ctx: &EvalCtx) -> Result<Datum, ExecError> {
    // PG: a positional ORDER BY out of range is 42P10 (invalid_column_reference),
    // not 0A000 — the feature IS supported, the position is just invalid.
    if let Some(index) =
        crate::sql92::output_position(expr, scope.width(), crate::sql92::Sql92Clause::OrderBy)?
    {
        return Ok(row[index].clone());
    }
    crate::eval::eval(expr, scope, row, ctx)
}

/// Fold a set-op subtree to combined rows, and coerce each leaf's rows to the
/// common per-column output types `out_tys`, which `resolve_set_columns`
/// resolved once. Both sides of a `SetOp` node then carry identical types, so
/// the multiset combine compares like-typed `Datum`s.
fn fold(
    ctx: &crate::subquery::SubCtx<'_>,
    e: &SetExpr,
    out_tys: &[ColumnType],
    depth: usize,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    // Defense-in-depth (parser already caps the tree at MAX_DEPTH): 54001, not a crash.
    if depth > MAX_SETOP_DEPTH {
        return Err(ExecError::StackDepthExceeded);
    }
    match e {
        SetExpr::Query(QueryBody::Select(s)) => {
            let rel = crate::exec::select_to_relation_with_ctes(ctx, s)?;
            coerce_rows(rel.rows, &rel.scope, out_tys, ctx.eval_ctx)
        }
        SetExpr::Query(QueryBody::Values(v)) => {
            let rel = crate::values::values_to_relation_with_ctes(ctx, v)?;
            coerce_rows(rel.rows, &rel.scope, out_tys, ctx.eval_ctx)
        }
        SetExpr::Query(QueryBody::Nested(nested)) => {
            let rel = crate::query::query_to_relation_with_ctes(ctx, nested)?;
            coerce_rows(rel.rows, &rel.scope, out_tys, ctx.eval_ctx)
        }
        SetExpr::SetOp {
            op,
            all,
            left,
            right,
        } => {
            if !(*op == SetOp::Union && *all) {
                for ty in out_tys {
                    crate::eval::require_equality_operator(*ty)?;
                }
            }
            let lrows = fold(ctx, left, out_tys, depth + 1)?;
            let rrows = fold(ctx, right, out_tys, depth + 1)?;
            let combined_bytes = lrows.iter().chain(&rrows).fold(0usize, |bytes, row| {
                bytes.saturating_add(crate::scanner::datum_row_bytes(row))
            });
            if crate::scanner::exceeds_query_memory(
                combined_bytes,
                crate::scanner::BLOCKING_QUERY_MEMORY,
            ) {
                return Err(crate::scanner::memory_budget_exceeded());
            }
            Ok(combine_rows(*op, *all, lrows, rrows))
        }
    }
}

/// Multiset combine of two already-same-typed row sets under one set operator.
fn combine_rows(
    op: SetOp,
    all: bool,
    lrows: Vec<Vec<Datum>>,
    rrows: Vec<Vec<Datum>>,
) -> Vec<Vec<Datum>> {
    match op {
        SetOp::Union if all => {
            let mut v = lrows;
            v.extend(rrows);
            v
        }
        SetOp::Union => dedup_keep_order(lrows.into_iter().chain(rrows)),
        SetOp::Intersect => intersect(lrows, rrows, all),
        SetOp::Except => except(lrows, rrows, all),
    }
}

/// Coerce each row's cells from the child's column types to the common `tys`. A
/// NULL cell passes through unchanged, because NULL of any type is NULL. A
/// same-type cell stays untouched. Anything else takes a cast. For example, an
/// `unknown` string literal resolved to `int4` parses through `text` to `int4`,
/// and raises 22P02 on a bad value exactly like PG.
fn coerce_rows(
    rows: Vec<Vec<Datum>>,
    scope: &Scope,
    tys: &[ColumnType],
    ctx: &EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut cells = Vec::with_capacity(row.len());
        for (i, cell) in row.into_iter().enumerate() {
            if scope.ty_at(i) == tys[i] || cell.is_null() {
                cells.push(cell);
            } else {
                cells.push(crate::eval::cast_value(&cell, tys[i], &ctx.time_zone)?);
            }
        }
        out.push(cells);
    }
    Ok(out)
}

/// Distinct, with first-seen order preserved (UNION).
fn dedup_keep_order<I: Iterator<Item = Vec<Datum>>>(it: I) -> Vec<Vec<Datum>> {
    let mut seen: HashSet<Vec<Datum>> = HashSet::new();
    let mut out = Vec::new();
    for row in it {
        if seen.insert(row.clone()) {
            out.push(row);
        }
    }
    out
}

/// Multiset count of each distinct row.
fn counts(rows: &[Vec<Datum>]) -> HashMap<Vec<Datum>, usize> {
    let mut m: HashMap<Vec<Datum>, usize> = HashMap::new();
    for r in rows {
        *m.entry(r.clone()).or_insert(0) += 1;
    }
    m
}

/// INTERSECT: rows in both. The distinct form gives one row per distinct row
/// present in both. The ALL form gives min(Lₙ, Rₙ). Distinct left rows run in
/// first-seen order.
fn intersect(lrows: Vec<Vec<Datum>>, rrows: Vec<Vec<Datum>>, all: bool) -> Vec<Vec<Datum>> {
    let lc = counts(&lrows); // read only on the ALL path (min multiplicity)
    let rc = counts(&rrows);
    let mut seen: HashSet<Vec<Datum>> = HashSet::new();
    let mut out = Vec::new();
    for row in &lrows {
        if !seen.insert(row.clone()) {
            continue; // each distinct left row handled once, in order
        }
        let rcount = *rc.get(row).unwrap_or(&0);
        if rcount == 0 {
            continue; // not present in right
        }
        let mult = if all { lc[row].min(rcount) } else { 1 };
        for _ in 0..mult {
            out.push(row.clone());
        }
    }
    out
}

/// EXCEPT: the distinct form gives each distinct left row ABSENT from the right
/// (count_R == 0) once. The ALL form gives max(0, Lₙ − Rₙ). Distinct left rows
/// run in first-seen order.
fn except(lrows: Vec<Vec<Datum>>, rrows: Vec<Vec<Datum>>, all: bool) -> Vec<Vec<Datum>> {
    let lc = counts(&lrows);
    let rc = counts(&rrows);
    let mut seen: HashSet<Vec<Datum>> = HashSet::new();
    let mut out = Vec::new();
    for row in &lrows {
        if !seen.insert(row.clone()) {
            continue;
        }
        let rcount = *rc.get(row).unwrap_or(&0);
        let mult = if all {
            lc[row].saturating_sub(rcount)
        } else if rcount == 0 {
            1
        } else {
            0
        };
        for _ in 0..mult {
            out.push(row.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn i4(n: i32) -> Vec<Datum> {
        vec![Datum::Int4(n)]
    }

    #[test]
    fn union_dedups_union_all_keeps() {
        let l = vec![i4(1), i4(2)];
        let r = vec![i4(2), i4(3)];
        assert_eq!(
            combine_rows(SetOp::Union, false, l.clone(), r.clone()),
            vec![i4(1), i4(2), i4(3)]
        );
        assert_eq!(
            combine_rows(SetOp::Union, true, l, r),
            vec![i4(1), i4(2), i4(2), i4(3)]
        );
    }

    #[test]
    fn intersect_and_except_multiplicity() {
        let l = vec![i4(1), i4(1), i4(2)];
        let r = vec![i4(1), i4(3)];
        assert_eq!(
            combine_rows(SetOp::Intersect, false, l.clone(), r.clone()),
            vec![i4(1)]
        );
        assert_eq!(
            combine_rows(SetOp::Intersect, true, l.clone(), r.clone()),
            vec![i4(1)]
        );
        // EXCEPT distinct: {2}; EXCEPT ALL: two 1s minus one 1 = one 1, plus 2 => [1,2]
        assert_eq!(
            combine_rows(SetOp::Except, false, l.clone(), r.clone()),
            vec![i4(2)]
        );
        assert_eq!(combine_rows(SetOp::Except, true, l, r), vec![i4(1), i4(2)]);
    }

    #[test]
    fn except_all_underflows_to_empty() {
        // When the right side has MORE copies than the left, EXCEPT ALL clamps the
        // multiplicity at 0 (max(0, Lₙ − Rₙ)) — it never wraps. Pins `saturating_sub`.
        assert_eq!(
            combine_rows(SetOp::Except, true, vec![i4(1)], vec![i4(1), i4(1)]),
            Vec::<Vec<Datum>>::new()
        );
    }

    #[test]
    fn null_equals_null_in_dedup() {
        let n = || vec![Datum::Null];
        assert_eq!(
            combine_rows(SetOp::Union, false, vec![n(), n()], vec![n()]),
            vec![n()]
        );
    }

    #[test]
    fn unify_col_numeric_tower_and_incompatible() {
        // int4 ∪ int8 → int8
        assert_eq!(
            unify_col(ColumnType::Int4, false, ColumnType::Int8, false).expect("ok"),
            (ColumnType::Int8, false)
        );
        // two CONCRETE incompatible types → 42804
        assert!(matches!(
            unify_col(ColumnType::Int4, false, ColumnType::Text, false),
            Err(ExecError::TypeMismatch(_))
        ));
    }

    #[test]
    fn unify_col_unknown_takes_the_other_branch_type() {
        // An `unknown` (bare NULL / string literal) column unifies to the concrete
        // side — the fix that lets `SELECT NULL UNION SELECT 1` and
        // `SELECT 1 UNION SELECT '5'` resolve to int4 (matching PG) instead of 42804.
        assert_eq!(
            unify_col(ColumnType::Text, true, ColumnType::Int4, false).expect("ok"),
            (ColumnType::Int4, false)
        );
        assert_eq!(
            unify_col(ColumnType::Int4, false, ColumnType::Text, true).expect("ok"),
            (ColumnType::Int4, false)
        );
        // both unknown stays unknown (→ text at output, PG's final unknown→text rule)
        assert_eq!(
            unify_col(ColumnType::Text, true, ColumnType::Text, true).expect("ok"),
            (ColumnType::Text, true)
        );
    }

    #[test]
    fn is_unknown_literal_only_bare_null_and_string() {
        assert!(is_unknown_literal(&Expr::NullLiteral));
        assert!(is_unknown_literal(&Expr::StringLiteral("x".into())));
        // an integer literal / column ref / explicit value is concrete, not unknown
        assert!(!is_unknown_literal(&Expr::IntLiteral("1".into())));
        assert!(!is_unknown_literal(&Expr::Column {
            table: None,
            name: "c".into()
        }));
    }

    /// End-to-end: UNION deduplicates across two tables and ORDER BY positions
    /// the combined output. This exercises the query relation pipeline and
    /// session dispatch.
    #[tokio::test]
    async fn union_runs_end_to_end() {
        use crabka_pgwire::engine::{Engine, QueryResult, Session};

        use crate::SqlEngine;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        for sql in [
            "CREATE TABLE t (a int4)",
            "INSERT INTO t VALUES (1),(2),(2)",
            "CREATE TABLE u (a int4)",
            "INSERT INTO u VALUES (2),(3)",
        ] {
            s.simple_query(sql).await.expect("setup");
        }
        let r = s
            .simple_query("SELECT a FROM t UNION SELECT a FROM u ORDER BY a")
            .await
            .expect("union");
        let QueryResult::Rows { rows, .. } = &r[0] else {
            panic!("expected rows")
        };
        let got: Vec<_> = rows
            .iter()
            .map(|row| row[0].as_ref().expect("non-null").text.to_vec())
            .collect();
        assert_eq!(
            got,
            vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()],
            "UNION should dedup and order: [1, 2, 3]"
        );
    }

    /// Every value of column 0, as wire text (`None` for SQL NULL).
    async fn column0(engine: &crate::SqlEngine, sql: &str) -> Vec<Option<String>> {
        use crabka_pgwire::engine::{Engine, QueryResult, Session};

        let r = engine
            .connect()
            .simple_query(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        let QueryResult::Rows { rows, .. } = &r[0] else {
            panic!("expected rows from {sql}")
        };
        rows.iter()
            .map(|row| {
                row[0]
                    .as_ref()
                    .map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
            })
            .collect()
    }

    /// A subquery nested under ANY expression form in a set-operation branch must
    /// be resolved before the branch is typed.
    ///
    /// `resolve_set_columns` types each branch schema-only, through
    /// `subquery::resolve_types_in_projection_with_ctes`. While that pass matched a
    /// hand-listed few node kinds and cloned the rest, a subquery under `CASE` (or
    /// `BETWEEN`, an `IN` list, an array literal, `COLLATE`, …) survived into
    /// `eval::infer_type`, which refuses one — XX000 "scalar subquery must be
    /// resolved before type inference". The expectations are `PostgreSQL` 18.4's.
    #[tokio::test]
    async fn subquery_under_every_expr_form_in_a_union_branch() {
        use assert2::assert;
        use crabka_pgwire::engine::{Engine, Session};

        let engine = crate::SqlEngine::new();
        let mut s = engine.connect();
        for sql in [
            "CREATE TABLE sq (v int4)",
            "INSERT INTO sq VALUES (10),(20),(30)",
        ] {
            s.simple_query(sql).await.expect("setup");
        }

        // (form, second branch, that branch's expected single value)
        let cases = [
            (
                "CASE result",
                "SELECT CASE WHEN true THEN (SELECT max(v) FROM sq) ELSE 0 END",
                "30",
            ),
            (
                "CASE condition",
                "SELECT CASE WHEN (SELECT count(*) FROM sq) = 3 THEN 7 ELSE 0 END",
                "7",
            ),
            (
                "CASE else",
                "SELECT CASE WHEN false THEN 0 ELSE (SELECT min(v) FROM sq) END",
                "10",
            ),
            (
                "CASE operand",
                "SELECT CASE (SELECT max(v) FROM sq) WHEN 30 THEN 1 ELSE 2 END",
                "1",
            ),
            (
                "COALESCE argument",
                "SELECT COALESCE((SELECT max(v) FROM sq), 0)",
                "30",
            ),
            (
                "array literal under a subscript",
                "SELECT (ARRAY[(SELECT max(v) FROM sq), 7])[1]",
                "30",
            ),
            (
                "ARRAY(subquery)",
                "SELECT array_length(ARRAY(SELECT v FROM sq), 1)",
                "3",
            ),
        ];
        for (form, branch, expected) in cases {
            let sql = format!("SELECT 0 UNION ALL {branch}");
            let got = column0(&engine, &sql).await;
            assert!(
                got == vec![Some("0".to_owned()), Some(expected.to_owned())],
                "{form}: {sql}"
            );
        }

        // The boolean-valued forms, whose left branch must also be boolean.
        let bool_cases = [
            (
                "BETWEEN bounds",
                "SELECT 20 BETWEEN (SELECT min(v) FROM sq) AND (SELECT max(v) FROM sq)",
                "t",
            ),
            (
                "IN list element",
                "SELECT 10 IN ((SELECT min(v) FROM sq), 99)",
                "t",
            ),
            (
                "IS NULL operand",
                "SELECT (SELECT max(v) FROM sq) IS NULL",
                "f",
            ),
            (
                "= ANY over an array literal",
                "SELECT 30 = ANY(ARRAY[(SELECT max(v) FROM sq)])",
                "t",
            ),
            ("LIKE pattern", "SELECT 'abc' LIKE (SELECT 'a%'::text)", "t"),
        ];
        for (form, branch, expected) in bool_cases {
            let sql = format!("SELECT false UNION ALL {branch}");
            let got = column0(&engine, &sql).await;
            assert!(
                got == vec![Some("f".to_owned()), Some(expected.to_owned())],
                "{form}: {sql}"
            );
        }

        // COLLATE takes a text operand, so it gets its own text-typed left branch.
        let got = column0(
            &engine,
            "SELECT 'z'::text UNION ALL SELECT (SELECT 'a'::text) COLLATE \"C\"",
        )
        .await;
        assert!(got == vec![Some("z".to_owned()), Some("a".to_owned())]);
    }

    /// The shape psql's `\d` publication lookup has: a three-branch UNION whose
    /// middle branch wraps a CORRELATED subquery in a `CASE`. Both halves have to
    /// hold — the branch must type without executing, and the subquery must then
    /// re-run per outer row rather than being folded once. Matches `PostgreSQL` 18.4.
    #[tokio::test]
    async fn correlated_case_subquery_in_a_union_branch() {
        use assert2::assert;
        use crabka_pgwire::engine::{Engine, Session};

        let engine = crate::SqlEngine::new();
        let mut s = engine.connect();
        for sql in [
            "CREATE TABLE att (attrelid int4, attname text)",
            "INSERT INTO att VALUES (10,'id'),(10,'name'),(20,'k')",
            "CREATE TABLE pr (prrelid int4, prattrs int4[])",
            "INSERT INTO pr VALUES (10,'{1,2}'),(20,NULL)",
        ] {
            s.simple_query(sql).await.expect("setup");
        }
        let got = column0(
            &engine,
            "SELECT NULL::text AS c \
             UNION ALL \
             SELECT CASE WHEN pr.prattrs IS NOT NULL \
                         THEN (SELECT string_agg(a.attname, ', ') FROM att a \
                               WHERE a.attrelid = pr.prrelid) \
                    END \
               FROM pr \
             UNION ALL \
             SELECT NULL::text",
        )
        .await;
        // Row 2 correlates to prrelid=10; row 3 takes the CASE's implicit NULL.
        assert!(got == vec![None, Some("id, name".to_owned()), None, None]);

        // The same lookup as psql actually spells it: the sub-select's FROM
        // itself reads the branch's row, through a set-returning function's
        // argument. Describing that FROM needs the reference's TYPE, which is
        // why the branch is described against a NULL row rather than not at all.
        let got = column0(
            &engine,
            "SELECT NULL::text AS c \
             UNION ALL \
             SELECT CASE WHEN pr.prattrs IS NOT NULL \
                         THEN (SELECT string_agg(attname, ', ') \
                                 FROM generate_series(1, array_upper(pr.prattrs, 1)) s, att \
                                WHERE attrelid = pr.prrelid AND attname IS NOT NULL) \
                    END \
               FROM pr",
        )
        .await;
        // prrelid=10 has two attributes, and the two generate_series values
        // repeat them; prattrs IS NULL for 20, so that row takes the CASE's NULL.
        assert!(got == vec![None, Some("id, name, id, name".to_owned()), None]);
    }

    /// A positional ORDER BY past the number of output columns is PG 42P10
    /// (invalid_column_reference), NOT 0A000.
    #[tokio::test]
    async fn order_by_position_out_of_range_is_42p10() {
        use crabka_pgwire::engine::{Engine, Session};

        use crate::SqlEngine;

        let engine = SqlEngine::new();
        let err = engine
            .connect()
            .simple_query("SELECT 1 UNION SELECT 2 ORDER BY 5")
            .await
            .expect_err("out-of-range ORDER BY position");
        assert_eq!(err.code, "42P10");
    }
}
