//! SP38: set operations — UNION / INTERSECT / EXCEPT [ALL].
//!
//! A set operation folds the outputs of two or more SELECT branches. Each leaf is
//! evaluated to a `Relation` via the existing `exec::select_to_relation`; this module
//! resolves the combined output columns (PostgreSQL `select_common_type` semantics,
//! incl. `unknown`-literal resolution), coerces every branch's rows to those common
//! types, and applies the duplicate semantics. Duplicate matching reuses `Datum`'s
//! grouping `Eq`/`Hash` (NULL = NULL), which is exactly PG's "not distinct" rule.

use std::collections::{HashMap, HashSet};

use crabka_pgkv::Kv;
use crabka_pgmvcc::visibility::Snapshot;
use crabka_pgparser::ast::{Expr, QueryBody, SetExpr, SetOp};
use crabka_pgtypes::{ColumnType, Datum};

use crate::{
    clock::EvalCtx,
    error::ExecError,
    scanner::RangeScanner,
    scope::{ColumnBinding, Scope},
};

/// Defense-in-depth recursion bound for the `SetExpr` tree walks (`fold` /
/// `resolve_set_columns`), mirroring `eval`'s `MAX_EVAL_DEPTH`. The parser already
/// caps a parsed set-op tree at `pgparser`'s `MAX_DEPTH` (well under this), so this
/// only fires for a `SetExpr` built programmatically deeper than the parser allows.
const MAX_SETOP_DEPTH: usize = 150;

/// One resolved output column of a set-op query: its `name` (from the leftmost
/// branch), its resolved `ty`, and whether it is still `unknown` — i.e. every
/// contributing branch column was a bare untyped literal (`NULL` or a string
/// literal), which PostgreSQL leaves as the `unknown` pseudo-type. An unknown column
/// takes whatever a typed branch resolves to; if it stays unknown across every branch
/// it becomes `text` (PG's final unknown→text rule).
struct ResolvedCol {
    name: String,
    ty: ColumnType,
    unknown: bool,
}

/// A bare untyped literal — `NULL` or a string literal — is PostgreSQL's `unknown`
/// pseudo-type in set-operation type resolution: it takes the type of the other
/// branch rather than forcing a clash. An explicit cast (`'x'::text`), a column
/// reference, or any function/expression result is a CONCRETE type and is NOT
/// unknown (so `1 UNION 'x'::text` is still a 42804 mismatch, like PG).
fn is_unknown_literal(e: &Expr) -> bool {
    matches!(e, Expr::NullLiteral | Expr::StringLiteral(_))
}

/// Unknown-aware pairwise column unification (PG `select_common_type`): an `unknown`
/// operand yields the other operand's type; two `unknown`s stay `unknown`; two
/// concrete types fold through `eval::unify_types` (numeric tower / identical, else
/// 42804). `unify_types` is the LUB, so folding pairwise across a branch list equals
/// resolving the whole list at once.
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

/// Resolve a set-op subtree's output columns (name + type + unknown-ness),
/// schema-only (no rows). Names come from the LEFT branch; types are the
/// unknown-aware unification across branches; a column-count mismatch raises 42601
/// with the offending operator. Shared by `describe_set_expr` and
/// `set_expr_to_relation`.
fn resolve_set_columns(
    catalog_kv: &dyn Kv,
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
                crate::exec::build_from_schema_with_ctes(catalog_kv, &s.from, ctes)?.scope
            };
            // Run the SP34 scalar-subquery type pass (so a subquery column's OID is
            // known without executing), then resolve names + types + unknown-ness.
            let projection = crate::subquery::resolve_types_in_projection_with_ctes(
                catalog_kv,
                &s.projection,
                ctes,
            )?;
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
            let rel = crate::values::values_schema_relation_with_ctes(catalog_kv, v, ctes)?;
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
            crate::query::describe_query_expr_with_ctes(catalog_kv, nested, ctes)?
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
            let l = resolve_set_columns(catalog_kv, left, ctes, depth + 1)?;
            let r = resolve_set_columns(catalog_kv, right, ctes, depth + 1)?;
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

/// The final wire type of an output column: an unresolved `unknown` column becomes
/// `text` (PG's final unknown→text rule).
fn output_type(c: &ResolvedCol) -> ColumnType {
    if c.unknown { ColumnType::Text } else { c.ty }
}

pub(crate) fn describe_set_expr_with_ctes(
    catalog_kv: &dyn Kv,
    body: &SetExpr,
    ctes: &crate::cte::CteContext,
) -> Result<Vec<crabka_pgwire::engine::FieldDescription>, ExecError> {
    let cols = resolve_set_columns(catalog_kv, body, ctes, 0)?;
    Ok(cols
        .iter()
        .map(|c| crate::exec::field(&c.name, output_type(c)))
        .collect())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn set_expr_to_relation(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &Snapshot,
    snapshot: &Snapshot,
    own: Option<u64>,
    body: &SetExpr,
    order_by: &[crabka_pgparser::ast::OrderItem],
    offset: Option<i64>,
    limit: Option<i64>,
    ctes: &crate::cte::CteContext,
    ctx: &EvalCtx,
    fctx: crate::exec::ForeignCtx,
    range_scanner: &dyn RangeScanner,
) -> Result<crate::join::Relation, ExecError> {
    let cols = resolve_set_columns(catalog_kv, body, ctes, 0)?;
    let out_tys: Vec<ColumnType> = cols.iter().map(output_type).collect();
    let mut rows = fold(
        catalog_kv,
        kv,
        global,
        gsnap,
        snapshot,
        own,
        body,
        &out_tys,
        ctes,
        ctx,
        fctx,
        range_scanner,
        0,
    )?;

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

    if !order_by.is_empty() {
        let mut keyed: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut keys = Vec::with_capacity(order_by.len());
            for item in order_by {
                keys.push(order_key(&item.expr, &scope, &row, ctx)?);
            }
            keyed.push((keys, row));
        }
        keyed.sort_by(|a, b| crate::exec::order_cmp(&a.0, &b.0, order_by));
        rows = keyed.into_iter().map(|(_, r)| r).collect();
    }
    crate::exec::apply_offset_limit(&mut rows, offset, limit);

    Ok(crate::join::Relation { scope, rows })
}

/// One ORDER BY key for the set-op output: integer literal → 1-based position;
/// otherwise evaluate against the output scope (output column name / expression).
fn order_key(expr: &Expr, scope: &Scope, row: &[Datum], ctx: &EvalCtx) -> Result<Datum, ExecError> {
    if let Expr::IntLiteral(s) = expr {
        // PG: a positional ORDER BY out of range is 42P10 (invalid_column_reference),
        // not 0A000 — the feature IS supported, the position is just invalid.
        let pos: usize = s.parse().map_err(|_| {
            ExecError::InvalidColumnReference(format!(
                "ORDER BY position {s} is not in select list"
            ))
        })?;
        if pos == 0 || pos > scope.width() {
            return Err(ExecError::InvalidColumnReference(format!(
                "ORDER BY position {pos} is not in select list"
            )));
        }
        return Ok(row[pos - 1].clone());
    }
    crate::eval::eval(expr, scope, row, ctx)
}

/// Fold a set-op subtree to combined rows, coercing each leaf's rows to the common
/// per-column output types `out_tys` (resolved once by `resolve_set_columns`). Both
/// sides of a `SetOp` node therefore carry identical types, so the multiset combine
/// compares like-typed `Datum`s.
#[allow(clippy::too_many_arguments)]
fn fold(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &Snapshot,
    snapshot: &Snapshot,
    own: Option<u64>,
    e: &SetExpr,
    out_tys: &[ColumnType],
    ctes: &crate::cte::CteContext,
    ctx: &EvalCtx,
    fctx: crate::exec::ForeignCtx,
    range_scanner: &dyn RangeScanner,
    depth: usize,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    // Defense-in-depth (parser already caps the tree at MAX_DEPTH): 54001, not a crash.
    if depth > MAX_SETOP_DEPTH {
        return Err(ExecError::StackDepthExceeded);
    }
    match e {
        SetExpr::Query(QueryBody::Select(s)) => {
            let rel = crate::exec::select_to_relation_with_ctes(
                catalog_kv,
                kv,
                global,
                gsnap,
                snapshot,
                own,
                s,
                ctes,
                ctx,
                fctx,
                range_scanner,
            )?;
            coerce_rows(rel.rows, &rel.scope, out_tys, ctx)
        }
        SetExpr::Query(QueryBody::Values(v)) => {
            let rel = crate::values::values_to_relation_with_ctes(
                catalog_kv,
                kv,
                global,
                gsnap,
                snapshot,
                own,
                v,
                ctes,
                ctx,
                fctx,
                range_scanner,
            )?;
            coerce_rows(rel.rows, &rel.scope, out_tys, ctx)
        }
        SetExpr::Query(QueryBody::Nested(nested)) => {
            let rel = crate::query::query_to_relation_with_ctes(
                catalog_kv,
                kv,
                global,
                gsnap,
                snapshot,
                own,
                nested,
                ctes,
                ctx,
                fctx,
                range_scanner,
            )?;
            coerce_rows(rel.rows, &rel.scope, out_tys, ctx)
        }
        SetExpr::SetOp {
            op,
            all,
            left,
            right,
        } => {
            let lrows = fold(
                catalog_kv,
                kv,
                global,
                gsnap,
                snapshot,
                own,
                left,
                out_tys,
                ctes,
                ctx,
                fctx,
                range_scanner,
                depth + 1,
            )?;
            let rrows = fold(
                catalog_kv,
                kv,
                global,
                gsnap,
                snapshot,
                own,
                right,
                out_tys,
                ctes,
                ctx,
                fctx,
                range_scanner,
                depth + 1,
            )?;
            let combined_bytes = lrows.iter().chain(&rrows).fold(0usize, |bytes, row| {
                bytes.saturating_add(crate::scanner::datum_row_bytes(row))
            });
            if combined_bytes > crate::scanner::BLOCKING_QUERY_MEMORY_BYTES {
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

/// Coerce each row's cells from the child's column types to the common `tys`. A NULL
/// cell passes through unchanged (NULL of any type is NULL); a same-type cell is
/// untouched; anything else is cast (e.g. an `unknown` string literal resolved to
/// `int4` parses via `text→int4`, raising 22P02 on a bad value exactly like PG).
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
                cells.push(crabka_pgtypes::cast::cast(&cell, tys[i], &ctx.time_zone)?);
            }
        }
        out.push(cells);
    }
    Ok(out)
}

/// Distinct, preserving first-seen order (UNION).
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

/// INTERSECT: rows in both. distinct → once per distinct row present in both;
/// ALL → min(Lₙ, Rₙ). Distinct left rows are processed in first-seen order.
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

/// EXCEPT: distinct → distinct left rows ABSENT from right (count_R == 0), once;
/// ALL → max(0, Lₙ − Rₙ). Distinct left rows are processed in first-seen order.
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
    /// the combined output — exercises the query relation pipeline plus session dispatch.
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
