//! Positional binding: resolve an expression's column references against a
//! [`Scope`] ONCE, before the row loop, instead of once per row.
//!
//! [`Scope::resolve`] is a linear scan with a string compare per column, so a
//! predicate or projection evaluated over `N` rows against a scope `W` columns
//! wide costs `O(N × W)` string comparisons. A `perf` capture of a wide
//! partition join showed nearly the whole thread inside `Scope::resolve`.
//!
//! The fix is the representation `PostgreSQL` itself uses: a `Var` names a
//! column by position, not by spelling. [`crate::scope::POSITION_QUALIFIER`]
//! (`$pos`) is already that representation here — `Scope::resolve` short-circuits
//! it into a `parse::<usize>()` and never touches `columns` — so binding is a
//! rewrite of every `Expr::Column` into its `$pos.<index>` form.
//!
//! # Why binding never changes an error
//!
//! Binding is DELIBERATELY best-effort: a reference that does not resolve is
//! left exactly as written, so evaluation raises the identical
//! [`crate::error::ExecError`] at the identical moment it would have without
//! binding.
//!
//! It has to be. Resolution is a per-statement question, but reporting a
//! resolution FAILURE is a per-row event here: a `WHERE` or `ON` clause naming a
//! column that does not resolve — or resolves ambiguously — is silent over an
//! empty relation, because no row ever evaluates it. Binding runs once whether
//! or not a row exists, so a binder that reported the failure would turn every
//! one of those statements into a 42703/42702/42P01 on an empty table.
//! `PostgreSQL` does report them (it binds at parse analysis), so that would
//! arguably be an improvement — but it is a behavior change, and this is a
//! performance fix. `an_unresolvable_reference_is_silent_until_a_row_evaluates_it`
//! in `tests/joins.rs` pins the behavior that must not move.
//!
//! # Scope pairing
//!
//! A bound expression is meaningful ONLY against the scope it was bound to:
//! `$pos.3` is "column 3 of that scope". [`BoundExpr`] exists to make that
//! pairing explicit — construct it immediately before the loop that evaluates
//! it, against the same `Scope` the loop passes to `eval`.

use crabka_pgparser::ast::Expr;

use crate::{
    error::ExecError,
    scope::{POSITION_QUALIFIER, Scope},
};

/// An expression whose resolvable column references have been rewritten to
/// positional (`$pos.<index>`) form against one specific [`Scope`].
///
/// Evaluate it only against that scope — see the module docs.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BoundExpr(Expr);

impl BoundExpr {
    /// Bind `expr` against `scope`.
    pub(crate) fn new(expr: &Expr, scope: &Scope) -> Self {
        Self(bind(expr, scope))
    }

    /// The rewritten expression, to hand to `eval` against the same scope.
    pub(crate) fn expr(&self) -> &Expr {
        &self.0
    }
}

/// [`BoundExpr::new`] over a list, preserving order.
pub(crate) fn bind_all(exprs: &[Expr], scope: &Scope) -> Vec<BoundExpr> {
    exprs.iter().map(|e| BoundExpr::new(e, scope)).collect()
}

/// [`BoundExpr::new`] over an optional expression.
pub(crate) fn bind_optional(expr: Option<&Expr>, scope: &Scope) -> Option<BoundExpr> {
    expr.map(|e| BoundExpr::new(e, scope))
}

/// The positional rewrite itself.
///
/// It drives [`crate::grouping::rewrite`], the crate's one exhaustive [`Expr`]
/// fold, which already leaves a subquery's inner tree alone — that tree resolves
/// against its own scope, not this one.
///
/// Aggregate arguments are left alone (`into_aggregates: false`). The grouped
/// evaluator matches an aggregate call against its accumulators by expression,
/// and it evaluates those arguments against a different relation than the one
/// bound here; a scalar `eval` reaching an aggregate call is an error either way,
/// and rewriting its arguments would not change which error.
fn bind(expr: &Expr, scope: &Scope) -> Expr {
    let mut fold = |node: &Expr| -> Result<Option<Expr>, ExecError> {
        let Expr::Column { table, name } = node else {
            return Ok(None);
        };
        // `eval` tries the no-paren niladic keywords (`current_schema`) BEFORE
        // resolving a bare name, so binding must not claim one of those names
        // even if the scope happens to hold a column spelled the same way.
        if table.is_none() && crate::func::niladic_keyword_call(name).is_some() {
            return Ok(None);
        }
        Ok(scope
            .resolve(table.as_deref(), name)
            .ok()
            .map(positional_reference))
    };
    // The fold is infallible, and `rewrite` only ever propagates what the fold
    // returns, so the walk cannot fail.
    crate::grouping::rewrite(expr, &mut fold, false).unwrap_or_else(|_| expr.clone())
}

/// The `$pos.<index>` reference for a resolved flat column index.
fn positional_reference(index: usize) -> Expr {
    Expr::Column {
        table: Some(POSITION_QUALIFIER.to_string()),
        name: index.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgcatalog::{Column, RelationName, Table};
    use crabka_pgparser::ast::BinaryOp;
    use crabka_pgtypes::{ColumnType, Datum};

    use super::*;
    use crate::clock::EvalCtx;

    fn tbl(name: &str, cols: &[(&str, ColumnType)]) -> Table {
        Table {
            id: 1,
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            name: RelationName::public(name),
            columns: cols.iter().map(|(n, t)| Column::new(*n, *t)).collect(),
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            materialized: None,
            checks: Vec::new(),
        }
    }

    fn two_table_scope() -> Scope {
        let a = tbl("a", &[("id", ColumnType::Int4), ("v", ColumnType::Int4)]);
        let b = tbl("b", &[("id", ColumnType::Int4), ("w", ColumnType::Text)]);
        let mut scope = Scope::single(&a, "a");
        scope.columns.extend(Scope::single(&b, "b").columns);
        scope
    }

    fn parse(sql: &str) -> Expr {
        crabka_pgparser::parser::parse_expression(sql).expect("expression parses")
    }

    fn column(table: Option<&str>, name: &str) -> Expr {
        Expr::Column {
            table: table.map(ToString::to_string),
            name: name.to_string(),
        }
    }

    #[test]
    fn resolvable_references_become_positional() {
        let scope = two_table_scope();
        let cases: Vec<(&str, Expr)> = vec![
            ("a.id", column(Some(POSITION_QUALIFIER), "0")),
            ("a.v", column(Some(POSITION_QUALIFIER), "1")),
            ("b.id", column(Some(POSITION_QUALIFIER), "2")),
            ("b.w", column(Some(POSITION_QUALIFIER), "3")),
            // A bare name that is unique in the scope binds like a qualified one.
            ("v", column(Some(POSITION_QUALIFIER), "1")),
        ];
        for (sql, expected) in cases {
            assert!(
                BoundExpr::new(&parse(sql), &scope) == BoundExpr(expected),
                "binding {sql}"
            );
        }
    }

    #[test]
    fn unresolvable_references_are_left_as_written() {
        let scope = two_table_scope();
        // Ambiguous (`id` is in both), unknown qualifier, and unknown name: each
        // is an error `resolve` would report, and each survives binding intact so
        // that evaluation reports it at the same point it always did.
        for sql in ["id", "c.id", "a.nope"] {
            let expr = parse(sql);
            assert!(
                BoundExpr::new(&expr, &scope) == BoundExpr(expr.clone()),
                "binding {sql}"
            );
        }
    }

    #[test]
    fn binding_rewrites_nested_references() {
        let scope = two_table_scope();
        let bound = BoundExpr::new(&parse("a.v + b.id"), &scope);
        let expected = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(column(Some(POSITION_QUALIFIER), "1")),
            right: Box::new(column(Some(POSITION_QUALIFIER), "2")),
        };
        assert!(bound == BoundExpr(expected));
    }

    #[test]
    fn a_subquery_keeps_its_own_scope() {
        let scope = two_table_scope();
        // `a.v` outside the subquery binds; nothing inside the subquery may,
        // because the inner query resolves against its own scope.
        let expr = parse("a.v IN (SELECT id FROM b)");
        let Expr::InSubquery {
            expr: outer,
            subquery,
            negated,
        } = BoundExpr::new(&expr, &scope).0
        else {
            panic!("binding preserves the node shape");
        };
        assert!(*outer == column(Some(POSITION_QUALIFIER), "1"));
        assert!(!negated);
        let Expr::InSubquery {
            subquery: original, ..
        } = &expr
        else {
            unreachable!("parsed as an IN-subquery")
        };
        assert!(subquery == *original);
    }

    #[test]
    fn a_niladic_keyword_is_not_claimed_as_a_column() {
        // `current_schema` evaluates as a call before it resolves as a column,
        // so it must survive binding even when the scope holds that name.
        let t = tbl("t", &[("current_schema", ColumnType::Text)]);
        let scope = Scope::single(&t, "t");
        let expr = column(None, "current_schema");
        assert!(BoundExpr::new(&expr, &scope) == BoundExpr(expr.clone()));
        // The qualified spelling is an ordinary column and does bind.
        assert!(
            BoundExpr::new(&column(Some("t"), "current_schema"), &scope)
                == BoundExpr(column(Some(POSITION_QUALIFIER), "0"))
        );
    }

    /// The property that makes the optimization safe: for every row, the bound
    /// expression evaluates to exactly what the unbound one does.
    #[test]
    fn bound_and_unbound_evaluation_agree() {
        let scope = two_table_scope();
        let ctx = EvalCtx::test_default();
        let rows: Vec<Vec<Datum>> = vec![
            vec![
                Datum::Int4(1),
                Datum::Int4(10),
                Datum::Int4(1),
                Datum::Text("x".into()),
            ],
            vec![Datum::Int4(2), Datum::Int4(20), Datum::Int4(9), Datum::Null],
        ];
        let sqls = [
            "a.v + b.id",
            "a.id = b.id",
            "CASE WHEN a.v > 15 THEN b.w ELSE 'small' END",
            "a.v BETWEEN 5 AND 15",
            "b.w IS NULL",
            "coalesce(b.w, 'none')",
            // Unbindable references keep reporting the same error per row.
            "id = 1",
            "c.id = 1",
            // Evaluation short-circuits a CASE and binding does not, so a dead
            // arm naming a column that cannot resolve must still be left alone.
            "CASE WHEN a.v > 1000 THEN nosuchcol ELSE a.id END",
            "CASE WHEN a.v > 1000 THEN id ELSE a.id END",
        ];
        for sql in sqls {
            let expr = parse(sql);
            let bound = BoundExpr::new(&expr, &scope);
            for row in &rows {
                let plain = crate::eval::eval(&expr, &scope, row, &ctx);
                let positional = crate::eval::eval(bound.expr(), &scope, row, &ctx);
                assert!(plain == positional, "evaluating {sql}");
            }
        }
    }
}
