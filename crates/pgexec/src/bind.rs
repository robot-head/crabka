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
//! # Binding decides the error, once, for the whole statement
//!
//! Resolution is a per-statement question, so binding answers it once and
//! reports the failure: a reference that does not resolve — or resolves
//! ambiguously — is a 42703/42702/42P01 for the statement, whether or not any
//! row ever reaches the expression. That is what `PostgreSQL` does, at parse
//! analysis, and it is the only answer that does not depend on the data.
//!
//! Binding used to swallow the failure and leave the reference as written, so
//! that evaluation raised it per row instead. The cost was silence wherever no
//! row evaluated the reference — and, because `AND` short-circuits in
//! [`crate::eval`], that is not only the empty relation:
//!
//! ```sql
//! CREATE TABLE g (a int, b int); INSERT INTO g VALUES (1,1),(2,2),(3,3);
//! SELECT a FROM g WHERE a = 1  AND nosuchcol = 1;  -- 42703
//! SELECT a FROM g WHERE a = 99 AND nosuchcol = 1;  -- was 0 rows, now 42703
//! DELETE FROM g WHERE a = 99 AND nosuchcol = 1;    -- was DELETE 0, now 42703
//! ```
//!
//! A `DELETE` whose predicate was misspelled answered success. Reporting at
//! bind time costs nothing to measure: binding already called
//! [`Scope::resolve`] for every reference and discarded the `Err`, and the
//! reference it left behind was re-resolved by name on every row after that. So
//! the fix removes work rather than adding it.
//!
//! Two references still do not resolve against the scope and must survive
//! binding, because [`crate::eval`] answers them itself:
//!
//! - a no-paren niladic keyword (`current_schema`), which `eval` tries as a
//!   call before it tries the scope; and
//! - a whole-row reference (`SELECT g FROM g`), a bare name that is a relation
//!   in the `FROM` clause rather than a column of it.
//!
//! A correlated expression is not bound at all — its references are resolved
//! per row against a [`crate::scope::CORRELATED_QUALIFIER`] scope built from
//! the outer row — so a typo inside one is still reported per row.
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
    /// Bind `expr` against `scope`, reporting the first reference that does not
    /// resolve.
    pub(crate) fn new(expr: &Expr, scope: &Scope) -> Result<Self, ExecError> {
        bind(expr, scope).map(Self)
    }

    /// The rewritten expression, to hand to `eval` against the same scope.
    pub(crate) fn expr(&self) -> &Expr {
        &self.0
    }
}

/// [`BoundExpr::new`] over a list, preserving order.
pub(crate) fn bind_all(exprs: &[Expr], scope: &Scope) -> Result<Vec<BoundExpr>, ExecError> {
    exprs.iter().map(|e| BoundExpr::new(e, scope)).collect()
}

/// [`BoundExpr::new`] over an optional expression.
pub(crate) fn bind_optional(
    expr: Option<&Expr>,
    scope: &Scope,
) -> Result<Option<BoundExpr>, ExecError> {
    expr.map(|e| BoundExpr::new(e, scope)).transpose()
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
///
/// The walk is pre-order and left to right, and `rewrite` propagates the first
/// `Err` the fold returns, so a statement naming two unresolvable columns
/// reports the leftmost — as `PostgreSQL` does.
fn bind(expr: &Expr, scope: &Scope) -> Result<Expr, ExecError> {
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
        match scope.resolve(table.as_deref(), name) {
            Ok(index) => Ok(Some(positional_reference(index))),
            // A bare name that is no column may still name a relation of the
            // FROM clause, and then it is that relation's whole row. It has no
            // single position, so it survives binding for `eval` to answer.
            Err(error) => {
                if matches!(error, ExecError::UndefinedColumn(_))
                    && ((table.is_none() && scope.whole_row(name).is_some())
                        || (name == "*"
                            && table
                                .as_deref()
                                .is_some_and(|qualifier| scope.whole_row(qualifier).is_some())))
                {
                    return Ok(None);
                }
                Err(error)
            }
        }
    };
    crate::grouping::rewrite(expr, &mut fold, false)
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
                BoundExpr::new(&parse(sql), &scope) == Ok(BoundExpr(expected)),
                "binding {sql}"
            );
        }
    }

    #[test]
    fn an_unresolvable_reference_is_a_binder_error() {
        let scope = two_table_scope();
        // Ambiguous (`id` is in both), unknown qualifier, and unknown name: each
        // is reported for the statement, whether or not a row would have reached
        // the reference.
        let cases = [
            ("id", ExecError::AmbiguousColumn("id".into())),
            ("c.id", ExecError::MissingFromEntry("c".into())),
            ("a.nope", ExecError::UndefinedColumn("nope".into())),
            ("nope", ExecError::UndefinedColumn("nope".into())),
            // A dead arm is bound like any other: binding is per statement, and
            // evaluation never reaches this one.
            (
                "CASE WHEN a.v > 1000 THEN nosuchcol ELSE a.id END",
                ExecError::UndefinedColumn("nosuchcol".into()),
            ),
        ];
        for (sql, expected) in cases {
            assert!(
                BoundExpr::new(&parse(sql), &scope) == Err(expected),
                "binding {sql}"
            );
        }
    }

    #[test]
    fn a_whole_row_reference_survives_binding() {
        // `a` is no column of the scope, but it is a relation in it, so it is
        // that relation's whole row and `eval` answers it. It has no single
        // position, so binding must leave it alone rather than report it.
        let scope = two_table_scope();
        let expr = column(None, "a");
        assert!(BoundExpr::new(&expr, &scope) == Ok(BoundExpr(expr.clone())));
        // `a.*` is also a whole row, even though `a` is the function item's
        // output column name. Its internal spelling must survive binding.
        let expr = column(Some("a"), "*");
        assert!(BoundExpr::new(&expr, &scope) == Ok(BoundExpr(expr.clone())));
        // A name that is neither a column nor a relation is still an error.
        assert!(
            BoundExpr::new(&column(None, "nosuchrel"), &scope)
                == Err(ExecError::UndefinedColumn("nosuchrel".into()))
        );
        assert!(
            BoundExpr::new(&column(Some("a"), "nope"), &scope)
                == Err(ExecError::UndefinedColumn("nope".into()))
        );
        assert!(
            BoundExpr::new(&column(Some("nosuchrel"), "*"), &scope)
                == Err(ExecError::MissingFromEntry("nosuchrel".into()))
        );
    }

    #[test]
    fn binding_rewrites_nested_references() {
        let scope = two_table_scope();
        let bound = BoundExpr::new(&parse("a.v + b.id"), &scope).expect("both sides resolve");
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
        } = BoundExpr::new(&expr, &scope)
            .expect("the outer reference resolves")
            .0
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
        assert!(BoundExpr::new(&expr, &scope) == Ok(BoundExpr(expr.clone())));
        // The qualified spelling is an ordinary column and does bind.
        assert!(
            BoundExpr::new(&column(Some("t"), "current_schema"), &scope)
                == Ok(BoundExpr(column(Some(POSITION_QUALIFIER), "0")))
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
            // A whole-row reference survives binding, so the two forms are the
            // same expression and must still agree.
            "a IS NULL",
        ];
        for sql in sqls {
            let expr = parse(sql);
            let bound = BoundExpr::new(&expr, &scope).expect("every reference resolves");
            for row in &rows {
                let plain = crate::eval::eval(&expr, &scope, row, &ctx);
                let positional = crate::eval::eval(bound.expr(), &scope, row, &ctx);
                assert!(plain == positional, "evaluating {sql}");
            }
        }
    }
}
