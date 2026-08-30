//! FROM predicate pushdown and filtering.

use super::*;

/// Apply immutable top-level WHERE conjuncts that bind to exactly one join side
/// before materializing an inner/cross product. The complete WHERE is evaluated
/// again after FROM, so this optimization cannot weaken filtering.
/// Apply the single-relation conjuncts of a `WHERE` to one side *before* the
/// join, so the join walks the rows that can still survive it.
///
/// This only ever pre-applies a filter the caller runs again over the joined
/// relation, so it can never keep a row the full `WHERE` would drop. What it
/// must not do is drop a row the `WHERE` would have kept, and that is what the
/// join kind decides: a predicate on a *null-preserved* side is safe, because
/// such a row carries its own columns unchanged into the output whether it
/// matched or was NULL-padded, so it fails the predicate either way. A
/// predicate on the *nullable* side is not — dropping those rows early would
/// suppress the NULL-padded row the outer join owes for an unmatched partner.
pub(crate) fn push_local_where(
    left: &mut Relation,
    right: &mut Relation,
    kind: crabka_pgparser::ast::JoinKind,
    filter: &Expr,
    ctx: &crate::clock::EvalCtx,
    left_is_security_free: bool,
    right_is_security_free: bool,
) -> Result<(), ExecError> {
    use crabka_pgparser::ast::JoinKind;
    push_left_where(left, kind, filter, ctx, left_is_security_free)?;
    let push_right = matches!(kind, JoinKind::Inner | JoinKind::Cross | JoinKind::Right);
    let mut conjuncts = Vec::new();
    collect_conjuncts(filter, &mut conjuncts);
    for conjunct in conjuncts {
        if !immutable_row_predicate(conjunct)
            || !(right_is_security_free || leakproof_predicate(conjunct))
        {
            continue;
        }
        let right_only = expr_references_scope(conjunct, &right.scope)
            && crate::eval::check_predicate_resolves(conjunct, &right.scope).is_ok();
        if right_only && push_right {
            filter_relation(right, conjunct, ctx)?;
        }
    }
    Ok(())
}

/// Apply safe single-relation `WHERE` conjuncts before a join preserves its
/// left side. This is also needed before LATERAL, which otherwise evaluates
/// its right side once for every left row before the final WHERE runs.
pub(crate) fn push_left_where(
    left: &mut Relation,
    kind: crabka_pgparser::ast::JoinKind,
    filter: &Expr,
    ctx: &crate::clock::EvalCtx,
    is_security_free: bool,
) -> Result<(), ExecError> {
    if !matches!(
        kind,
        crabka_pgparser::ast::JoinKind::Inner
            | crabka_pgparser::ast::JoinKind::Cross
            | crabka_pgparser::ast::JoinKind::Left
    ) {
        return Ok(());
    }
    let mut conjuncts = Vec::new();
    collect_conjuncts(filter, &mut conjuncts);
    for conjunct in conjuncts {
        if immutable_row_predicate(conjunct)
            && (is_security_free || leakproof_predicate(conjunct))
            && expr_references_scope(conjunct, &left.scope)
            && crate::eval::check_predicate_resolves(conjunct, &left.scope).is_ok()
        {
            filter_relation(left, conjunct, ctx)?;
        }
    }
    Ok(())
}

/// Whether a predicate may be evaluated *before* the rows a security policy
/// would hide — `PostgreSQL`'s `proleakproof`.
///
/// Row-level security makes the difference observable: the upstream
/// `rowsecurity` test defines an `f_leak(text)` that `RAISE NOTICE`s whatever it
/// is handed, and asserts it never sees a row the policy filters out. Pushing a
/// call to it under the policy would print exactly those titles. Casts and
/// division can leak through their error messages the same way, so only
/// columns, literals and the total operators are allowed across.
pub(crate) fn leakproof_predicate(expr: &Expr) -> bool {
    let mut leakproof = true;
    crate::grouping::visit_expr(expr, &mut |node| {
        leakproof &= !matches!(
            node,
            Expr::Func(_)
                | Expr::Cast { .. }
                | Expr::ScalarSubquery(_)
                | Expr::Exists(_)
                | Expr::InSubquery { .. }
                | Expr::Quantified { .. }
                | Expr::ArraySubquery(_)
        ) && !matches!(
            node,
            Expr::Binary {
                op: crabka_pgparser::ast::BinaryOp::Div | crabka_pgparser::ast::BinaryOp::Mod,
                ..
            }
        );
    });
    leakproof
}

pub(crate) fn immutable_row_predicate(expr: &Expr) -> bool {
    let mut immutable = !crate::agg::contains_aggregate(expr);
    crate::grouping::visit_expr(expr, &mut |node| {
        immutable &= !matches!(
            node,
            Expr::ScalarSubquery(_)
                | Expr::Exists(_)
                | Expr::InSubquery { .. }
                | Expr::Quantified { .. }
                | Expr::ArraySubquery(_)
        ) && !matches!(node, Expr::Func(call) if !is_immutable_function(&call.name));
    });
    immutable
}

/// The immutable, safe `WHERE` conjuncts that an inner join over `scope` can
/// already evaluate. The residual `WHERE` remains above the join; this only
/// avoids materializing rows that it will discard.
pub(crate) fn inner_join_predicate(
    filter: Option<&Expr>,
    scope: &Scope,
    security_free: bool,
) -> Option<Expr> {
    let mut conjuncts = Vec::new();
    collect_conjuncts(filter?, &mut conjuncts);
    conjuncts
        .into_iter()
        .filter(|conjunct| {
            immutable_row_predicate(conjunct)
                && (security_free || leakproof_predicate(conjunct))
                && expr_references_scope(conjunct, scope)
                && crate::eval::check_predicate_resolves(conjunct, scope).is_ok()
        })
        .cloned()
        .reduce(|left, right| Expr::Binary {
            op: crabka_pgparser::ast::BinaryOp::And,
            left: Box::new(left),
            right: Box::new(right),
        })
}

pub(crate) fn filter_relation(
    relation: &mut Relation,
    predicate: &Expr,
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    let rows = std::mem::take(&mut relation.rows);
    let predicate = crate::bind::BoundExpr::new(predicate, &relation.scope)?;
    for row in rows {
        if row_matches(Some(predicate.expr()), &relation.scope, &row, ctx)? {
            relation.rows.push(row);
        }
    }
    Ok(())
}
