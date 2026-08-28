use super::*;

/// The immediate sub-expressions of `expr` — including those reached through a
/// subquery, which are visited by way of the subquery's own clauses.
pub(crate) fn expr_children(expr: &Expr) -> Vec<&Expr> {
    let mut owned: Vec<&Expr> = Vec::new();
    match expr {
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Quantified { expr, .. }
        | Expr::InSubquery { expr, .. } => owned.push(expr),
        Expr::Binary { left, right, .. } => owned.extend([left.as_ref(), right.as_ref()]),
        Expr::Func(call) => {
            if let FuncArgs::Exprs(args) = &call.args {
                owned.extend(args);
            }
            owned.extend(call.order_by.iter().map(|item| &item.expr));
            owned.extend(call.filter.iter().map(std::convert::AsRef::as_ref));
        }
        Expr::InList { expr, list, .. } => {
            owned.push(expr);
            owned.extend(list);
        }
        Expr::Between {
            expr, low, high, ..
        } => owned.extend([expr.as_ref(), low.as_ref(), high.as_ref()]),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            owned.extend([expr.as_ref(), pattern.as_ref()]);
            owned.extend(escape.iter().map(std::convert::AsRef::as_ref));
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => {
            owned.extend(operand.iter().map(std::convert::AsRef::as_ref));
            owned.extend(whens.iter().flat_map(|(when, then)| [when, then]));
            owned.extend(else_result.iter().map(std::convert::AsRef::as_ref));
        }
        Expr::QuantifiedArray { expr, array, .. } => {
            owned.extend([expr.as_ref(), array.as_ref()]);
        }
        Expr::ArrayLiteral(items) | Expr::Row(items) => owned.extend(items),
        Expr::Subscript { base, index } => owned.extend([base.as_ref(), index.as_ref()]),
        Expr::ArrayRef { base, subscripts } => {
            owned.push(base.as_ref());
            owned.extend(subscripts.iter().flat_map(ArraySubscript::bounds));
        }
        Expr::ArraySubquery(_) => {}
        Expr::FieldSelect { base, .. } | Expr::FieldSelectAll(base) => owned.push(base.as_ref()),
        Expr::SqlJson(json) => owned.extend(json.children()),
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BitStringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Column { .. }
        | Expr::Param(_)
        | Expr::Default
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::Const { .. } => {}
    }
    owned
}

/// The mutable counterpart of [`expr_children`].
pub(crate) fn expr_children_mut(expr: &mut Expr) -> Vec<&mut Expr> {
    let mut owned: Vec<&mut Expr> = Vec::new();
    match expr {
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Quantified { expr, .. }
        | Expr::InSubquery { expr, .. } => owned.push(expr),
        Expr::Binary { left, right, .. } => owned.extend([left.as_mut(), right.as_mut()]),
        Expr::Func(call) => {
            if let FuncArgs::Exprs(args) = &mut call.args {
                owned.extend(args);
            }
            owned.extend(call.order_by.iter_mut().map(|item| &mut item.expr));
            owned.extend(call.filter.iter_mut().map(std::convert::AsMut::as_mut));
        }
        Expr::InList { expr, list, .. } => {
            owned.push(expr);
            owned.extend(list);
        }
        Expr::Between {
            expr, low, high, ..
        } => owned.extend([expr.as_mut(), low.as_mut(), high.as_mut()]),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            owned.extend([expr.as_mut(), pattern.as_mut()]);
            owned.extend(escape.iter_mut().map(std::convert::AsMut::as_mut));
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => {
            owned.extend(operand.iter_mut().map(std::convert::AsMut::as_mut));
            owned.extend(whens.iter_mut().flat_map(|(when, then)| [when, then]));
            owned.extend(else_result.iter_mut().map(std::convert::AsMut::as_mut));
        }
        Expr::QuantifiedArray { expr, array, .. } => {
            owned.extend([expr.as_mut(), array.as_mut()]);
        }
        Expr::ArrayLiteral(items) | Expr::Row(items) => owned.extend(items),
        Expr::Subscript { base, index } => owned.extend([base.as_mut(), index.as_mut()]),
        Expr::ArrayRef { base, subscripts } => {
            owned.push(base.as_mut());
            owned.extend(subscripts.iter_mut().flat_map(ArraySubscript::bounds_mut));
        }
        Expr::ArraySubquery(_) => {}
        Expr::FieldSelect { base, .. } | Expr::FieldSelectAll(base) => owned.push(base.as_mut()),
        Expr::SqlJson(json) => owned.extend(json.children_mut()),
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BitStringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Column { .. }
        | Expr::Param(_)
        | Expr::Default
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::Const { .. } => {}
    }
    owned
}

/// The query expressions directly under `expr` (its subqueries).
///
/// [`expr_children`] stops at a subquery, because an inner query is its own
/// scope; a walk that has to see inside one — collecting the relations a view
/// body reads, say — continues through here.
pub(crate) fn query_children(expr: &Expr) -> Vec<&crabka_pgparser::ast::QueryExpr> {
    match expr {
        Expr::ScalarSubquery(query) | Expr::ArraySubquery(query) | Expr::Exists(query) => {
            vec![query]
        }
        Expr::InSubquery { subquery, .. } | Expr::Quantified { subquery, .. } => vec![subquery],
        _ => Vec::new(),
    }
}

/// The mutable counterpart of [`query_children`].
pub(crate) fn query_children_mut(expr: &mut Expr) -> Vec<&mut crabka_pgparser::ast::QueryExpr> {
    match expr {
        Expr::ScalarSubquery(query) | Expr::ArraySubquery(query) | Expr::Exists(query) => {
            vec![query]
        }
        Expr::InSubquery { subquery, .. } | Expr::Quantified { subquery, .. } => vec![subquery],
        _ => Vec::new(),
    }
}
