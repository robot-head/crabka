//! F-2: reconstruct a view's SQL text the way PostgreSQL's rule deparser
//! `ruleutils.c` does. `pg_get_viewdef` therefore answers a *normalized*
//! definition and does not echo what the user typed.
//!
//! The layout PostgreSQL produces is fixed and worth writing down, because it
//! is what clients diff against:
//!
//! ```text
//!  SELECT b,
//!     count(*) AS count
//!    FROM t
//!   WHERE (a > 0)
//!   GROUP BY b
//!  HAVING (count(*) > 1)
//!   ORDER BY b
//!  LIMIT 5
//! ```
//!
//! Each clause keyword sits at its own fixed indent. Continuation lines of the
//! select list have an indent of four. Two behaviors depend on the pretty flag.
//! Without the flag, every operator expression is fully parenthesized and a
//! join tree is wrapped in parentheses. With the flag, neither is.
//! `pg_get_viewdef(oid)` is the un-pretty form. `pg_get_viewdef(oid, true)` and
//! the wrap-column overload are the pretty one.
//!
//! This module reproduces two of PostgreSQL's rules exactly, because they
//! decide whether the text round-trips:
//!
//! * **Column qualification.** A reference is written `tbl.col` whenever the
//!   query has other than exactly one range-table entry, or the query is nested
//!   inside another. This matches `get_query_def`'s `varprefix`.
//! * **Output naming.** A bare column reference whose name already equals the
//!   view's column name is written without `AS`; everything else always carries
//!   one.

use std::fmt::Write as _;

use crabka_pgparser::ast::{
    BinaryOp, DistinctClause, Expr, FuncArgs, FuncCall, JoinConstraint, JoinKind, OrderItem,
    QueryBody, QueryExpr, SelectItem, SelectStmt, SetExpr, SetOp, TableExpr, UnaryOp, ValuesStmt,
};
use crabka_pgtypes::{ColumnType, Datum};

use crate::catalog_fn::quote_identifier;

/// Deparser state: the pretty flag and whether column references need their
/// relation prefix.
#[derive(Debug, Clone, Copy)]
struct Ctx<'a> {
    pretty: bool,
    qualify: bool,
    /// The relation an unqualified column reference belongs to, when the query
    /// has exactly one FROM item. PostgreSQL resolves the prefix from its range
    /// table. The parse tree keeps no such link, so the sole FROM item is the
    /// only case where the prefix can be recovered.
    qualifier: Option<&'a str>,
    /// The column the select list is packed to, from the
    /// `pg_get_viewdef(oid, integer)` overload. `None`, which is every other
    /// overload, puts one output column per line.
    wrap: Option<usize>,
}

impl Ctx<'_> {
    /// Wrap `inner` in the parentheses PostgreSQL adds only in un-pretty mode.
    fn paren(self, inner: String) -> String {
        if self.pretty {
            inner
        } else {
            format!("({inner})")
        }
    }
}

/// Write a whole query expression, and name its output columns from `names`.
pub(crate) fn write_query(
    out: &mut String,
    query: &QueryExpr,
    names: &[String],
    pretty: bool,
    wrap: Option<usize>,
) {
    let ctx = Ctx {
        pretty,
        qualify: false,
        qualifier: None,
        wrap,
    };
    write_set_expr(out, &query.body, names, ctx);
    write_query_tail(out, query, ctx);
}

fn write_query_tail(out: &mut String, query: &QueryExpr, ctx: Ctx<'_>) {
    if !query.order_by.is_empty() {
        let _ = write!(out, "\n  ORDER BY {}", order_list(&query.order_by, ctx));
    }
    if let Some(limit) = &query.limit {
        let _ = write!(out, "\n LIMIT {}", expr_text(limit, ctx));
    }
    if let Some(offset) = &query.offset {
        let _ = write!(out, "\n OFFSET {}", expr_text(offset, ctx));
    }
}

fn write_set_expr(out: &mut String, body: &SetExpr, names: &[String], ctx: Ctx<'_>) {
    match body {
        SetExpr::Query(QueryBody::Select(select)) => write_select(out, select, names, ctx),
        SetExpr::Query(QueryBody::Values(values)) => write_values(out, values, ctx),
        SetExpr::Query(QueryBody::Nested(query)) => {
            let nested = Ctx {
                qualify: true,
                ..ctx
            };
            write_set_expr(out, &query.body, names, nested);
            write_query_tail(out, query, nested);
        }
        SetExpr::SetOp {
            op,
            all,
            left,
            right,
        } => {
            // Every arm of a set operation is a sub-query, which is exactly the
            // condition PostgreSQL qualifies column references under.
            let arm = Ctx {
                qualify: true,
                ..ctx
            };
            write_set_expr(out, left, names, arm);
            let keyword = match op {
                SetOp::Union => "UNION",
                SetOp::Intersect => "INTERSECT",
                SetOp::Except => "EXCEPT",
            };
            let _ = write!(out, "\n{keyword}{}", if *all { " ALL" } else { "" });
            out.push('\n');
            write_set_expr(out, right, names, arm);
        }
    }
}

fn write_values(out: &mut String, values: &ValuesStmt, ctx: Ctx<'_>) {
    let rows = values
        .rows
        .iter()
        .map(|row| {
            let cells = row
                .iter()
                .map(|cell| expr_text(cell, ctx))
                .collect::<Vec<_>>()
                .join(", ");
            format!("({cells})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let _ = write!(out, " VALUES {rows}");
}

fn write_select(out: &mut String, select: &SelectStmt, names: &[String], ctx: Ctx<'_>) {
    let ctx = Ctx {
        qualify: ctx.qualify || range_table_len(&select.from) != 1,
        qualifier: sole_from_name(&select.from),
        ..ctx
    };
    out.push_str(" SELECT");
    write_distinct(out, &select.distinct, ctx);
    write_target_list(out, &target_list(select, names, ctx), ctx);
    if !select.from.is_empty() {
        let from = select
            .from
            .iter()
            .map(|item| from_text(item, ctx))
            .collect::<Vec<_>>()
            .join(",\n     ");
        let _ = write!(out, "\n   FROM {from}");
    }
    if let Some(filter) = &select.filter {
        let _ = write!(out, "\n  WHERE {}", expr_text(filter, ctx));
    }
    if !select.group_by.is_empty() {
        let list = select
            .group_by
            .iter()
            .map(|expr| expr_text(expr, ctx))
            .collect::<Vec<_>>()
            .join(", ");
        let _ = write!(out, "\n  GROUP BY {list}");
    }
    if let Some(having) = &select.having {
        let _ = write!(out, "\n HAVING {}", expr_text(having, ctx));
    }
    if !select.order_by.is_empty() {
        let _ = write!(out, "\n  ORDER BY {}", order_list(&select.order_by, ctx));
    }
    if let Some(limit) = &select.limit {
        let _ = write!(out, "\n LIMIT {}", expr_text(limit, ctx));
    }
    if let Some(offset) = &select.offset {
        let _ = write!(out, "\n OFFSET {}", expr_text(offset, ctx));
    }
}

fn write_distinct(out: &mut String, distinct: &DistinctClause, ctx: Ctx<'_>) {
    match distinct {
        DistinctClause::All => out.push(' '),
        DistinctClause::Distinct => out.push_str(" DISTINCT "),
        DistinctClause::On(keys) => {
            let list = keys
                .iter()
                .map(|expr| expr_text(expr, ctx))
                .collect::<Vec<_>>()
                .join(", ");
            let _ = write!(out, " DISTINCT ON ({list}) ");
        }
    }
}

/// PostgreSQL's `list_length(query->rtable)`: one entry per base relation plus
/// one per join node.
fn range_table_len(from: &[TableExpr]) -> usize {
    from.iter().map(range_table_len_of).sum()
}

fn range_table_len_of(item: &TableExpr) -> usize {
    match item {
        TableExpr::Join { left, right, .. } => {
            1 + range_table_len_of(left) + range_table_len_of(right)
        }
        _ => 1,
    }
}

/// The select list, one rendered item per output column.
fn target_list(select: &SelectStmt, names: &[String], ctx: Ctx<'_>) -> Vec<String> {
    let mut out = Vec::new();
    let mut next = names.iter();
    for item in &select.projection {
        match item {
            // `*` expands to the view's own column list; the qualifier is the
            // sole FROM item's name when there is exactly one.
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                let qualifier = match item {
                    SelectItem::QualifiedWildcard(name) => Some(name.as_str()),
                    _ => ctx.qualifier,
                };
                for name in next.by_ref() {
                    out.push(match (qualifier, ctx.qualify) {
                        (Some(prefix), true) => {
                            format!("{}.{}", quote_identifier(prefix), quote_identifier(name))
                        }
                        _ => quote_identifier(name),
                    });
                }
            }
            SelectItem::Expr { expr, alias } => {
                // The view's catalog column list names one output column per
                // projected item, whether or not the item was written with an
                // alias — so the cursor advances either way.
                let catalog_name = next.next().cloned();
                let target = alias.clone().or(catalog_name).unwrap_or_default();
                out.push(target_item(expr, &target, ctx));
            }
        }
    }
    if out.is_empty() {
        out.push(String::new());
    }
    out
}

/// Append the rendered select list.
///
/// Without a wrap column, each entry gets its own line at PostgreSQL's
/// four-space continuation indent. With a wrap column, this function packs
/// entries greedily and breaks a line before the entry that would cross it.
fn write_target_list(out: &mut String, items: &[String], ctx: Ctx<'_>) {
    let mut line_start = out.rfind('\n').map_or(0, |at| at + 1);
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        let fits = ctx
            .wrap
            .is_some_and(|wrap| out.len() - line_start + 1 + item.len() <= wrap);
        if index > 0 && !fits {
            out.push_str("\n    ");
            line_start = out.len() - 4;
        } else if index > 0 {
            out.push(' ');
        }
        out.push_str(item);
    }
}

/// One select-list entry. A bare column reference that already carries the
/// output name needs no `AS`. PostgreSQL writes one for everything else.
fn target_item(expr: &Expr, target: &str, ctx: Ctx<'_>) -> String {
    let text = expr_text(expr, ctx);
    if target.is_empty() {
        return text;
    }
    if let Expr::Column { name, .. } = expr
        && name == target
    {
        return text;
    }
    format!("{text} AS {}", quote_identifier(target))
}

/// The name of the only FROM item, when there is exactly one plain table.
fn sole_from_name(from: &[TableExpr]) -> Option<&str> {
    match from {
        [TableExpr::Table { name, alias, .. }] => Some(alias.as_deref().unwrap_or(&name.name)),
        _ => None,
    }
}

fn order_list(items: &[OrderItem], ctx: Ctx<'_>) -> String {
    items
        .iter()
        .map(|item| {
            let mut text = expr_text(&item.expr, ctx);
            if !item.asc {
                text.push_str(" DESC");
            }
            // PostgreSQL prints the NULLS clause only when it is not the
            // default for the direction.
            if item.asc && item.nulls_first {
                text.push_str(" NULLS FIRST");
            } else if !item.asc && !item.nulls_first {
                text.push_str(" NULLS LAST");
            }
            text
        })
        .collect::<Vec<_>>()
        .join(", ")
}

// ------------------------------------------------------------- FROM rendering

fn from_text(item: &TableExpr, ctx: Ctx<'_>) -> String {
    match item {
        TableExpr::Table { name, alias, .. } => {
            // PostgreSQL deparses a view body under the creator's search path,
            // printing the relation unqualified when the path reaches it.
            let base = quote_identifier(&name.name);
            alias.as_ref().map_or(base.clone(), |alias| {
                format!("{base} {}", quote_identifier(alias))
            })
        }
        TableExpr::Derived {
            subquery,
            alias,
            lateral,
            ..
        } => {
            let mut inner = String::new();
            let nested = Ctx {
                qualify: true,
                ..ctx
            };
            write_set_expr(&mut inner, &subquery.body, &[], nested);
            write_query_tail(&mut inner, subquery, nested);
            format!(
                "{}({inner}) {}",
                if *lateral { "LATERAL " } else { "" },
                quote_identifier(alias)
            )
        }
        TableExpr::Join { .. } => {
            let inner = join_text(item, ctx);
            if ctx.pretty {
                inner
            } else {
                format!("({inner})")
            }
        }
        TableExpr::Function { functions, .. } => functions
            .iter()
            .map(|call| {
                let args = call
                    .args
                    .iter()
                    .map(|arg| expr_text(arg, ctx))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}({args})", call.name)
            })
            .collect::<Vec<_>>()
            .join(", "),
    }
}

/// A join tree: the left side on the FROM line, each join on its own line at
/// PostgreSQL's five-space indent.
fn join_text(item: &TableExpr, ctx: Ctx<'_>) -> String {
    let TableExpr::Join {
        left,
        right,
        kind,
        constraint,
    } = item
    else {
        return from_text(item, ctx);
    };
    let keyword = match kind {
        JoinKind::Inner => "JOIN",
        JoinKind::Left => "LEFT JOIN",
        JoinKind::Right => "RIGHT JOIN",
        JoinKind::Full => "FULL JOIN",
        JoinKind::Cross => "CROSS JOIN",
    };
    let tail = match constraint {
        JoinConstraint::On(expr) => format!(" ON {}", ctx.paren(expr_text(expr, ctx))),
        JoinConstraint::Using(columns) => format!(
            " USING ({})",
            columns
                .iter()
                .map(|column| quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        JoinConstraint::Natural | JoinConstraint::None => String::new(),
    };
    let natural = matches!(constraint, JoinConstraint::Natural);
    format!(
        "{}\n     {}{keyword} {}{tail}",
        join_text(left, ctx),
        if natural { "NATURAL " } else { "" },
        from_text(right, ctx)
    )
}

// ------------------------------------------------------- expression rendering

/// Render an expression. In un-pretty mode this function wraps every operator
/// node in parentheses, which is what PostgreSQL's `PRETTYFLAG_PAREN`-off path
/// does.
fn expr_text(expr: &Expr, ctx: Ctx<'_>) -> String {
    match expr {
        Expr::IntLiteral(text) | Expr::NumericLiteral(text) => text.clone(),
        Expr::StringLiteral(text) => format!("'{}'::text", text.replace('\'', "''")),
        Expr::BoolLiteral(value) => (if *value { "true" } else { "false" }).to_string(),
        Expr::NullLiteral => "NULL::text".to_string(),
        Expr::Default => "DEFAULT".to_string(),
        Expr::Param(n) => format!("${n}"),
        Expr::Column { table, name } => match (table.as_deref().or(ctx.qualifier), ctx.qualify) {
            (Some(prefix), true) => {
                format!("{}.{}", quote_identifier(prefix), quote_identifier(name))
            }
            _ => quote_identifier(name),
        },
        Expr::Const { value, ty } => const_text(value, *ty),
        Expr::Unary { op, expr } => unary_text(*op, expr, ctx),
        Expr::Binary { op, left, right } => ctx.paren(format!(
            "{} {} {}",
            expr_text(left, ctx),
            binary_op_text(*op),
            expr_text(right, ctx)
        )),
        Expr::Func(call) => func_text(call, ctx),
        Expr::IsNull { expr, negated } => ctx.paren(format!(
            "{} IS {}NULL",
            expr_text(expr, ctx),
            if *negated { "NOT " } else { "" }
        )),
        Expr::Cast { expr, ty } => cast_text(expr, *ty, ctx),
        _ => expr_text_rest(expr, ctx),
    }
}

/// The remaining expression node kinds.
fn expr_text_rest(expr: &Expr, ctx: Ctx<'_>) -> String {
    match expr {
        // PostgreSQL rewrites a value list into `= ANY (ARRAY[...])` before the
        // rule is stored, so that is what the deparser prints.
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let items = list
                .iter()
                .map(|item| expr_text(item, ctx))
                .collect::<Vec<_>>()
                .join(", ");
            ctx.paren(format!(
                "{} {} ANY (ARRAY[{items}])",
                expr_text(expr, ctx),
                if *negated { "<>" } else { "=" }
            ))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => ctx.paren(format!(
            "{} {}BETWEEN {} AND {}",
            expr_text(expr, ctx),
            if *negated { "NOT " } else { "" },
            expr_text(low, ctx),
            expr_text(high, ctx)
        )),
        Expr::Like {
            expr,
            pattern,
            negated,
            ..
        } => ctx.paren(format!(
            "{} {}~~ {}",
            expr_text(expr, ctx),
            if *negated { "!" } else { "" },
            expr_text(pattern, ctx)
        )),
        Expr::Case {
            operand,
            whens,
            else_result,
        } => case_text(operand.as_deref(), whens, else_result.as_deref(), ctx),
        Expr::ArrayLiteral(items) => format!(
            "ARRAY[{}]",
            items
                .iter()
                .map(|item| expr_text(item, ctx))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expr::Row(items) => format!(
            "ROW({})",
            items
                .iter()
                .map(|item| expr_text(item, ctx))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expr::Subscript { base, index } => {
            format!("{}[{}]", expr_text(base, ctx), expr_text(index, ctx))
        }
        Expr::ScalarSubquery(query) => subquery_text(query, ctx),
        Expr::Exists(query) => ctx.paren(format!("EXISTS {}", subquery_text(query, ctx))),
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => ctx.paren(format!(
            "{} {}IN {}",
            expr_text(expr, ctx),
            if *negated { "NOT " } else { "" },
            subquery_text(subquery, ctx)
        )),
        Expr::Quantified {
            expr,
            op,
            all,
            subquery,
        } => ctx.paren(format!(
            "{} {} {} {}",
            expr_text(expr, ctx),
            binary_op_text(*op),
            if *all { "ALL" } else { "ANY" },
            subquery_text(subquery, ctx)
        )),
        Expr::QuantifiedArray {
            expr,
            op,
            all,
            array,
        } => ctx.paren(format!(
            "{} {} {} ({})",
            expr_text(expr, ctx),
            binary_op_text(*op),
            if *all { "ALL" } else { "ANY" },
            expr_text(array, ctx)
        )),
        _ => "?column?".to_string(),
    }
}

fn subquery_text(query: &QueryExpr, ctx: Ctx<'_>) -> String {
    let mut inner = String::new();
    let nested = Ctx {
        qualify: true,
        ..ctx
    };
    write_set_expr(&mut inner, &query.body, &[], nested);
    write_query_tail(&mut inner, query, nested);
    format!("({inner})")
}

fn unary_text(op: UnaryOp, expr: &Expr, ctx: Ctx<'_>) -> String {
    match op {
        UnaryOp::Neg => ctx.paren(format!("- {}", expr_text(expr, ctx))),
        UnaryOp::Plus => ctx.paren(format!("+ {}", expr_text(expr, ctx))),
        UnaryOp::Not => ctx.paren(format!("NOT {}", expr_text(expr, ctx))),
        UnaryOp::Abs => ctx.paren(format!("@ {}", expr_text(expr, ctx))),
        UnaryOp::Sqrt => ctx.paren(format!("|/ {}", expr_text(expr, ctx))),
        UnaryOp::Cbrt => ctx.paren(format!("||/ {}", expr_text(expr, ctx))),
        UnaryOp::IsTrue => ctx.paren(format!("{} IS TRUE", expr_text(expr, ctx))),
        UnaryOp::IsNotTrue => ctx.paren(format!("{} IS NOT TRUE", expr_text(expr, ctx))),
        UnaryOp::IsFalse => ctx.paren(format!("{} IS FALSE", expr_text(expr, ctx))),
        UnaryOp::IsNotFalse => ctx.paren(format!("{} IS NOT FALSE", expr_text(expr, ctx))),
        UnaryOp::IsUnknown => ctx.paren(format!("{} IS UNKNOWN", expr_text(expr, ctx))),
        UnaryOp::IsNotUnknown => ctx.paren(format!("{} IS NOT UNKNOWN", expr_text(expr, ctx))),
        UnaryOp::BitNot => ctx.paren(format!("~ {}", expr_text(expr, ctx))),
        UnaryOp::TsNot => ctx.paren(format!("!! {}", expr_text(expr, ctx))),
    }
}

fn func_text(call: &FuncCall, ctx: Ctx<'_>) -> String {
    let args = match &call.args {
        FuncArgs::Star => "*".to_string(),
        FuncArgs::Exprs(exprs) => exprs
            .iter()
            .map(|arg| expr_text(arg, ctx))
            .collect::<Vec<_>>()
            .join(", "),
    };
    format!(
        "{}({}{args})",
        call.name,
        if call.distinct { "DISTINCT " } else { "" }
    )
}

/// A cast. PostgreSQL parenthesizes the operand in un-pretty mode (`(a)::text`)
/// and leaves it bare in pretty mode (`a::text`).
fn cast_text(expr: &Expr, ty: ColumnType, ctx: Ctx<'_>) -> String {
    let inner = expr_text(expr, ctx);
    let operand = if ctx.pretty || inner.starts_with('(') {
        inner
    } else {
        format!("({inner})")
    };
    format!("{operand}::{}", ty.name())
}

/// A `CASE` expression, on its own indented lines exactly as PostgreSQL prints
/// one inside a select list.
fn case_text(
    operand: Option<&Expr>,
    whens: &[(Expr, Expr)],
    else_result: Option<&Expr>,
    ctx: Ctx<'_>,
) -> String {
    let mut out = String::from("\n        CASE");
    if let Some(operand) = operand {
        let _ = write!(out, " {}", expr_text(operand, ctx));
    }
    for (when, then) in whens {
        let _ = write!(
            out,
            "\n            WHEN {} THEN {}",
            expr_text(when, ctx),
            expr_text(then, ctx)
        );
    }
    if let Some(result) = else_result {
        let _ = write!(out, "\n            ELSE {}", expr_text(result, ctx));
    }
    out.push_str("\n        END");
    out
}

/// A stored constant, in the `value::type` spelling PostgreSQL uses inside a
/// stored rule or a column default.
pub(crate) fn const_text(value: &Datum, ty: ColumnType) -> String {
    match value {
        Datum::Null => format!("NULL::{}", ty.name()),
        Datum::Bool(flag) => (if *flag { "true" } else { "false" }).to_string(),
        Datum::Int2(n) => n.to_string(),
        Datum::Int4(n) => n.to_string(),
        Datum::Int8(n) => n.to_string(),
        Datum::Float4(n) => n.to_string(),
        Datum::Float8(n) => n.to_string(),
        Datum::Numeric(n) => n.to_string(),
        other => {
            let rendered = crate::func::text_render(other, &jiff::tz::TimeZone::UTC);
            format!("'{}'::{}", rendered.replace('\'', "''"), ty.name())
        }
    }
}

fn binary_op_text(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::Mod => "%",
        BinaryOp::Pow => "^",
        BinaryOp::Concat => "||",
        BinaryOp::Eq => "=",
        BinaryOp::Ne => "<>",
        BinaryOp::Lt => "<",
        BinaryOp::Le => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::Ge => ">=",
        BinaryOp::And => "AND",
        BinaryOp::Or => "OR",
        BinaryOp::Match => "~",
        BinaryOp::MatchCi => "~*",
        BinaryOp::NotMatch => "!~",
        BinaryOp::NotMatchCi => "!~*",
        BinaryOp::IsDistinctFrom => "IS DISTINCT FROM",
        BinaryOp::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
        _ => binary_op_text_rest(op),
    }
}

fn binary_op_text_rest(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::JsonGet => "->",
        BinaryOp::JsonGetText => "->>",
        BinaryOp::JsonGetPath => "#>",
        BinaryOp::JsonGetPathText => "#>>",
        BinaryOp::Contains => "@>",
        BinaryOp::ContainedBy => "<@",
        BinaryOp::KeyExists => "?",
        BinaryOp::KeyExistsAny => "?|",
        BinaryOp::KeyExistsAll => "?&",
        BinaryOp::Overlaps => "&&",
        BinaryOp::BitAnd => "&",
        BinaryOp::BitOr => "|",
        BinaryOp::BitXor => "#",
        BinaryOp::Shl => "<<",
        BinaryOp::Shr => ">>",
        _ => "?",
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgcatalog::{Column, RelationName, View};
    use crabka_pgtypes::ColumnType;

    use crate::catalog_fn::view_definition_text;

    fn view(definition: &str, columns: &[&str]) -> View {
        View {
            name: RelationName::public("v"),
            definition: definition.into(),
            columns: columns
                .iter()
                .map(|name| Column::new(*name, ColumnType::Int4))
                .collect(),
        }
    }

    /// Each case is the definition as written, then the text PostgreSQL 18.4
    /// answers for `pg_get_viewdef(oid)` and `pg_get_viewdef(oid, true)`.
    #[test]
    fn deparses_the_shapes_postgres_normalizes() {
        let cases = [
            (
                "SELECT a, b FROM t WHERE a > 0",
                &["a", "b"][..],
                " SELECT a,\n    b\n   FROM t\n  WHERE (a > 0);",
                " SELECT a,\n    b\n   FROM t\n  WHERE a > 0;",
            ),
            (
                "SELECT a AS x, b FROM t",
                &["x", "b"][..],
                " SELECT a AS x,\n    b\n   FROM t;",
                " SELECT a AS x,\n    b\n   FROM t;",
            ),
            (
                "SELECT b, count(*) FROM t GROUP BY b HAVING count(*) > 1 ORDER BY b LIMIT 5",
                &["b", "count"][..],
                " SELECT b,\n    count(*) AS count\n   FROM t\n  GROUP BY b\n HAVING (count(*) > 1)\n  ORDER BY b\n LIMIT 5;",
                " SELECT b,\n    count(*) AS count\n   FROM t\n  GROUP BY b\n HAVING count(*) > 1\n  ORDER BY b\n LIMIT 5;",
            ),
            (
                "SELECT DISTINCT b FROM t WHERE a IN (1,2,3)",
                &["b"][..],
                " SELECT DISTINCT b\n   FROM t\n  WHERE (a = ANY (ARRAY[1, 2, 3]));",
                " SELECT DISTINCT b\n   FROM t\n  WHERE a = ANY (ARRAY[1, 2, 3]);",
            ),
        ];
        for (definition, columns, plain, pretty) in cases {
            let view = view(definition, columns);
            assert!(view_definition_text(&view, false) == plain, "{definition}");
            assert!(view_definition_text(&view, true) == pretty, "{definition}");
        }
    }

    /// A join qualifies every column reference and, without the pretty flag,
    /// wraps the whole join tree in parentheses.
    #[test]
    fn deparses_a_join_the_way_postgres_does() {
        let view = view(
            "SELECT t.a, u.d FROM t JOIN u ON t.a = u.a WHERE t.a > 1",
            &["a", "d"],
        );
        assert!(
            view_definition_text(&view, false)
                == " SELECT t.a,\n    u.d\n   FROM (t\n     JOIN u ON ((t.a = u.a)))\n  WHERE (t.a > 1);"
        );
        assert!(
            view_definition_text(&view, true)
                == " SELECT t.a,\n    u.d\n   FROM t\n     JOIN u ON t.a = u.a\n  WHERE t.a > 1;"
        );
    }

    /// A set operation puts its keyword at column zero and qualifies both arms.
    #[test]
    fn deparses_a_union() {
        let view = view("SELECT a FROM t UNION ALL SELECT a FROM u", &["a"]);
        assert!(
            view_definition_text(&view, true)
                == " SELECT t.a\n   FROM t\nUNION ALL\n SELECT u.a\n   FROM u;"
        );
    }

    /// An unparseable stored definition still answers a usable statement.
    #[test]
    fn an_unparseable_definition_falls_back_to_its_source_text() {
        let view = view("SELECT ???", &["a"]);
        assert!(view_definition_text(&view, false) == "SELECT ???;");
    }
}
