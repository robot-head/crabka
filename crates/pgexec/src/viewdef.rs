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
//! Each clause keyword sits at its own offset from the indent the query was
//! entered at; continuation lines of the select list are indented four. Two
//! behaviors depend on the pretty flag: without it every operator expression is
//! fully parenthesized and each join node is wrapped in parentheses, with it
//! neither is. `pg_get_viewdef(oid)` is the un-pretty form;
//! `pg_get_viewdef(oid, true)` and the wrap-column overload are the pretty one.
//!
//! Three of PostgreSQL's rules are reproduced exactly because they are the ones
//! that decide whether the text round-trips:
//!
//! * **Column qualification.** A reference is written `tbl.col` whenever the
//!   query has other than exactly one range-table entry, or the query is nested
//!   inside another. This matches `get_query_def`'s `varprefix`.
//! * **Output naming.** A bare column reference whose name already equals the
//!   view's column name is written without `AS`; everything else carries one
//!   wherever the query's own column names are visible, and inside a sub-select
//!   — where they are not — only when the label is something other than
//!   `?column?`.
//! * **Nesting indent.** Writing `SELECT` deepens `indentLevel` by one
//!   eight-column step, so every query inside one — a `WITH` body, a derived
//!   table, a sub-select — is laid out one step further in than the query
//!   holding it, compounding with depth. `pg_get_expr` never enters a `SELECT`,
//!   which is why the sub-selects in a stored qual start at column zero.

use std::fmt::Write as _;

use crabka_pgparser::ast::{
    BinaryOp, DistinctClause, Expr, FrameBound, FrameExclusion, FrameMode, FuncArgs, FuncCall,
    GroupItem, JoinConstraint, JoinKind, OrderItem, QueryBody, QueryExpr, SelectItem, SelectStmt,
    SetExpr, SetOp, TableExpr, UnaryOp, ValuesStmt, WindowCall, WindowRef, WindowSpec,
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
    /// `ruleutils.c`'s `colNamesVisible`: whether this query's output column
    /// names matter to whoever reads it. They do for a view body, a `WITH`
    /// entry and a derived table, all of which are referenced by name; they do
    /// not inside a sub-select or on the right of a set operation, where an
    /// unnamed expression is left unlabelled rather than written out as
    /// `AS "?column?"`.
    colnames: bool,
    /// The `SELECT`'s window calls. Each one is held outside the expression
    /// tree, with a [`crabka_pgparser::ast::window_placeholder`] standing in
    /// for it, so rendering a select list needs them alongside.
    window_calls: &'a [crabka_pgparser::ast::WindowCall],
    /// `ruleutils.c`'s `indentLevel` at this point in the tree. A query's
    /// clause keywords sit at the level it was *entered* with; writing `SELECT`
    /// deepens it by one step, which is why every sub-query — a derived table,
    /// a sub-select, a `WITH` body — is laid out eight columns further in than
    /// the query holding it, and `pg_get_expr`, which never enters a `SELECT`,
    /// lays its sub-selects out at column zero.
    indent: usize,
}

/// `ruleutils.c`'s `PRETTYINDENT_STD`: one nesting step.
const INDENT_STEP: usize = 8;
/// `ruleutils.c`'s `PRETTYINDENT_LIMIT`: past this the per-level step is
/// divided down and wrapped, so a deeply nested tree cannot spend quadratic
/// space on leading blanks.
const INDENT_LIMIT: usize = 40;

impl Ctx<'_> {
    /// Wrap `inner` in the parentheses PostgreSQL adds only in un-pretty mode.
    fn paren(self, inner: String) -> String {
        if self.pretty {
            inner
        } else {
            format!("({inner})")
        }
    }

    /// The context a nested query is written in: one indent step deeper, and
    /// column references always qualified, because a sub-query is by
    /// definition not the outermost one.
    fn nested(self) -> Self {
        Self {
            qualify: true,
            qualifier: None,
            ..self
        }
    }
}

/// `appendContextKeyword`: a newline, then this clause's own indent.
///
/// `level` is the indent the enclosing query was entered at and `plus` the
/// keyword's `indentPlus` — 2 for `FROM`, 1 for `WHERE`/`GROUP BY`/`ORDER BY`,
/// 0 for `HAVING`/`LIMIT`/`OFFSET` and a set-operation keyword, 4 for a
/// continuation line of the select list.
fn clause_break(level: usize, plus: usize) -> String {
    let amount = if level < INDENT_LIMIT {
        level
    } else {
        (INDENT_LIMIT + (level - INDENT_LIMIT) / (INDENT_STEP / 2)) % INDENT_LIMIT
    };
    format!("\n{:width$}", "", width = amount + plus)
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
        colnames: true,
        window_calls: &[],
        indent: 0,
    };
    write_query_at(out, query, names, ctx);
}

/// One whole query at this context's indent — its `WITH` list, its body, and
/// the `ORDER BY`/`LIMIT`/`OFFSET` that belong to the query rather than to any
/// one set-operation arm.
fn write_query_at(out: &mut String, query: &QueryExpr, names: &[String], ctx: Ctx<'_>) {
    write_with_clause(out, query, ctx);
    write_set_expr(out, &query.body, names, ctx);
    // `ORDER BY`/`LIMIT`/`OFFSET` belong to the same `Query` node as the select
    // body, so they see the body's range table: a bare column there is
    // qualified by the sole FROM item exactly as one in the select list is.
    let tail = match &query.body {
        SetExpr::Query(QueryBody::Select(select)) => Ctx {
            qualify: ctx.qualify || range_table_len(&select.from) != 1,
            qualifier: sole_from_name(&select.from),
            ..ctx
        },
        _ => ctx,
    };
    write_query_tail(out, query, tail);
}

/// The `WITH` list, laid out as `ruleutils.c`'s `get_with_clause` does: each
/// entry's body one indent step in, on its own lines, with the closing paren on
/// a line of its own and the next entry continuing from it.
fn write_with_clause(out: &mut String, query: &QueryExpr, ctx: Ctx<'_>) {
    let Some(with) = &query.with else { return };
    if with.ctes.is_empty() {
        return;
    }
    let body = Ctx {
        indent: ctx.indent + INDENT_STEP,
        ..ctx.nested()
    };
    let mut separator = if with.recursive {
        " WITH RECURSIVE "
    } else {
        " WITH "
    };
    for cte in &with.ctes {
        out.push_str(separator);
        separator = ", ";
        out.push_str(&quote_identifier(&cte.name));
        if let Some(columns) = &cte.columns {
            let names = columns
                .iter()
                .map(|name| quote_identifier(name))
                .collect::<Vec<_>>()
                .join(", ");
            let _ = write!(out, "({names})");
        }
        out.push_str(" AS ");
        match cte.materialized {
            Some(true) => out.push_str("MATERIALIZED "),
            Some(false) => out.push_str("NOT MATERIALIZED "),
            None => {}
        }
        out.push('(');
        out.push_str(&clause_break(body.indent, 0));
        match &cte.body {
            crabka_pgparser::ast::CteBody::Query(inner) => {
                write_query_at(out, inner, &[], body);
            }
            // A data-modifying entry cannot reach a stored view — `CREATE VIEW`
            // refuses one — so its text is echoed rather than deparsed.
            crabka_pgparser::ast::CteBody::Dml(_) => out.push_str("..."),
        }
        out.push_str(&clause_break(body.indent, 0));
        out.push(')');
    }
    out.push_str(&clause_break(ctx.indent, 0));
}

/// Deparse one stored expression, the way `pg_get_expr` renders a qual held in
/// a catalog column.
///
/// PostgreSQL deparses such an expression with `PRETTYFLAG_INDENT` alone — the
/// same flags `pg_get_viewdef(oid)` uses — so it is the un-pretty form here:
/// every operator node fully parenthesized, any sub-select laid out on its own
/// indented lines. `varprefix` starts *false* because the context is built from
/// the single relation the expression belongs to, which is why a column
/// reference is bare at the top level and qualified only once a sub-select puts
/// it a level down.
pub(crate) fn expression_text(expr: &Expr) -> String {
    expr_text(
        expr,
        Ctx {
            pretty: false,
            qualify: false,
            qualifier: None,
            wrap: None,
            colnames: false,
            window_calls: &[],
            indent: 0,
        },
    )
}

fn write_query_tail(out: &mut String, query: &QueryExpr, ctx: Ctx<'_>) {
    // These clauses belong to the query, so they sit at the indent it was
    // entered with; the expressions in them are written one step deeper,
    // because `get_select_query_def` has already stepped in by this point.
    let inner = Ctx {
        indent: ctx.indent + INDENT_STEP,
        ..ctx
    };
    if !query.order_by.is_empty() {
        let _ = write!(
            out,
            "{} ORDER BY {}",
            clause_break(ctx.indent, 1),
            order_list(&query.order_by, inner)
        );
    }
    if let Some(limit) = &query.limit {
        let _ = write!(
            out,
            "{} LIMIT {}",
            clause_break(ctx.indent, 0),
            expr_text(limit, inner)
        );
    }
    if let Some(offset) = &query.offset {
        let _ = write!(
            out,
            "{} OFFSET {}",
            clause_break(ctx.indent, 0),
            expr_text(offset, inner)
        );
    }
}

fn write_set_expr(out: &mut String, body: &SetExpr, names: &[String], ctx: Ctx<'_>) {
    match body {
        SetExpr::Query(QueryBody::Select(select)) => write_select(out, select, names, ctx),
        SetExpr::Query(QueryBody::Values(values)) => write_values(out, values, ctx),
        SetExpr::Query(QueryBody::Nested(query)) => {
            write_query_at(out, query, names, ctx.nested());
        }
        SetExpr::SetOp {
            op,
            all,
            left,
            right,
        } => {
            // Every arm of a set operation is a sub-query, which is exactly the
            // condition PostgreSQL qualifies column references under.
            let arm = ctx.nested();
            write_set_expr(out, left, names, arm);
            let keyword = match op {
                SetOp::Union => "UNION",
                SetOp::Intersect => "INTERSECT",
                SetOp::Except => "EXCEPT",
            };
            let _ = write!(
                out,
                "{}{keyword}{}{}",
                clause_break(ctx.indent, 0),
                if *all { " ALL" } else { "" },
                clause_break(ctx.indent, 0)
            );
            // The right arm's own output names are never the ones anybody
            // reads, which is `get_setop_query` clearing `colNamesVisible`.
            write_set_expr(
                out,
                right,
                names,
                Ctx {
                    colnames: false,
                    ..arm
                },
            );
        }
    }
}

fn write_values(out: &mut String, values: &ValuesStmt, ctx: Ctx<'_>) {
    // PostgreSQL separates the columns of one row with a bare comma and the
    // rows themselves with a comma and a space.
    let rows = values
        .rows
        .iter()
        .map(|row| {
            let cells = row
                .iter()
                .map(|cell| expr_text(cell, ctx))
                .collect::<Vec<_>>()
                .join(",");
            format!("({cells})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let _ = write!(out, " VALUES {rows}");
}

fn write_select(out: &mut String, select: &SelectStmt, names: &[String], ctx: Ctx<'_>) {
    // The clause keywords sit at the indent this query was entered with;
    // everything they carry is written one step deeper, exactly as
    // `get_basic_select_query` deepens `indentLevel` before writing SELECT.
    let level = ctx.indent;
    let ctx = Ctx {
        qualify: ctx.qualify || range_table_len(&select.from) != 1,
        qualifier: sole_from_name(&select.from),
        window_calls: &select.window_calls,
        indent: level + INDENT_STEP,
        ..ctx
    };
    out.push_str(" SELECT");
    write_distinct(out, &select.distinct, ctx);
    write_target_list(out, &target_list(select, names, ctx), level, ctx);
    if !select.from.is_empty() {
        let from = select
            .from
            .iter()
            .map(|item| from_text(item, ctx))
            .collect::<Vec<_>>()
            .join(&format!(",{}", clause_break(level, 4)));
        let _ = write!(out, "{} FROM {from}", clause_break(level, 2));
    }
    if let Some(filter) = &select.filter {
        let _ = write!(
            out,
            "{} WHERE {}",
            clause_break(level, 1),
            expr_text(filter, ctx)
        );
    }
    if !select.group_by.is_empty() {
        let _ = write!(
            out,
            "{} GROUP BY {}",
            clause_break(level, 1),
            group_by_text(select, ctx)
        );
    }
    if let Some(having) = &select.having {
        let _ = write!(
            out,
            "{} HAVING {}",
            clause_break(level, 0),
            expr_text(having, ctx)
        );
    }
    if !select.windows.is_empty() {
        let list = select
            .windows
            .iter()
            .map(|window| {
                format!(
                    "{} AS ({})",
                    quote_identifier(&window.name),
                    window_spec_text(&window.spec, ctx)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let _ = write!(out, "{} WINDOW {list}", clause_break(level, 1));
    }
    if !select.order_by.is_empty() {
        let _ = write!(
            out,
            "{} ORDER BY {}",
            clause_break(level, 1),
            order_list(&select.order_by, ctx)
        );
    }
    if let Some(limit) = &select.limit {
        let _ = write!(
            out,
            "{} LIMIT {}",
            clause_break(level, 0),
            expr_text(limit, ctx)
        );
    }
    if let Some(offset) = &select.offset {
        let _ = write!(
            out,
            "{} OFFSET {}",
            clause_break(level, 0),
            expr_text(offset, ctx)
        );
    }
}

/// The `GROUP BY` list. A plain clause is its expressions; a grouping clause
/// prints the structure `PostgreSQL` reconstructs from the expanded sets —
/// `ROLLUP(a, b)`, `CUBE(a, b)`, `GROUPING SETS ((a, b), (a), ())` — over the
/// same expression list.
fn group_by_text(select: &SelectStmt, ctx: Ctx<'_>) -> String {
    let expression = |index: &usize| {
        select
            .group_by
            .get(*index)
            .map_or_else(String::new, |expr| expr_text(expr, ctx))
    };
    let Some(grouping) = &select.grouping else {
        return select
            .group_by
            .iter()
            .map(|expr| expr_text(expr, ctx))
            .collect::<Vec<_>>()
            .join(", ");
    };
    let items = grouping
        .items
        .iter()
        .map(|item| group_item_text(item, true, &expression))
        .collect::<Vec<_>>()
        .join(", ");
    if grouping.distinct {
        format!("DISTINCT {items}")
    } else {
        items
    }
}

/// One `GROUP BY` element.
///
/// `omit_parens` is `get_rule_groupingset`'s: a one-column set keeps its
/// parentheses only where they carry meaning. They are dropped at the top of
/// the clause and inside `ROLLUP`/`CUBE`, where a bare column is unambiguous,
/// and kept inside `GROUPING SETS`, where `(b)` is a set and `b` would not be.
fn group_item_text(
    item: &GroupItem,
    omit_parens: bool,
    expression: &impl Fn(&usize) -> String,
) -> String {
    let list = |items: &[GroupItem], omit: bool| {
        items
            .iter()
            .map(|item| group_item_text(item, omit, expression))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let simple = |columns: String, count: usize| {
        if omit_parens && count == 1 {
            columns
        } else {
            format!("({columns})")
        }
    };
    match item {
        GroupItem::Expr(index) => simple(expression(index), 1),
        GroupItem::Empty => "()".to_string(),
        GroupItem::Composite(indexes) => simple(
            indexes
                .iter()
                .map(expression)
                .collect::<Vec<_>>()
                .join(", "),
            indexes.len(),
        ),
        GroupItem::Rollup(items) => format!("ROLLUP({})", list(items, true)),
        GroupItem::Cube(items) => format!("CUBE({})", list(items, true)),
        GroupItem::GroupingSets(items) => format!("GROUPING SETS ({})", list(items, false)),
    }
}

/// One `f(…) OVER …` call, reconstructed from the placeholder that stands in
/// for it in the expression tree.
fn window_call_text(call: &WindowCall, ctx: Ctx<'_>) -> String {
    let args = match &call.args {
        FuncArgs::Star => "*".to_string(),
        FuncArgs::Exprs(args) => args
            .iter()
            .map(|argument| expr_text(argument, ctx))
            .collect::<Vec<_>>()
            .join(", "),
    };
    let filter = call.filter.as_ref().map_or_else(String::new, |predicate| {
        // `get_agg_expr` adds no parentheses of its own around the predicate;
        // the operator node it holds brings whatever it needs.
        format!(" FILTER (WHERE {})", expr_text(predicate, ctx))
    });
    let over = match &call.over {
        WindowRef::Named(name) => quote_identifier(name),
        WindowRef::Spec(spec) => format!("({})", window_spec_text(spec, ctx)),
    };
    format!(
        "{}({}{args}){filter} OVER {over}",
        call.name,
        if call.distinct { "DISTINCT " } else { "" }
    )
}

fn window_spec_text(spec: &WindowSpec, ctx: Ctx<'_>) -> String {
    let mut parts = Vec::new();
    if let Some(base) = &spec.base {
        parts.push(quote_identifier(base));
    }
    if !spec.partition_by.is_empty() {
        parts.push(format!(
            "PARTITION BY {}",
            spec.partition_by
                .iter()
                .map(|expr| expr_text(expr, ctx))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !spec.order_by.is_empty() {
        parts.push(format!("ORDER BY {}", order_list(&spec.order_by, ctx)));
    }
    if let Some(frame) = &spec.frame {
        let mode = match frame.mode {
            FrameMode::Rows => "ROWS",
            FrameMode::Range => "RANGE",
            FrameMode::Groups => "GROUPS",
        };
        let mut text = format!(
            "{mode} BETWEEN {} AND {}",
            frame_bound_text(&frame.start, ctx),
            frame_bound_text(&frame.end, ctx)
        );
        match frame.exclusion {
            FrameExclusion::NoOthers => {}
            FrameExclusion::CurrentRow => text.push_str(" EXCLUDE CURRENT ROW"),
            FrameExclusion::Group => text.push_str(" EXCLUDE GROUP"),
            FrameExclusion::Ties => text.push_str(" EXCLUDE TIES"),
        }
        parts.push(text);
    }
    parts.join(" ")
}

fn frame_bound_text(bound: &FrameBound, ctx: Ctx<'_>) -> String {
    match bound {
        FrameBound::UnboundedPreceding => "UNBOUNDED PRECEDING".to_string(),
        FrameBound::Preceding(offset) => format!("{} PRECEDING", expr_text(offset, ctx)),
        FrameBound::CurrentRow => "CURRENT ROW".to_string(),
        FrameBound::Following(offset) => format!("{} FOLLOWING", expr_text(offset, ctx)),
        FrameBound::UnboundedFollowing => "UNBOUNDED FOLLOWING".to_string(),
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
                // alias — so the cursor advances either way. It also *wins*
                // over the alias, the way `resultDesc` wins over `resname` in
                // `get_target_list`: a renamed view column, and the right arm
                // of a set operation, are both named by the view.
                let target = next.next().cloned().or_else(|| alias.clone());
                // A query with no column list of its own labels each item the
                // way the parser's `FigureColname` would.
                let target = target.unwrap_or_else(|| crate::exec::derived_name(expr));
                out.push(target_item(expr, &target, ctx));
            }
        }
    }
    if out.is_empty() {
        out.push(String::new());
    }
    out
}

/// Append the rendered select list. Without a wrap column each entry gets its
/// own line at PostgreSQL's four-space continuation indent; with one, entries
/// are packed greedily and a line breaks before the entry that would cross it.
fn write_target_list(out: &mut String, items: &[String], level: usize, ctx: Ctx<'_>) {
    let mut line_start = out.rfind('\n').map_or(0, |at| at + 1);
    let continuation = clause_break(level, 4);
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        let fits = ctx
            .wrap
            .is_some_and(|wrap| out.len() - line_start + 1 + item.len() <= wrap);
        if index > 0 && !fits {
            out.push_str(&continuation);
            line_start = out.len() - continuation.len() + 1;
        } else if index > 0 {
            out.push(' ');
        }
        out.push_str(item);
    }
}

/// One select-list entry.
///
/// `get_target_list` writes `AS` unless the label the reader would infer
/// already matches. For a column reference that inferred label is the column's
/// own name. For everything else it is nothing at all when the query's names
/// are visible — so a view or a `WITH` entry labels every expression, even one
/// whose label is `?column?` — and `?column?` when they are not, which is what
/// leaves an unnamed expression inside a sub-select unlabelled.
fn target_item(expr: &Expr, target: &str, ctx: Ctx<'_>) -> String {
    let text = expr_text(expr, ctx);
    if target.is_empty() {
        return text;
    }
    let inferred = match expr {
        Expr::Column { name, .. } => Some(name.as_str()),
        _ if ctx.colnames => None,
        _ => Some("?column?"),
    };
    if inferred == Some(target) {
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
            columns,
            lateral,
        } => {
            let mut inner = String::new();
            write_query_at(&mut inner, subquery, &[], ctx.nested());
            let columns = columns.as_ref().map_or_else(String::new, |columns| {
                let names = columns
                    .iter()
                    .map(|name| quote_identifier(name))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("({names})")
            });
            format!(
                "{}({inner}) {}{columns}",
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
        TableExpr::Function {
            functions,
            with_ordinality,
            alias,
            column_aliases,
            lateral,
            rows_from,
        } => {
            // A single call carries its column-definition list *inside the
            // alias parens* (`f(…) t(a integer)`); only `ROWS FROM` writes it as
            // `AS (…)` per call. The distinction is not cosmetic — neither
            // spelling parses in the other's position — and getting it wrong is
            // a stored rule that cannot be replayed.
            let single = (!*rows_from).then(|| functions.first()).flatten();
            let inline_defs = single.and_then(|call| call.column_defs.as_deref());
            let calls: Vec<String> = functions
                .iter()
                .map(|call| func_item_text(call, inline_defs.is_none(), ctx))
                .collect();
            let mut text = String::new();
            if *lateral {
                text.push_str("LATERAL ");
            }
            if inline_defs.is_none() && !*rows_from && calls.len() == 1 {
                text.push_str(&calls[0]);
            } else if *rows_from || calls.len() > 1 {
                text.push_str("ROWS FROM(");
                text.push_str(&calls.join(", "));
                text.push(')');
            } else {
                text.push_str(&calls[0]);
            }
            if *with_ordinality {
                text.push_str(" WITH ORDINALITY");
            }
            // PostgreSQL always names the item when it has to print a column
            // list, falling back to the function's own name — `f(…) (a integer)`
            // is not grammatical.
            let alias = alias.as_deref().or_else(|| {
                (inline_defs.is_some() || column_aliases.is_some())
                    .then(|| single.map(|call| call.name.as_str()))
                    .flatten()
            });
            if let Some(alias) = alias {
                text.push(' ');
                text.push_str(&quote_identifier(alias));
            }
            let listed = inline_defs.map_or_else(
                || {
                    column_aliases.as_ref().map(|columns| {
                        columns
                            .iter()
                            .map(|name| quote_identifier(name))
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                },
                |defs| Some(column_def_text(defs)),
            );
            if let Some(listed) = listed {
                text.push('(');
                text.push_str(&listed);
                text.push(')');
            }
            text
        }
        TableExpr::JsonTable(table) => json_table_text(table, ctx),
    }
}

/// One call of a FROM function item, with the column-definition list that gives
/// a record-returning call its row type.
///
/// Dropping that list would not merely lose detail: the rendered text would no
/// longer re-parse, because `json_to_record(…)` without one is a 42601. A stored
/// rule has to round-trip, so the list is part of the call, not part of the
/// alias.
fn func_item_text(
    call: &crabka_pgparser::ast::TableFuncCall,
    with_defs: bool,
    ctx: Ctx<'_>,
) -> String {
    let args = call
        .args
        .iter()
        .map(|arg| expr_text(arg, ctx))
        .collect::<Vec<_>>()
        .join(", ");
    let mut text = format!("{}({args})", call.name);
    if with_defs && let Some(defs) = &call.column_defs {
        text.push_str(" AS (");
        text.push_str(&column_def_text(defs));
        text.push(')');
    }
    text
}

/// `a integer, b text` — a column-definition list's body.
fn column_def_text(defs: &[crabka_pgparser::ast::TableFuncColumnDef]) -> String {
    defs.iter()
        .map(|def| format!("{} {}", quote_identifier(&def.name), def.ty.name()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A `JSON_TABLE(…)` FROM item.
///
/// `PostgreSQL` lays this out over many lines; crabka prints one, which
/// round-trips through the parser — the property a stored rule actually needs —
/// without claiming byte-identical rule text.
fn json_table_text(table: &crabka_pgparser::ast::JsonTable, ctx: Ctx<'_>) -> String {
    let mut out = String::from("JSON_TABLE(");
    let _ = write!(out, "{}, ", expr_text(&table.context, ctx));
    out.push_str(&quote_json_path(&table.path));
    if let Some(name) = &table.path_name {
        let _ = write!(out, " AS {}", quote_identifier(name));
    }
    if !table.passing.is_empty() {
        let passing = table
            .passing
            .iter()
            .map(|(name, value)| format!("{} AS {}", expr_text(value, ctx), quote_identifier(name)))
            .collect::<Vec<_>>()
            .join(", ");
        let _ = write!(out, " PASSING {passing}");
    }
    let _ = write!(
        out,
        " COLUMNS ({})",
        json_table_columns_text(&table.columns, ctx)
    );
    if table.error_on_error() {
        out.push_str(" ERROR ON ERROR");
    }
    out.push(')');
    if let Some(alias) = &table.alias {
        let _ = write!(out, " {}", quote_identifier(alias));
        if let Some(columns) = &table.column_aliases {
            let names = columns
                .iter()
                .map(|name| quote_identifier(name))
                .collect::<Vec<_>>()
                .join(", ");
            let _ = write!(out, "({names})");
        }
    }
    out
}

fn json_table_columns_text(
    columns: &[crabka_pgparser::ast::JsonTableColumn],
    ctx: Ctx<'_>,
) -> String {
    use crabka_pgparser::ast::JsonTableColumn;

    columns
        .iter()
        .map(|column| match column {
            JsonTableColumn::Ordinality { name } => {
                format!("{} FOR ORDINALITY", quote_identifier(name))
            }
            JsonTableColumn::Value(value) => json_table_value_column_text(value, ctx),
            JsonTableColumn::Exists(exists) => {
                let mut text = format!(
                    "{} {} EXISTS",
                    quote_identifier(&exists.name),
                    exists.ty.name()
                );
                if let Some(path) = &exists.path {
                    let _ = write!(text, " PATH {}", quote_json_path(path));
                }
                if let Some(behavior) = &exists.on_error {
                    let _ = write!(text, " {} ON ERROR", json_behavior_text(behavior, ctx));
                }
                text
            }
            JsonTableColumn::Nested(nested) => {
                let mut text = format!("NESTED PATH {}", quote_json_path(&nested.path));
                if let Some(name) = &nested.name {
                    let _ = write!(text, " AS {}", quote_identifier(name));
                }
                let _ = write!(
                    text,
                    " COLUMNS ({})",
                    json_table_columns_text(&nested.columns, ctx)
                );
                text
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn json_table_value_column_text(
    column: &crabka_pgparser::ast::JsonTableValueColumn,
    ctx: Ctx<'_>,
) -> String {
    use crabka_pgparser::ast::JsonWrapper;

    let mut text = format!("{} {}", quote_identifier(&column.name), column.ty.name());
    if column.format_json {
        text.push_str(" FORMAT JSON");
    }
    if let Some(path) = &column.path {
        let _ = write!(text, " PATH {}", quote_json_path(path));
    }
    match column.wrapper {
        None => {}
        Some(JsonWrapper::Without) => text.push_str(" WITHOUT WRAPPER"),
        Some(JsonWrapper::Conditional) => text.push_str(" WITH CONDITIONAL WRAPPER"),
        Some(JsonWrapper::Unconditional) => text.push_str(" WITH UNCONDITIONAL WRAPPER"),
    }
    match column.omit_quotes {
        None => {}
        Some(true) => text.push_str(" OMIT QUOTES"),
        Some(false) => text.push_str(" KEEP QUOTES"),
    }
    if let Some(behavior) = &column.on_empty {
        let _ = write!(text, " {} ON EMPTY", json_behavior_text(behavior, ctx));
    }
    if let Some(behavior) = &column.on_error {
        let _ = write!(text, " {} ON ERROR", json_behavior_text(behavior, ctx));
    }
    text
}

fn json_behavior_text(behavior: &crabka_pgparser::ast::JsonBehavior, ctx: Ctx<'_>) -> String {
    use crabka_pgparser::ast::JsonBehavior;

    match behavior {
        JsonBehavior::Error => "ERROR".into(),
        JsonBehavior::Null => "NULL".into(),
        JsonBehavior::True => "TRUE".into(),
        JsonBehavior::False => "FALSE".into(),
        JsonBehavior::Unknown => "UNKNOWN".into(),
        JsonBehavior::EmptyArray => "EMPTY ARRAY".into(),
        JsonBehavior::EmptyObject => "EMPTY OBJECT".into(),
        JsonBehavior::Default(expr) => format!("DEFAULT {}", expr_text(expr, ctx)),
    }
}

/// A jsonpath as the string literal it was written as.
fn quote_json_path(path: &str) -> String {
    format!("'{}'", path.replace('\'', "''"))
}

/// A join tree: the left side on the FROM line, each join on its own line at
/// PostgreSQL's five-space indent.
///
/// In un-pretty mode `get_from_clause_item` parenthesizes *every* join node, so
/// a three-way join reads `((a JOIN b ON …) JOIN c ON …)`. The caller wraps the
/// outermost node; this wraps each nested one.
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
    let left = {
        let text = join_text(left, ctx);
        if ctx.pretty || !matches!(**left, TableExpr::Join { .. }) {
            text
        } else {
            format!("({text})")
        }
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
        "{left}{} {}{keyword} {}{tail}",
        clause_break(ctx.indent.saturating_sub(INDENT_STEP), 4),
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
        // A window call is held beside the select list rather than in it, so the
        // placeholder standing in its place is written back as the call.
        Expr::Column { .. }
            if crabka_pgparser::ast::window_placeholder_index(expr)
                .is_some_and(|index| index < ctx.window_calls.len()) =>
        {
            let index = crabka_pgparser::ast::window_placeholder_index(expr)
                .expect("the guard already matched a placeholder");
            window_call_text(&ctx.window_calls[index], ctx)
        }
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
            operand_text(left, ctx),
            binary_op_text(*op),
            operand_text(right, ctx)
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
        // `ARRAY(…)` is one node in PostgreSQL, printed with the keyword glued
        // to the sub-select's own parenthesis.
        Expr::ArraySubquery(query) => format!("ARRAY{}", subquery_text(query, ctx)),
        // `get_sublink_expr` opens a parenthesis of its own before anything
        // else and closes it after the sub-select, whatever the pretty flag
        // says — so these three keep their parentheses in both forms.
        Expr::Exists(query) => format!("(EXISTS {})", subquery_text(query, ctx)),
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => format!(
            "({} {}IN {})",
            expr_text(expr, ctx),
            if *negated { "NOT " } else { "" },
            subquery_text(subquery, ctx)
        ),
        // `get_sublink_expr` spells `= ANY (subquery)` as `IN`, which is the
        // one quantified form that has a shorter equivalent; every other
        // operator keeps `ANY`/`ALL`.
        Expr::Quantified {
            expr,
            op,
            all,
            subquery,
        } => format!(
            "({} {} {})",
            expr_text(expr, ctx),
            if *all || *op != BinaryOp::Eq {
                format!(
                    "{} {}",
                    binary_op_text(*op),
                    if *all { "ALL" } else { "ANY" }
                )
            } else {
                "IN".to_string()
            },
            subquery_text(subquery, ctx)
        ),
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

/// One operand of a binary operator.
///
/// In pretty mode `get_rule_expr` asks `isSimpleNode` whether an argument needs
/// parentheses of its own, and a sub-select under an operator always does — so
/// `a > (SELECT …)` keeps a parenthesis the surrounding operator no longer
/// supplies. In un-pretty mode the operator node parenthesizes everything and
/// the question is never asked.
fn operand_text(expr: &Expr, ctx: Ctx<'_>) -> String {
    let text = expr_text(expr, ctx);
    if ctx.pretty && matches!(expr, Expr::ScalarSubquery(_) | Expr::ArraySubquery(_)) {
        format!("({text})")
    } else {
        text
    }
}

fn subquery_text(query: &QueryExpr, ctx: Ctx<'_>) -> String {
    let mut inner = String::new();
    // Nobody names the columns of a sub-select, so an expression that would
    // only be labelled `?column?` is left bare.
    let nested = Ctx {
        colnames: false,
        ..ctx.nested()
    };
    write_query_at(&mut inner, query, &[], nested);
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
        // The geometric prefix operators. `get_rule_expr` prints every prefix
        // operator the same way, so these parenthesize like `@` and `~` do.
        UnaryOp::NPoints => ctx.paren(format!("# {}", expr_text(expr, ctx))),
        UnaryOp::Length => ctx.paren(format!("@-@ {}", expr_text(expr, ctx))),
        UnaryOp::Center => ctx.paren(format!("@@ {}", expr_text(expr, ctx))),
        UnaryOp::IsHorizontal => ctx.paren(format!("?- {}", expr_text(expr, ctx))),
        UnaryOp::IsVertical => ctx.paren(format!("?| {}", expr_text(expr, ctx))),
        // Alone among the postfix predicates, `IS DOCUMENT` is an `XmlExpr` in
        // PostgreSQL rather than a `BooleanTest`, and `get_rule_expr` adds no
        // parentheses around it — `SELECT data IS DOCUMENT AS d`, where the
        // same view over `IS TRUE` would print `((…) IS TRUE)`.
        UnaryOp::IsDocument => format!("{} IS DOCUMENT", expr_text(expr, ctx)),
        UnaryOp::IsNotDocument => format!("{} IS NOT DOCUMENT", expr_text(expr, ctx)),
    }
}

/// The keyword spellings PostgreSQL's deparser gives the SQL value functions.
///
/// Each is parsed here as a zero-argument function call, but PostgreSQL holds
/// it as an `SQLValueFunction` node and `get_rule_expr` prints the keyword back
/// in upper case with no parentheses — `CURRENT_USER`, never `current_user()`.
/// The parenthesized spellings (`current_timestamp(0)`) carry arguments and are
/// ordinary calls in both engines, so matching on an empty argument list is
/// what separates the two.
const SQL_VALUE_FUNCTIONS: [(&str, &str); 7] = [
    ("current_date", "CURRENT_DATE"),
    ("current_time", "CURRENT_TIME"),
    ("current_timestamp", "CURRENT_TIMESTAMP"),
    ("current_user", "CURRENT_USER"),
    ("localtime", "LOCALTIME"),
    ("localtimestamp", "LOCALTIMESTAMP"),
    ("session_user", "SESSION_USER"),
];

fn func_text(call: &FuncCall, ctx: Ctx<'_>) -> String {
    if let FuncArgs::Exprs(args) = &call.args
        && let Some(text) = xml_construct_text(&call.name, args, ColumnType::Text, ctx)
    {
        return text;
    }
    if matches!(&call.args, FuncArgs::Exprs(args) if args.is_empty())
        && let Some((_, keyword)) = SQL_VALUE_FUNCTIONS
            .iter()
            .find(|(name, _)| *name == call.name)
    {
        return (*keyword).to_string();
    }
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

/// `XMLPARSE`, `XMLSERIALIZE` and `XMLCONCAT` reach the executor as ordinary
/// calls (the parser lowers their keyword grammar onto one), but `ruleutils.c`
/// holds them as `XmlExpr` nodes and prints the grammar back in upper case.
/// `xmlcomment` and `xmltext` are real `pg_proc` entries and print like any
/// other function, which is why they are absent here.
///
/// The mode words come out of the literals the parser planted, so a view over
/// `XMLSERIALIZE(DOCUMENT …)` reads back as one. `XMLPARSE` always prints
/// `STRIP WHITESPACE`: `PostgreSQL` stores the flag rather than the spelling,
/// and the grammar's default is to strip.
fn xml_construct_text(
    name: &str,
    args: &[Expr],
    serialize_type: ColumnType,
    ctx: Ctx<'_>,
) -> Option<String> {
    // The parser's coercion of an untyped literal is a *resolution*, not a
    // second cast: PostgreSQL holds one `Const` of type xml and prints
    // `'good'::xml`, never `('good'::text)::xml`.
    let text = |expr: &Expr| match expr {
        Expr::Cast { expr, ty } => match (&**expr, ty) {
            (Expr::StringLiteral(literal), ColumnType::Xml) => {
                format!("'{}'::xml", literal.replace('\'', "''"))
            }
            _ => expr_text(
                &Expr::Cast {
                    expr: expr.clone(),
                    ty: *ty,
                },
                ctx,
            ),
        },
        _ => expr_text(expr, ctx),
    };
    let mode = |expr: &Expr| match expr {
        Expr::StringLiteral(word) => Some(word.to_ascii_uppercase()),
        _ => None,
    };
    match (name, args) {
        ("xmlconcat", args) if !args.is_empty() => Some(format!(
            "XMLCONCAT({})",
            args.iter().map(text).collect::<Vec<_>>().join(", ")
        )),
        ("xmlparse", [option, value]) => Some(format!(
            "XMLPARSE({} {} STRIP WHITESPACE)",
            mode(option)?,
            text(value)
        )),
        ("xmlserialize", [option, value, indent]) => Some(format!(
            "XMLSERIALIZE({} {} AS {} {})",
            mode(option)?,
            text(value),
            crate::func::format_type(
                i64::from(serialize_type.oid()),
                i64::from(serialize_type.typmod())
            ),
            if matches!(indent, Expr::BoolLiteral(true)) {
                "INDENT"
            } else {
                "NO INDENT"
            }
        )),
        _ => None,
    }
}

/// A cast. PostgreSQL parenthesizes the operand in un-pretty mode (`(a)::text`)
/// and leaves it bare in pretty mode (`a::text`).
fn cast_text(expr: &Expr, ty: ColumnType, ctx: Ctx<'_>) -> String {
    // `XMLSERIALIZE` names its own target type, so the cast the parser wrapped
    // around it is redundant when that type is `text` -- and PostgreSQL prints
    // it only when it is not, which is why `xmlview9` has no `::text` and
    // `xmlview8` has a `::character(10)`.
    if let Expr::Func(call) = expr
        && let FuncArgs::Exprs(args) = &call.args
        && let Some(text) = xml_construct_text(&call.name, args, ty, ctx)
    {
        return if ty == ColumnType::Text {
            text
        } else {
            format!(
                "({text})::{}",
                crate::func::format_type(i64::from(ty.oid()), i64::from(ty.typmod()))
            )
        };
    }
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
            // `bit` is a reserved word, so `pg_get_expr` double-quotes it:
            // `'1001'::"bit"`, but plain `'1001'::bit varying`.
            let type_name = match ty {
                ColumnType::Bit(_) => "\"bit\"",
                other => other.name(),
            };
            format!("'{}'::{type_name}", rendered.replace('\'', "''"))
        }
    }
}

/// The SQL spelling a binary operator deparses to.
///
/// This is `eval::op_spelling`, which the 42883 messages already spell every
/// operator with. Sharing it is what keeps a view over `~=`, `<->` or any of
/// the geometric operators round-tripping: the table there is exhaustive over
/// `BinaryOp`, so a new operator cannot reach `pg_get_viewdef` unspelled.
fn binary_op_text(op: BinaryOp) -> &'static str {
    crate::eval::op_spelling(op)
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
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            columns: columns
                .iter()
                .map(|name| Column::new(*name, ColumnType::Int4))
                .collect(),
            options: crabka_pgcatalog::ViewOptions::default(),
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
            // A SQL value function is a keyword, not a call: PostgreSQL holds
            // it as its own node kind and prints the keyword back in upper
            // case, so `current_user()` is never what comes out.
            (
                "SELECT a FROM t WHERE b = current_user",
                &["a"][..],
                " SELECT a\n   FROM t\n  WHERE (b = CURRENT_USER);",
                " SELECT a\n   FROM t\n  WHERE b = CURRENT_USER;",
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

    /// Every shape a widened view body can take, and the exact text
    /// `PostgreSQL` 18.4 answers for `pg_get_viewdef(oid)` over it.
    ///
    /// What these pin is the indentation rule: a nested query — a `WITH` body,
    /// a derived table, a sub-select — is laid out one eight-column step
    /// further in than the query holding it, and the step compounds with depth.
    #[test]
    fn deparses_the_nested_shapes_at_postgres_indents() {
        let cases: [(&str, &[&str], &str); 12] = [
            (
                "WITH s AS (SELECT a FROM t WHERE a > 1) SELECT a FROM s",
                &["a"],
                " WITH s AS (\n         SELECT t.a\n           FROM t\n          WHERE (t.a > 1)\
                 \n        )\n SELECT a\n   FROM s;",
            ),
            (
                "WITH s AS (SELECT a FROM t), r AS (SELECT a FROM u) \
                 SELECT s.a FROM s JOIN r ON s.a = r.a",
                &["a"],
                " WITH s AS (\n         SELECT t.a\n           FROM t\n        ), r AS (\
                 \n         SELECT u.a\n           FROM u\n        )\n SELECT s.a\
                 \n   FROM (s\n     JOIN r ON ((s.a = r.a)));",
            ),
            (
                "WITH s AS MATERIALIZED (SELECT a FROM t) SELECT a FROM s",
                &["a"],
                " WITH s AS MATERIALIZED (\n         SELECT t.a\n           FROM t\n        )\
                 \n SELECT a\n   FROM s;",
            ),
            (
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT 2) SELECT i FROM n",
                &["i"],
                " WITH RECURSIVE n(i) AS (\n         SELECT 1 AS \"?column?\"\n        UNION ALL\
                 \n         SELECT 2\n        )\n SELECT i\n   FROM n;",
            ),
            (
                "SELECT a, b FROM (SELECT a, b FROM t WHERE a > 1) s",
                &["a", "b"],
                " SELECT a,\n    b\n   FROM ( SELECT t.a,\n            t.b\n           FROM t\
                 \n          WHERE (t.a > 1)) s;",
            ),
            (
                "SELECT q.a FROM (SELECT p.a FROM (SELECT a FROM t) p) q",
                &["a"],
                " SELECT a\n   FROM ( SELECT p.a\n           FROM ( SELECT t.a\
                 \n                   FROM t) p) q;",
            ),
            (
                "SELECT a, (SELECT max(e) FROM w) AS mx FROM t",
                &["a", "mx"],
                " SELECT a,\n    ( SELECT max(w.e) AS max\n           FROM w) AS mx\n   FROM t;",
            ),
            (
                "SELECT ARRAY(SELECT a FROM u ORDER BY a) AS arr",
                &["arr"],
                " SELECT ARRAY( SELECT u.a\n           FROM u\n          ORDER BY u.a) AS arr;",
            ),
            // `= ANY (subquery)` is the one quantified form PostgreSQL prints
            // back as `IN`.
            (
                "SELECT a FROM t WHERE a = ANY (SELECT a FROM w)",
                &["a"],
                " SELECT a\n   FROM t\n  WHERE (a IN ( SELECT w.a\n           FROM w));",
            ),
            (
                "SELECT a FROM t WHERE a > ALL (SELECT a FROM w)",
                &["a"],
                " SELECT a\n   FROM t\n  WHERE (a > ALL ( SELECT w.a\n           FROM w));",
            ),
            // Un-pretty mode parenthesizes every join node, so a three-way
            // join nests its parentheses.
            (
                "SELECT t.a FROM t JOIN u ON t.a = u.a JOIN w ON w.a = t.a",
                &["a"],
                " SELECT t.a\n   FROM ((t\n     JOIN u ON ((t.a = u.a)))\
                 \n     JOIN w ON ((w.a = t.a)));",
            ),
            (
                "SELECT t.a, u.d FROM t, u WHERE t.a = u.a",
                &["a", "d"],
                " SELECT t.a,\n    u.d\n   FROM t,\n    u\n  WHERE (t.a = u.a);",
            ),
        ];
        for (definition, columns, expected) in cases {
            let view = view(definition, columns);
            assert!(
                view_definition_text(&view, false) == expected,
                "{definition}"
            );
        }
    }

    /// A sub-select carries parentheses the pretty flag does not remove:
    /// `get_sublink_expr` opens one unconditionally, and under an operator
    /// `isSimpleNode` adds the one the operator itself no longer supplies.
    #[test]
    fn a_sublink_keeps_its_parentheses_in_both_forms() {
        let cases: [(&str, &[&str], &str, &str); 4] = [
            (
                "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u)",
                &["a"],
                " SELECT a\n   FROM t\n  WHERE (EXISTS ( SELECT 1\n           FROM u));",
                " SELECT a\n   FROM t\n  WHERE (EXISTS ( SELECT 1\n           FROM u));",
            ),
            (
                "SELECT a FROM t WHERE a IN (SELECT a FROM u)",
                &["a"],
                " SELECT a\n   FROM t\n  WHERE (a IN ( SELECT u.a\n           FROM u));",
                " SELECT a\n   FROM t\n  WHERE (a IN ( SELECT u.a\n           FROM u));",
            ),
            // Un-pretty, the operator node supplies the outer parenthesis;
            // pretty, it does not, and the operand grows one of its own.
            (
                "SELECT b, count(*) FROM t GROUP BY b HAVING count(*) > (SELECT 0)",
                &["b", "count"],
                " SELECT b,\n    count(*) AS count\n   FROM t\n  GROUP BY b\
                 \n HAVING (count(*) > ( SELECT 0));",
                " SELECT b,\n    count(*) AS count\n   FROM t\n  GROUP BY b\
                 \n HAVING count(*) > (( SELECT 0));",
            ),
            (
                "SELECT a FROM t WHERE a > ALL (SELECT a FROM u)",
                &["a"],
                " SELECT a\n   FROM t\n  WHERE (a > ALL ( SELECT u.a\n           FROM u));",
                " SELECT a\n   FROM t\n  WHERE (a > ALL ( SELECT u.a\n           FROM u));",
            ),
        ];
        for (definition, columns, plain, pretty) in cases {
            let view = view(definition, columns);
            assert!(view_definition_text(&view, false) == plain, "{definition}");
            assert!(view_definition_text(&view, true) == pretty, "{definition}");
        }
    }

    /// A window call lives beside the select list rather than in it, and a
    /// grouping clause keeps a structure the flat expression list cannot hold.
    /// Both are written back from where they are actually stored.
    #[test]
    fn deparses_window_calls_and_grouping_clauses() {
        let cases: [(&str, &[&str], &str); 7] = [
            (
                "SELECT a, sum(c) OVER (ORDER BY a) AS running FROM t",
                &["a", "running"],
                " SELECT a,\n    sum(c) OVER (ORDER BY a) AS running\n   FROM t;",
            ),
            (
                "SELECT a, sum(c) OVER w AS s FROM t WINDOW w AS (PARTITION BY b ORDER BY a)",
                &["a", "s"],
                " SELECT a,\n    sum(c) OVER w AS s\n   FROM t\
                 \n  WINDOW w AS (PARTITION BY b ORDER BY a);",
            ),
            (
                "SELECT a, row_number() OVER (PARTITION BY b ORDER BY a \
                 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS r FROM t",
                &["a", "r"],
                " SELECT a,\n    row_number() OVER (PARTITION BY b ORDER BY a \
                 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS r\n   FROM t;",
            ),
            (
                "SELECT a, count(*) FILTER (WHERE a > 1) OVER (ORDER BY a) AS n FROM t",
                &["a", "n"],
                " SELECT a,\n    count(*) FILTER (WHERE (a > 1)) OVER (ORDER BY a) AS n\
                 \n   FROM t;",
            ),
            (
                "SELECT b, count(*) FROM t GROUP BY ROLLUP (a, b)",
                &["b", "count"],
                " SELECT b,\n    count(*) AS count\n   FROM t\n  GROUP BY ROLLUP(a, b);",
            ),
            (
                "SELECT b, count(*) FROM t GROUP BY GROUPING SETS ((a,b),(a),())",
                &["b", "count"],
                " SELECT b,\n    count(*) AS count\n   FROM t\
                 \n  GROUP BY GROUPING SETS ((a, b), (a), ());",
            ),
            (
                "SELECT b, count(*) FROM t GROUP BY DISTINCT ROLLUP(a), b",
                &["b", "count"],
                " SELECT b,\n    count(*) AS count\n   FROM t\
                 \n  GROUP BY DISTINCT ROLLUP(a), b;",
            ),
        ];
        for (definition, columns, expected) in cases {
            let view = view(definition, columns);
            assert!(
                view_definition_text(&view, false) == expected,
                "{definition}"
            );
        }
    }

    /// The view's own column list names its output, which is why the right arm
    /// of a set operation is labelled by the left arm's name rather than its
    /// own alias. A query nobody names labels each item the way the parser
    /// would — and inside a sub-select, where the names are invisible, an
    /// expression that would only be `?column?` is left bare.
    #[test]
    fn labels_output_columns_the_way_postgres_does() {
        let cases: [(&str, &[&str], &str); 4] = [
            (
                "SELECT a AS ll FROM t UNION ALL SELECT a AS rr FROM u",
                &["ll"],
                " SELECT t.a AS ll\n   FROM t\nUNION ALL\n SELECT u.a AS ll\n   FROM u;",
            ),
            (
                "SELECT (SELECT 1) AS one FROM t",
                &["one"],
                " SELECT ( SELECT 1) AS one\n   FROM t;",
            ),
            (
                "SELECT s.n FROM (SELECT count(*) AS n FROM t) s",
                &["n"],
                " SELECT n\n   FROM ( SELECT count(*) AS n\n           FROM t) s;",
            ),
            (
                "WITH s AS (SELECT 1) SELECT * FROM s",
                &["c"],
                " WITH s AS (\n         SELECT 1 AS \"?column?\"\n        )\n SELECT c\
                 \n   FROM s;",
            ),
        ];
        for (definition, columns, expected) in cases {
            let view = view(definition, columns);
            assert!(
                view_definition_text(&view, false) == expected,
                "{definition}"
            );
        }
    }

    /// The expression entry point `pg_get_expr` reaches for a qual stored in a
    /// catalog column. Column references carry no prefix at the top level —
    /// the deparse context is the one relation the expression belongs to — and
    /// pick one up as soon as a sub-select puts them a level down.
    #[test]
    fn deparses_a_stored_expression_the_way_pg_get_expr_does() {
        let cases = [
            ("a>0", "(a > 0)"),
            ("t.a > 0", "(a > 0)"),
            ("a > 0 OR NOT b", "((a > 0) OR (NOT b))"),
            ("a IN (1, 2)", "(a = ANY (ARRAY[1, 2]))"),
            (
                "a <= (SELECT c FROM u WHERE d = session_user)",
                "(a <= ( SELECT u.c\n   FROM u\n  WHERE (u.d = SESSION_USER)))",
            ),
        ];
        for (source, expected) in cases {
            let expr = crabka_pgparser::parser::parse_expression(source).expect("parse");
            assert!(super::expression_text(&expr) == expected, "{source}");
        }
    }

    /// An unparseable stored definition still answers a usable statement.
    #[test]
    fn an_unparseable_definition_falls_back_to_its_source_text() {
        let view = view("SELECT ???", &["a"]);
        assert!(view_definition_text(&view, false) == "SELECT ???;");
    }
}
