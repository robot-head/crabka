//! One walk over a query tree, for the questions a stored view raises about
//! what its body reads.
//!
//! A view is stored as SQL text and re-parsed on every scan, so the relations
//! it depends on are not recorded anywhere: they have to be read back out of
//! the parse tree. Two callers need them and neither may miss one.
//!
//! * `CREATE VIEW` decides where the view lands from what its body names — a
//!   body reading a temporary relation makes the view itself temporary,
//!   wherever in the tree that relation appears.
//! * `DROP TABLE` and `ALTER TABLE` refuse to leave a stored view pointing at
//!   nothing, which means every relation any part of the body reads.
//!
//! The walk therefore descends into everything: each arm of a set operation,
//! each side of a join, every derived table, every subquery in a select list,
//! `WHERE`, `HAVING`, `GROUP BY`, a window frame's offsets, and each `WITH`
//! entry's body.
//!
//! A `WITH` name is *not* a dependency — it names a query, not a relation — so
//! the walk carries the CTE names currently in scope and drops a `FROM` item
//! that one of them shadows. Scope follows `PostgreSQL`'s rule: a plain `WITH`
//! entry sees the entries written before it, a `WITH RECURSIVE` entry also sees
//! itself, and both are visible to the query they precede. A schema-qualified
//! name is never shadowed, because a CTE has no schema.

use crabka_pgparser::ast::{
    CteBody, Expr, FuncArgs, QueryBody, QueryExpr, RelationRef, SelectItem, SelectStmt, SetExpr,
    TableExpr, WindowRef, WindowSpec,
};

/// A relation named by a `FROM` item, with the qualifier it is referenced by.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Source<'a> {
    pub(crate) reference: &'a RelationRef,
    /// The `AS` alias, when the item carries one.
    pub(crate) alias: Option<&'a str>,
}

impl<'a> Source<'a> {
    /// The name columns of this item are qualified by — its alias when it has
    /// one, otherwise the relation's own name.
    pub(crate) fn qualifier(self) -> &'a str {
        self.alias.unwrap_or(&self.reference.name)
    }
}

/// What the walk reports. Every node of interest is offered exactly once, in
/// the order the tree is written.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Node<'a> {
    /// A stored relation read by a `FROM` item, after CTE names are removed.
    Relation(Source<'a>),
    /// One expression node, including every node reached through a subquery.
    Expr(&'a Expr),
    /// A `FROM` item that is not a plain relation — a derived table, a
    /// set-returning function, or `JSON_TABLE`. Its own contents are walked
    /// too; this node exists so a caller that can only reason about a flat
    /// list of relations knows to give up.
    ComputedFrom,
    /// A `WITH` entry holding an `INSERT`/`UPDATE`/`DELETE`/`MERGE`.
    DataModifyingCte,
}

/// Walk `query`, offering every [`Node`] to `visit`.
pub(crate) fn walk_query<'a>(query: &'a QueryExpr, visit: &mut impl FnMut(Node<'a>)) {
    walk_query_scoped(query, &mut Vec::new(), visit);
}

/// Every relation `query` reads, in tree order and without duplicates removed.
pub(crate) fn query_sources(query: &QueryExpr) -> Vec<Source<'_>> {
    let mut sources = Vec::new();
    walk_query(query, &mut |node| {
        if let Node::Relation(source) = node {
            sources.push(source);
        }
    });
    sources
}

fn walk_query_scoped<'a>(
    query: &'a QueryExpr,
    scope: &mut Vec<&'a str>,
    visit: &mut impl FnMut(Node<'a>),
) {
    let outer = scope.len();
    if let Some(with) = &query.with {
        for cte in &with.ctes {
            // `WITH RECURSIVE` puts an entry in scope for its own body; a plain
            // entry becomes visible only once it is written.
            if with.recursive {
                scope.push(cte.name.as_str());
            }
            match &cte.body {
                CteBody::Query(body) => walk_query_scoped(body, scope, visit),
                CteBody::Dml(_) => visit(Node::DataModifyingCte),
            }
            if !with.recursive {
                scope.push(cte.name.as_str());
            }
        }
    }
    walk_set_expr(&query.body, scope, visit);
    for item in &query.order_by {
        walk_expr(&item.expr, scope, visit);
    }
    for bound in query.limit.iter().chain(query.offset.iter()) {
        walk_expr(bound, scope, visit);
    }
    scope.truncate(outer);
}

fn walk_set_expr<'a>(
    body: &'a SetExpr,
    scope: &mut Vec<&'a str>,
    visit: &mut impl FnMut(Node<'a>),
) {
    match body {
        SetExpr::Query(QueryBody::Select(select)) => walk_select(select, scope, visit),
        SetExpr::Query(QueryBody::Values(values)) => {
            for cell in values.rows.iter().flatten() {
                walk_expr(cell, scope, visit);
            }
        }
        SetExpr::Query(QueryBody::Nested(nested)) => walk_query_scoped(nested, scope, visit),
        SetExpr::SetOp { left, right, .. } => {
            walk_set_expr(left, scope, visit);
            walk_set_expr(right, scope, visit);
        }
    }
}

fn walk_select<'a>(
    select: &'a SelectStmt,
    scope: &mut Vec<&'a str>,
    visit: &mut impl FnMut(Node<'a>),
) {
    for item in &select.from {
        walk_table_expr(item, scope, visit);
    }
    for item in &select.projection {
        match item {
            SelectItem::Expr { expr, .. } => walk_expr(expr, scope, visit),
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {}
        }
    }
    for expression in select
        .filter
        .iter()
        .chain(select.group_by.iter())
        .chain(select.having.iter())
        .chain(select.limit.iter())
        .chain(select.offset.iter())
        .chain(select.order_by.iter().map(|item| &item.expr))
    {
        walk_expr(expression, scope, visit);
    }
    for window in &select.windows {
        walk_window_spec(&window.spec, scope, visit);
    }
    // A window call sits outside the expression tree — the projection holds a
    // placeholder naming its index — so its arguments are reached from here.
    for call in &select.window_calls {
        if let FuncArgs::Exprs(args) = &call.args {
            for argument in args {
                walk_expr(argument, scope, visit);
            }
        }
        if let Some(filter) = &call.filter {
            walk_expr(filter, scope, visit);
        }
        if let WindowRef::Spec(spec) = &call.over {
            walk_window_spec(spec, scope, visit);
        }
    }
}

fn walk_window_spec<'a>(
    spec: &'a WindowSpec,
    scope: &mut Vec<&'a str>,
    visit: &mut impl FnMut(Node<'a>),
) {
    for expression in spec
        .partition_by
        .iter()
        .chain(spec.order_by.iter().map(|item| &item.expr))
    {
        walk_expr(expression, scope, visit);
    }
    if let Some(frame) = &spec.frame {
        for bound in [&frame.start, &frame.end] {
            if let Some(offset) = frame_offset(bound) {
                walk_expr(offset, scope, visit);
            }
        }
    }
}

fn frame_offset(bound: &crabka_pgparser::ast::FrameBound) -> Option<&Expr> {
    use crabka_pgparser::ast::FrameBound;
    match bound {
        FrameBound::Preceding(offset) | FrameBound::Following(offset) => Some(offset),
        FrameBound::UnboundedPreceding
        | FrameBound::CurrentRow
        | FrameBound::UnboundedFollowing => None,
    }
}

fn walk_table_expr<'a>(
    item: &'a TableExpr,
    scope: &mut Vec<&'a str>,
    visit: &mut impl FnMut(Node<'a>),
) {
    match item {
        TableExpr::Table {
            name,
            alias,
            sample,
            ..
        } => {
            // A CTE has no schema, so only a bare name can be shadowed by one.
            let shadowed = name.schema.is_none() && scope.iter().any(|cte| *cte == name.name);
            if !shadowed {
                visit(Node::Relation(Source {
                    reference: name,
                    alias: alias.as_deref(),
                }));
            }
            if let Some(sample) = sample {
                walk_expr(&sample.percent, scope, visit);
                if let Some(seed) = &sample.repeatable {
                    walk_expr(seed, scope, visit);
                }
            }
        }
        TableExpr::Derived { subquery, .. } => {
            visit(Node::ComputedFrom);
            walk_query_scoped(subquery, scope, visit);
        }
        TableExpr::Join {
            left,
            right,
            constraint,
            ..
        } => {
            walk_table_expr(left, scope, visit);
            walk_table_expr(right, scope, visit);
            if let crabka_pgparser::ast::JoinConstraint::On(predicate) = constraint {
                walk_expr(predicate, scope, visit);
            }
        }
        TableExpr::Function { functions, .. } => {
            visit(Node::ComputedFrom);
            for call in functions {
                for argument in &call.args {
                    walk_expr(argument, scope, visit);
                }
            }
        }
        TableExpr::JsonTable(table) => {
            visit(Node::ComputedFrom);
            for expression in table.exprs() {
                walk_expr(expression, scope, visit);
            }
        }
    }
}

fn walk_expr<'a>(expr: &'a Expr, scope: &mut Vec<&'a str>, visit: &mut impl FnMut(Node<'a>)) {
    visit(Node::Expr(expr));
    for child in crate::exec::expr_children(expr) {
        walk_expr(child, scope, visit);
    }
    for subquery in crate::exec::query_children(expr) {
        walk_query_scoped(subquery, scope, visit);
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgparser::ast::Statement;

    use super::{Node, query_sources, walk_query};

    /// Every relation the body of `definition` reads, as `schema.name` where a
    /// qualifier was written and the bare name otherwise.
    fn sources(definition: &str) -> Vec<String> {
        let statements = crabka_pgparser::parse(definition).expect("parse");
        let [Statement::Query(query)] = statements.as_slice() else {
            panic!("expected one query");
        };
        query_sources(query)
            .into_iter()
            .map(|source| match &source.reference.schema {
                Some(schema) => format!("{schema}.{}", source.reference.name),
                None => source.reference.name.clone(),
            })
            .collect()
    }

    fn nodes(definition: &str) -> (usize, usize) {
        let statements = crabka_pgparser::parse(definition).expect("parse");
        let [Statement::Query(query)] = statements.as_slice() else {
            panic!("expected one query");
        };
        let (mut computed, mut dml) = (0, 0);
        walk_query(query, &mut |node| match node {
            Node::ComputedFrom => computed += 1,
            Node::DataModifyingCte => dml += 1,
            _ => {}
        });
        (computed, dml)
    }

    /// Each case is a view body and every relation it depends on, in tree
    /// order. A missed entry is a view left pointing at a dropped relation, so
    /// the shapes that hide a relation deepest are the ones enumerated.
    #[test]
    fn collects_every_relation_a_body_reads() {
        let cases: [(&str, &[&str]); 22] = [
            ("SELECT a FROM t", &["t"]),
            ("SELECT a FROM s1.t", &["s1.t"]),
            ("SELECT 1", &[]),
            ("SELECT t.a FROM t JOIN u ON t.a = u.a", &["t", "u"]),
            (
                "SELECT t.a FROM t JOIN u ON t.a = u.a JOIN w ON w.a = t.a",
                &["t", "u", "w"],
            ),
            ("SELECT a FROM t, u", &["t", "u"]),
            ("SELECT a FROM t LEFT JOIN u USING (a)", &["t", "u"]),
            ("SELECT s.a FROM (SELECT a FROM t) s", &["t"]),
            (
                "SELECT s.a FROM (SELECT a FROM t UNION SELECT a FROM u) s",
                &["t", "u"],
            ),
            ("SELECT a, (SELECT max(e) FROM w) FROM t", &["t", "w"]),
            (
                "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u)",
                &["t", "u"],
            ),
            ("SELECT a FROM t WHERE a IN (SELECT a FROM u)", &["t", "u"]),
            (
                "SELECT a FROM t WHERE a = ANY (SELECT a FROM u)",
                &["t", "u"],
            ),
            ("SELECT ARRAY(SELECT a FROM u) FROM t", &["t", "u"]),
            (
                "SELECT b FROM t GROUP BY b HAVING count(*) > (SELECT count(*) FROM u)",
                &["t", "u"],
            ),
            ("SELECT a FROM t UNION SELECT a FROM u", &["t", "u"]),
            (
                "SELECT a FROM t UNION SELECT a FROM u EXCEPT SELECT a FROM w",
                &["t", "u", "w"],
            ),
            (
                "SELECT a FROM t ORDER BY (SELECT max(a) FROM u) LIMIT (SELECT 1 FROM w)",
                &["t", "u", "w"],
            ),
            (
                "SELECT t.a FROM t, LATERAL (SELECT d FROM u WHERE u.a = t.a) l",
                &["t", "u"],
            ),
            (
                "SELECT a, sum(c) OVER (PARTITION BY (SELECT 1 FROM u)) FROM t",
                &["t", "u"],
            ),
            (
                "SELECT a FROM t WHERE a IN (SELECT a FROM u WHERE a IN (SELECT a FROM w))",
                &["t", "u", "w"],
            ),
            ("SELECT * FROM unnest(ARRAY[1,2]) AS f(x)", &[]),
        ];
        for (definition, expected) in cases {
            assert!(sources(definition) == expected, "{definition}");
        }
    }

    /// A `WITH` name is a query, not a relation. The relations *inside* the
    /// entry are dependencies; the name itself never is, even when a real
    /// relation shares it.
    #[test]
    fn a_cte_name_is_not_a_dependency() {
        let cases: [(&str, &[&str]); 8] = [
            ("WITH s AS (SELECT a FROM t) SELECT a FROM s", &["t"]),
            // The CTE shadows the relation of the same name.
            ("WITH t AS (SELECT 1 AS a) SELECT a FROM t", &[]),
            // A qualified reference is never shadowed: a CTE has no schema.
            (
                "WITH t AS (SELECT 1 AS a) SELECT a FROM public.t",
                &["public.t"],
            ),
            // A later entry sees an earlier one; the earlier name is not read.
            (
                "WITH s AS (SELECT a FROM t), r AS (SELECT a FROM s) SELECT a FROM r",
                &["t"],
            ),
            // A plain entry does NOT see itself, so `t` here is the relation.
            ("WITH t AS (SELECT a FROM t) SELECT a FROM t", &["t"]),
            // A recursive entry does see itself, so the same body reads nothing.
            (
                "WITH RECURSIVE t AS (SELECT 1 AS a UNION ALL SELECT a FROM t) SELECT a FROM t",
                &[],
            ),
            // Scope closes with the query that opened it.
            (
                "SELECT a FROM t WHERE a IN (WITH u AS (SELECT 1 AS a) SELECT a FROM u)",
                &["t"],
            ),
            (
                "SELECT a FROM (WITH s AS (SELECT a FROM u) SELECT a FROM s) x, t",
                &["u", "t"],
            ),
        ];
        for (definition, expected) in cases {
            assert!(sources(definition) == expected, "{definition}");
        }
    }

    #[test]
    fn reports_computed_from_items_and_data_modifying_ctes() {
        let cases = [
            ("SELECT a FROM t", 0, 0),
            ("SELECT a FROM t JOIN u ON t.a = u.a", 0, 0),
            ("SELECT a FROM (SELECT a FROM t) s", 1, 0),
            ("SELECT * FROM generate_series(1, 3) g", 1, 0),
            (
                "WITH d AS (DELETE FROM t RETURNING a) SELECT a FROM d",
                0,
                1,
            ),
            (
                "SELECT a FROM t WHERE a IN (SELECT a FROM (SELECT a FROM u) s)",
                1,
                0,
            ),
        ];
        for (definition, computed, dml) in cases {
            assert!(nodes(definition) == (computed, dml), "{definition}");
        }
    }

    /// Every expression node is offered, including those only a subquery can
    /// reach — which is what makes the parameter check a whole-tree check.
    #[test]
    fn offers_expressions_inside_subqueries() {
        let cases = [
            "SELECT a FROM t WHERE a = $1",
            "SELECT (SELECT $1) FROM t",
            "SELECT a FROM t WHERE a IN (SELECT a FROM u WHERE a = $1)",
            "SELECT a FROM (SELECT a FROM u WHERE a = $1) s",
            "WITH s AS (SELECT $1 AS a) SELECT a FROM s",
            "SELECT a FROM t UNION SELECT $1",
            "SELECT a FROM t ORDER BY $1",
            "SELECT * FROM generate_series(1, $1) g",
        ];
        for definition in cases {
            let statements = crabka_pgparser::parse(definition).expect("parse");
            let [Statement::Query(query)] = statements.as_slice() else {
                panic!("expected one query");
            };
            let mut found = false;
            walk_query(query, &mut |node| {
                if let Node::Expr(crabka_pgparser::ast::Expr::Param(_)) = node {
                    found = true;
                }
            });
            assert!(found, "{definition}");
        }
    }
}
