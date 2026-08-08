//! Materialized common table expression scope for SELECT execution, plus the
//! `WITH RECURSIVE` fixpoint.
//!
//! The executor evaluates every `WITH` item once, in list order, against a scope
//! that holds the items before it. That is `PostgreSQL`'s rule, and it is why a
//! forward reference is `42P01` and not a silent empty relation. The executor
//! accepts `MATERIALIZED` / `NOT MATERIALIZED` and ignores it: it is purely an
//! inlining hint, and this executor always materializes.
//!
//! A `WITH RECURSIVE` item whose body references itself runs the standard
//! iterative fixpoint. The non-recursive term runs first and seeds a *working
//! table*. The recursive term then runs with the self-reference bound to that
//! working table, and repeats with each round's output until a round is empty.
//! `UNION` also drops rows already in the result, so it terminates whenever the
//! reachable set is finite. `UNION ALL` terminates only when the query does.

use std::collections::{HashMap, HashSet};

use crabka_pgparser::ast::{
    Cte, CteBody, Expr, JoinKind, QueryBody, QueryExpr, SelectItem, SelectStmt, SetExpr, SetOp,
    TableExpr, WithClause,
};
use crabka_pgtypes::Datum;

use crate::{error::ExecError, join::Relation, scope::Scope, subquery::SubCtx};

/// Iteration bound for a recursive CTE.
///
/// `PostgreSQL` imposes no such limit. An unterminated recursion there runs
/// until the backend exhausts memory, or until the statement is cancelled.
/// Crabka's blocking-query memory budget catches the usual runaway, where the
/// accumulated result grows without bound. This cap catches a recursion that
/// churns without growth, so a bad query fails fast and does not pin the session
/// forever.
const MAX_RECURSION_ITERATIONS: usize = 100_000;

#[derive(Debug, Clone, Default)]
pub(crate) struct CteContext {
    relations: HashMap<String, Relation>,
}

impl CteContext {
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    pub(crate) fn child(&self) -> Self {
        self.clone()
    }

    pub(crate) fn lookup(&self, name: &str) -> Option<&Relation> {
        self.relations.get(name)
    }

    pub(crate) fn insert(&mut self, name: String, rel: Relation) {
        self.relations.insert(name, rel);
    }
}

pub(crate) fn requalify_cte(rel: &Relation, alias: &str) -> Relation {
    let mut out = rel.clone();
    for col in &mut out.scope.columns {
        col.qualifier = Some(alias.to_string());
    }
    out
}

pub(crate) fn apply_cte_column_aliases(
    rel: Relation,
    name: &str,
    columns: &Option<Vec<String>>,
) -> Result<Relation, ExecError> {
    // Only an alias list LONGER than the query is an error, and a CTE's carries
    // its own 42P10 wording rather than the 42601 a derived table's does. A
    // SHORTER list is legal in PostgreSQL — the trailing columns simply keep the
    // names the query gave them.
    if let Some(names) = columns
        && names.len() > rel.scope.width()
    {
        return Err(ExecError::InvalidColumnReference(format!(
            "WITH query \"{name}\" has {} columns available but {} columns specified",
            rel.scope.width(),
            names.len()
        )));
    }
    crate::values::requalify_derived(rel, name, columns)
}

/// Evaluate one `WITH` item to its materialized relation. This chooses the
/// recursive fixpoint when the item is part of a `WITH RECURSIVE` list and
/// refers to itself.
pub(crate) fn evaluate_cte_relation(
    ctx: &SubCtx<'_>,
    cte: &Cte,
    recursive: bool,
    ctes: &CteContext,
) -> Result<Relation, ExecError> {
    let cte_ctx = ctx.with_ctes(ctes);
    let query = cte.body.as_query();
    if let Some(query) = query
        && query.locking.is_some()
    {
        return Err(ExecError::Unsupported(
            "FOR UPDATE/SHARE is not supported in CTEs".into(),
        ));
    }
    let self_referential = query.is_some_and(|q| recursive && query_references(q, &cte.name));
    if !self_referential {
        reject_search_and_cycle(cte)?;
        let query = query.ok_or_else(|| {
            ExecError::Unsupported("data-modifying statements in WITH are not supported".into())
        })?;
        let rel = crate::query::query_to_relation_with_ctes(&cte_ctx, query)?;
        return apply_cte_column_aliases(rel, &cte.name, &cte.columns);
    }
    let query = query.expect("a self-referential CTE has a query body");
    let rel = evaluate_recursive_cte(&cte_ctx, cte, query)?;
    apply_cte_column_aliases(rel, &cte.name, &cte.columns)
}

/// The statement inside a data-modifying `WITH` item.
fn dml_body(cte: &Cte) -> &crabka_pgparser::ast::Statement {
    match &cte.body {
        crabka_pgparser::ast::CteBody::Dml(statement) => statement,
        crabka_pgparser::ast::CteBody::Query(_) => {
            unreachable!("caller checked the item is data-modifying")
        }
    }
}

/// The order the `WITH` items are evaluated in.
///
/// A plain `WITH` list runs left to right, which is why a forward reference
/// there is `42P01`. `WITH RECURSIVE` instead sorts the list so that a
/// referenced item runs first, and rejects a genuine cycle between two items
/// with `0A000`. `PostgreSQL` does not implement mutual recursion.
fn evaluation_order(with: &WithClause) -> Result<Vec<usize>, ExecError> {
    let count = with.ctes.len();
    if !with.recursive {
        return Ok((0..count).collect());
    }
    let deps: Vec<Vec<usize>> = (0..count)
        .map(|i| {
            (0..count)
                .filter(|&j| j != i && cte_references(&with.ctes[i], &with.ctes[j].name))
                .collect()
        })
        .collect();
    let mut order = Vec::with_capacity(count);
    let mut done = vec![false; count];
    while order.len() < count {
        let ready = (0..count).find(|&i| !done[i] && deps[i].iter().all(|&j| done[j]));
        let Some(next) = ready else {
            return Err(ExecError::Unsupported(
                "mutual recursion between WITH items is not implemented".into(),
            ));
        };
        done[next] = true;
        order.push(next);
    }
    Ok(order)
}

fn cte_references(cte: &Cte, name: &str) -> bool {
    match &cte.body {
        CteBody::Query(query) => query_references(query, name),
        CteBody::Dml(_) => false,
    }
}

/// Is this a `WITH RECURSIVE` item that refers to itself?
///
/// Such an item's body has to be analyzed with the item's own name already in
/// scope, so the describe pass runs before the binder, not after it.
pub(crate) fn is_recursive_item(cte: &Cte, recursive: bool) -> bool {
    recursive && cte_references(cte, &cte.name)
}

/// `SEARCH` / `CYCLE` have a meaning only on a self-referential item.
/// `PostgreSQL` rejects them anywhere else with 42601.
fn reject_search_and_cycle(cte: &Cte) -> Result<(), ExecError> {
    if cte.search.is_some() || cte.cycle.is_some() {
        return Err(ExecError::Syntax("WITH query is not recursive".into()));
    }
    Ok(())
}

pub(crate) fn evaluate_with_clause(
    ctx: &SubCtx<'_>,
    with: Option<&WithClause>,
) -> Result<CteContext, ExecError> {
    let Some(with) = with else {
        return Ok(ctx.ctes.child());
    };
    let mut out = ctx.ctes.child();
    for index in evaluation_order(with)? {
        let cte = &with.ctes[index];
        let rel = evaluate_cte_relation(ctx, cte, with.recursive, &out)?;
        out.insert(cte.name.clone(), rel);
    }
    Ok(out)
}

pub(crate) fn describe_with_clause(
    catalog_kv: &dyn crabka_pgkv::Kv,
    resolution: &crate::relname::ResolutionScope,
    with: Option<&WithClause>,
    parent: &CteContext,
) -> Result<CteContext, ExecError> {
    let Some(with) = with else {
        return Ok(parent.child());
    };
    let mut out = parent.child();
    for index in evaluation_order(with)? {
        let cte = &with.ctes[index];
        let rel = describe_cte_relation(catalog_kv, resolution, cte, with.recursive, &out)?;
        out.insert(cte.name.clone(), rel);
    }
    Ok(out)
}

/// The row-less relation a `WITH` item produces, for `RowDescription`.
///
/// A recursive item is described from its non-recursive term alone, which is
/// also where `PostgreSQL` takes the CTE's column names and types from.
pub(crate) fn describe_cte_relation(
    catalog_kv: &dyn crabka_pgkv::Kv,
    resolution: &crate::relname::ResolutionScope,
    cte: &Cte,
    recursive: bool,
    ctes: &CteContext,
) -> Result<Relation, ExecError> {
    let Some(query) = cte.body.as_query() else {
        // A data-modifying item contributes the columns its RETURNING projects.
        let fields = crate::exec::describe_statement(catalog_kv, resolution, dml_body(cte))?;
        let columns = fields
            .iter()
            .map(|f| {
                Ok(crate::scope::ColumnBinding {
                    qualifier: None,
                    name: f.name.clone(),
                    ty: crate::exec::column_type_from_oid(f.type_oid)?,
                })
            })
            .collect::<Result<Vec<_>, ExecError>>()?;
        return apply_cte_column_aliases(
            Relation {
                scope: Scope { columns },
                rows: Vec::new(),
            },
            &cte.name,
            &cte.columns,
        );
    };
    let describe = |q: &QueryExpr| -> Result<Relation, ExecError> {
        let fields = crate::query::describe_query_expr_with_ctes(catalog_kv, resolution, q, ctes)?;
        let columns = fields
            .iter()
            .map(|f| {
                Ok(crate::scope::ColumnBinding {
                    qualifier: None,
                    name: f.name.clone(),
                    ty: crate::exec::column_type_from_oid(f.type_oid)?,
                })
            })
            .collect::<Result<Vec<_>, ExecError>>()?;
        Ok(Relation {
            scope: Scope { columns },
            rows: Vec::new(),
        })
    };
    let rel = if recursive && query_references(query, &cte.name) {
        let (non_recursive, _, _) = split_recursive_terms(query, &cte.name)?;
        let seed = QueryExpr {
            with: None,
            body: non_recursive.clone(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            with_ties: false,
            locking: None,
        };
        let base = describe(&seed)?;
        appended_columns(cte, &base)?
    } else {
        describe(query)?
    };
    apply_cte_column_aliases(rel, &cte.name, &cte.columns)
}

/// The non-recursive term, the recursive term, and whether duplicates survive.
fn split_recursive_terms<'a>(
    query: &'a QueryExpr,
    name: &str,
) -> Result<(&'a SetExpr, &'a SetExpr, bool), ExecError> {
    let malformed = || {
        ExecError::InvalidRecursion(format!(
            "recursive query \"{name}\" does not have the form non-recursive-term UNION [ALL] \
             recursive-term"
        ))
    };
    if query.with.is_some() {
        return Err(malformed());
    }
    let SetExpr::SetOp {
        op: SetOp::Union,
        all,
        left,
        right,
    } = &query.body
    else {
        return Err(malformed());
    };
    // PostgreSQL splits at the TOP-level UNION: everything to its left is the
    // non-recursive term, so `A UNION ALL B UNION ALL <recursive>` is legal.
    if set_expr_references(left, name) {
        return Err(malformed());
    }
    Ok((left, right, *all))
}

fn evaluate_recursive_cte(
    ctx: &SubCtx<'_>,
    cte: &Cte,
    query: &QueryExpr,
) -> Result<Relation, ExecError> {
    if !query.order_by.is_empty() || query.limit.is_some() || query.offset.is_some() {
        return Err(ExecError::Unsupported(
            "ORDER BY/LIMIT/OFFSET on a recursive query's outermost UNION is not supported".into(),
        ));
    }
    let (non_recursive, recursive, all) = split_recursive_terms(query, &cte.name)?;
    check_recursive_term(recursive, &cte.name)?;
    if cte.search.is_some() || cte.cycle.is_some() {
        return Err(ExecError::Unsupported(
            "SEARCH and CYCLE clauses are not supported".into(),
        ));
    }

    // The non-recursive term runs in a scope where the CTE name is NOT bound, so
    // a stray reference there is a plain 42P01 exactly as in PostgreSQL.
    // The CTE's column alias list names the columns the recursive term sees, so
    // it has to be applied to the seed before the first iteration.
    let base = apply_cte_column_aliases(
        crate::setops::set_expr_relation(ctx, non_recursive)?,
        &cte.name,
        &cte.columns,
    )?;
    let width = base.scope.width();
    let output_scope = base.scope.clone();
    check_recursive_term_types(ctx, cte, recursive, &output_scope)?;

    let mut result: Vec<Vec<Datum>> = Vec::new();
    let mut seen: HashSet<Vec<Datum>> = HashSet::new();
    let mut working: Vec<Vec<Datum>> = Vec::new();
    let mut bytes = 0usize;
    for row in base.rows {
        if !all && !seen.insert(row.clone()) {
            continue;
        }
        bytes = accumulate(bytes, &row)?;
        working.push(row.clone());
        result.push(row);
    }

    let mut iterations = 0usize;
    while !working.is_empty() {
        iterations += 1;
        if iterations > MAX_RECURSION_ITERATIONS {
            return Err(ExecError::StackDepthExceeded);
        }
        let mut scoped = ctx.ctes.child();
        scoped.insert(
            cte.name.clone(),
            Relation {
                scope: requalify_cte(
                    &Relation {
                        scope: output_scope.clone(),
                        rows: Vec::new(),
                    },
                    &cte.name,
                )
                .scope,
                rows: std::mem::take(&mut working),
            },
        );
        let step_ctx = ctx.with_ctes(&scoped);
        let produced = crate::setops::set_expr_relation(&step_ctx, recursive)?;
        if produced.scope.width() != width {
            return Err(ExecError::SetOpColumnCount {
                op: SetOp::Union,
                left: width,
                right: produced.scope.width(),
            });
        }
        for row in produced.rows {
            let row = coerce_row(row, &produced.scope, &output_scope, ctx)?;
            if !all && !seen.insert(row.clone()) {
                continue;
            }
            bytes = accumulate(bytes, &row)?;
            working.push(row.clone());
            result.push(row);
        }
    }

    Ok(Relation {
        scope: output_scope,
        rows: result,
    })
}

/// `PostgreSQL`'s `analyzeCTE` type check, run before the first round.
///
/// The non-recursive term fixes each column's type, and the `UNION`'s overall
/// type, which is the two terms' common type, must equal it. A recursive term
/// that widens a column (`integer` seeded, `numeric` produced) is `42804`, as is
/// one whose type will not unify with the seed's at all. `PostgreSQL` raises
/// both at parse analysis, before any row exists, which is why this runs up
/// front and does not surface as a per-row cast failure after the recursion has
/// churned.
///
/// An `unknown` recursive-term column (a bare `NULL` or string literal) adopts
/// the seed's type and does not clash with it, exactly as `select_common_type`
/// does. `SELECT 1 UNION ALL SELECT 'x' FROM t` is well-typed here and fails at
/// run time with the cast's own `22P02`, which is `PostgreSQL`'s answer too.
fn check_recursive_term_types(
    ctx: &SubCtx<'_>,
    cte: &Cte,
    recursive: &SetExpr,
    seed: &Scope,
) -> Result<(), ExecError> {
    let mut scoped = ctx.ctes.child();
    scoped.insert(
        cte.name.clone(),
        requalify_cte(
            &Relation {
                scope: seed.clone(),
                rows: Vec::new(),
            },
            &cte.name,
        ),
    );
    let produced =
        crate::setops::set_expr_columns(ctx.catalog_kv, ctx.fctx.resolution, recursive, &scoped)?;
    if produced.len() != seed.width() {
        return Err(ExecError::SetOpColumnCount {
            op: SetOp::Union,
            left: seed.width(),
            right: produced.len(),
        });
    }
    for (index, column) in produced.iter().enumerate() {
        if column.unknown {
            continue;
        }
        let seeded = seed.ty_at(index);
        let overall = crate::eval::unify_types(seeded, column.ty).map_err(|_| {
            ExecError::TypeMismatch(format!(
                "UNION types {} and {} cannot be matched",
                seeded.name(),
                column.ty.name()
            ))
        })?;
        if overall != seeded {
            return Err(ExecError::TypeMismatch(format!(
                "recursive query \"{}\" column {} has type {} in non-recursive term but type {} \
                 overall",
                cte.name,
                index + 1,
                seeded.name(),
                overall.name()
            )));
        }
    }
    Ok(())
}

fn accumulate(bytes: usize, row: &[Datum]) -> Result<usize, ExecError> {
    let bytes = bytes.saturating_add(crate::scanner::datum_row_bytes(row));
    if crate::scanner::exceeds_query_memory(bytes, crate::scanner::BLOCKING_QUERY_MEMORY) {
        return Err(crate::scanner::memory_budget_exceeded());
    }
    Ok(bytes)
}

/// Coerce a recursive-term row to the CTE's column types, which the
/// non-recursive term fixed. A NULL passes through untouched.
fn coerce_row(
    row: Vec<Datum>,
    from: &Scope,
    to: &Scope,
    ctx: &SubCtx<'_>,
) -> Result<Vec<Datum>, ExecError> {
    row.into_iter()
        .enumerate()
        .map(|(index, cell)| {
            if cell.is_null() || from.ty_at(index) == to.ty_at(index) {
                return Ok(cell);
            }
            crabka_pgtypes::cast::cast(&cell, to.ty_at(index), &ctx.eval_ctx.time_zone)
                .map_err(ExecError::from)
        })
        .collect()
}

/// The columns `SEARCH`/`CYCLE` append to a recursive CTE's output.
///
/// Execution refuses both clauses, so this only has to keep the describe path
/// consistent with that refusal, and it never adds a column.
fn appended_columns(cte: &Cte, base: &Relation) -> Result<Relation, ExecError> {
    if cte.search.is_some() || cte.cycle.is_some() {
        return Err(ExecError::Unsupported(
            "SEARCH and CYCLE clauses are not supported".into(),
        ));
    }
    Ok(base.clone())
}

/// `PostgreSQL`'s well-formedness rules for a recursive term (`parse_cte.c`):
/// exactly one self-reference, at the top level of the term's own FROM, never on
/// the nullable side of an outer join, and no aggregation or `DISTINCT`.
fn check_recursive_term(recursive: &SetExpr, name: &str) -> Result<(), ExecError> {
    let SetExpr::Query(QueryBody::Select(select)) = recursive else {
        // A nested set operation or VALUES cannot hold the single top-level
        // self-reference the fixpoint needs.
        return Err(ExecError::InvalidRecursion(format!(
            "recursive query \"{name}\" does not have the form non-recursive-term UNION [ALL] \
             recursive-term"
        )));
    };
    let mut refs = SelfRefs::default();
    scan_from(
        &select.from,
        name,
        false,
        select_has_aggregate(select),
        &mut refs,
    );
    scan_select_expressions(select, name, &mut refs);
    if refs.nested > 0 {
        return Err(ExecError::InvalidRecursion(format!(
            "recursive reference to query \"{name}\" must not appear within a subquery"
        )));
    }
    if refs.outer_join > 0 {
        return Err(ExecError::InvalidRecursion(format!(
            "recursive reference to query \"{name}\" must not appear within an outer join"
        )));
    }
    if refs.top_level > 1 {
        return Err(ExecError::InvalidRecursion(format!(
            "recursive reference to query \"{name}\" must not appear more than once"
        )));
    }
    if refs.top_level == 0 {
        return Err(ExecError::InvalidRecursion(format!(
            "recursive query \"{name}\" does not have the form non-recursive-term UNION [ALL] \
             recursive-term"
        )));
    }
    if refs.aggregate_host {
        return Err(ExecError::InvalidRecursion(
            "aggregate functions are not allowed in a recursive query's recursive term".into(),
        ));
    }
    Ok(())
}

/// Does this query level aggregate? Only an aggregate CALL counts. `PostgreSQL`
/// accepts `GROUP BY`, `SELECT DISTINCT` and window functions in a recursive
/// term. They only make the step idempotent, which is why such a query does not
/// terminate under `UNION ALL`.
fn select_has_aggregate(select: &SelectStmt) -> bool {
    select.projection.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => crate::agg::contains_aggregate(expr),
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    }) || select.having.is_some()
        || select
            .order_by
            .iter()
            .any(|o| crate::agg::contains_aggregate(&o.expr))
}

/// Where a recursive term's self-references were found.
#[derive(Debug, Default)]
struct SelfRefs {
    /// Directly in the term's own FROM join tree, the only legal position.
    top_level: usize,
    /// Inside a derived table or an expression subquery.
    nested: usize,
    /// On the nullable side of an outer join.
    outer_join: usize,
    /// Does the query level that directly holds a top-level self-reference
    /// aggregate? `PostgreSQL` scopes its aggregate ban to that level, not to
    /// the recursive term as a whole. So `SELECT max(n)+1 FROM (SELECT n FROM
    /// t) q` is legal, because the aggregate is one level above the
    /// self-reference, while `SELECT n+1 FROM (SELECT max(n) AS n FROM t) q` is
    /// not.
    aggregate_host: bool,
}

impl SelfRefs {
    fn total(&self) -> usize {
        self.top_level + self.nested + self.outer_join
    }
}

/// Scan the FROM items of one query level. `host_aggregated` is that level's own
/// [`select_has_aggregate`], carried down so a self-reference found here can
/// record whether the level that holds it aggregates.
fn scan_from(
    from: &[TableExpr],
    name: &str,
    nullable: bool,
    host_aggregated: bool,
    refs: &mut SelfRefs,
) {
    for item in from {
        scan_table_expr(item, name, nullable, host_aggregated, refs);
    }
}

fn scan_table_expr(
    item: &TableExpr,
    name: &str,
    nullable: bool,
    host_aggregated: bool,
    refs: &mut SelfRefs,
) {
    match item {
        // A recursive CTE's self-reference is always unqualified: `s.t` names a
        // stored relation, never the term being defined.
        TableExpr::Table {
            name: table_name, ..
        } if table_name.schema.is_none() && table_name.name == *name => {
            if nullable {
                refs.outer_join += 1;
            } else {
                refs.top_level += 1;
                refs.aggregate_host |= host_aggregated;
            }
        }
        TableExpr::Table { .. } => {}
        // A FROM-clause sub-SELECT is NOT a "subquery" for this rule: PostgreSQL
        // restricts SubLinks (scalar subqueries, EXISTS, IN), not derived tables,
        // so `FROM (SELECT n FROM t) q` is legal and its self-reference counts as
        // the term's one top-level reference — including on the nullable side of
        // an outer join, where it is still rejected.
        TableExpr::Derived { subquery, .. } => scan_query(subquery, name, nullable, refs),
        TableExpr::Function { functions, .. } => {
            for call in functions {
                for arg in &call.args {
                    scan_expr(arg, name, refs);
                }
            }
        }
        TableExpr::Join {
            left,
            right,
            kind,
            constraint,
        } => {
            // Only a side that an outer join can NULL-extend is forbidden; the
            // preserved side of a LEFT/RIGHT join is fine.
            let (left_nullable, right_nullable) = match kind {
                JoinKind::Left => (nullable, true),
                JoinKind::Right => (true, nullable),
                JoinKind::Full => (true, true),
                JoinKind::Inner | JoinKind::Cross => (nullable, nullable),
            };
            scan_table_expr(left, name, left_nullable, host_aggregated, refs);
            scan_table_expr(right, name, right_nullable, host_aggregated, refs);
            if let crabka_pgparser::ast::JoinConstraint::On(on) = constraint {
                scan_expr(on, name, refs);
            }
        }
    }
}

/// Scan a query expression reached through a FROM clause, and keep `nullable`
/// from the join position it sits in.
///
/// A nested `WITH` item of the same name shadows the outer one. Otherwise the
/// items' own bodies are part of what the FROM position can see, which is why
/// `FROM (WITH x AS (SELECT n FROM t) SELECT n FROM x) q` is legal.
fn scan_query(query: &QueryExpr, name: &str, nullable: bool, refs: &mut SelfRefs) {
    if let Some(with) = &query.with {
        if with.ctes.iter().any(|inner| inner.name == name) {
            return;
        }
        for inner in &with.ctes {
            if let CteBody::Query(q) = &inner.body {
                scan_query(q, name, nullable, refs);
            }
        }
    }
    scan_set_expr(&query.body, name, nullable, refs);
}

fn scan_set_expr(body: &SetExpr, name: &str, nullable: bool, refs: &mut SelfRefs) {
    match body {
        SetExpr::Query(QueryBody::Select(select)) => {
            scan_from(
                &select.from,
                name,
                nullable,
                select_has_aggregate(select),
                refs,
            );
            scan_select_expressions(select, name, refs);
        }
        SetExpr::Query(QueryBody::Values(_)) => {}
        SetExpr::Query(QueryBody::Nested(nested)) => scan_query(nested, name, nullable, refs),
        SetExpr::SetOp { left, right, .. } => {
            scan_set_expr(left, name, nullable, refs);
            scan_set_expr(right, name, nullable, refs);
        }
    }
}

fn scan_select_expressions(select: &SelectStmt, name: &str, refs: &mut SelfRefs) {
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            scan_expr(expr, name, refs);
        }
    }
    for expr in select
        .filter
        .iter()
        .chain(select.having.iter())
        .chain(select.group_by.iter())
    {
        scan_expr(expr, name, refs);
    }
    for item in &select.order_by {
        scan_expr(&item.expr, name, refs);
    }
}

/// A self-reference inside an expression is always inside a subquery.
fn scan_expr(expr: &Expr, name: &str, refs: &mut SelfRefs) {
    crate::grouping::visit_expr(expr, &mut |node| {
        let nested = match node {
            Expr::ScalarSubquery(q) | Expr::Exists(q) => query_references(q, name),
            Expr::InSubquery { subquery, .. } | Expr::Quantified { subquery, .. } => {
                query_references(subquery, name)
            }
            _ => false,
        };
        if nested {
            refs.nested += 1;
        }
    });
}

/// Does `query` name `name` as a relation anywhere?
///
/// A nested `WITH` item of the same name shadows the outer one, exactly as in
/// `PostgreSQL`, so such a query does not count as a reference.
pub(crate) fn query_references(query: &QueryExpr, name: &str) -> bool {
    let mut refs = SelfRefs::default();
    scan_query(query, name, false, &mut refs);
    refs.total() > 0
}

fn set_expr_references(body: &SetExpr, name: &str) -> bool {
    let mut refs = SelfRefs::default();
    scan_set_expr(body, name, false, &mut refs);
    refs.total() > 0
}

#[cfg(test)]
mod tests;
