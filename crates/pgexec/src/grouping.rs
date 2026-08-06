//! Q3: `GROUP BY` grouping sets, that is `ROLLUP`, `CUBE`, `GROUPING SETS` and
//! the empty grouping set `()`. This module also holds the `GROUPING()` bitmask
//! function and the SQL92 output references `GROUP BY 1` and
//! `GROUP BY <output alias>`.
//!
//! A grouping-set query is one query over several grouping keys at once. Every
//! expanded set produces its own groups, and a grouping column that is *not* in
//! the set reads as NULL in that set's output rows. `PostgreSQL` implements this
//! by a re-scan of the input once per set. This module does the same thing by
//! **augmenting the input relation**, rather than by running several
//! aggregations:
//!
//! - the scope grows one hidden column per distinct grouping expression, plus
//!   one for the grouping-set ordinal, named `$g0 … $gN` and `$gset`. `$` cannot
//!   begin an unquoted identifier, so no user column can collide with them.
//! - this module emits each input row once per grouping set, and the row carries
//!   that set's key values and its ordinal. The expressions the set leaves out
//!   read as NULL.
//! - every reference to a grouping expression in the clauses evaluated above the
//!   grouping is rewritten onto the hidden column, and every `GROUPING(…)` call
//!   is folded to a `CASE` over the ordinal. Those clauses are the select list,
//!   `HAVING`, `ORDER BY` and the `DISTINCT ON` keys.
//!
//! The rewritten statement is then a *plain* grouped aggregate over the
//! augmented relation, so [`crate::agg::aggregate_rows`] computes it. That is
//! what keeps `HAVING`, `DISTINCT`, `DISTINCT ON`, `ORDER BY`, `OFFSET`/`LIMIT`
//! and every aggregate behaving identically to the non-grouping-set path.
//!
//! Window functions run *above* the grouping. So [`crate::window`] lowers a
//! windowed query onto its grouped output by a call to [`aggregate_rows`], this
//! module's one rather than [`crate::agg`]'s, with a leaf select that still
//! carries the grouping-set clause. That is what makes `count(*) OVER ()` count
//! the grouping-set rows rather than the input rows.
//!
//! Two details of `PostgreSQL`'s semantics drive the shape:
//!
//! - The grouping-set ordinal is part of the key because two different sets may
//!   produce the same visible key. `GROUP BY ROLLUP(a)` over a table with a NULL
//!   `a` emits both an `a IS NULL` group and a grand-total group, and
//!   `PostgreSQL` keeps them separate. `GROUPING(a)` is what tells them apart.
//! - The NULL substitution stops at an aggregate's arguments. `sum(a)` in a
//!   grand-total row sums the real `a` values even though the row's own `a`
//!   reads NULL, so the rewrite never descends into an aggregate call.

use std::collections::HashSet;

use crabka_pgparser::ast::{
    ArraySubscript, Expr, FuncArgs, FuncCall, GroupItem, GroupingClause, SelectItem, SelectStmt,
};
use crabka_pgtypes::{ColumnType, Datum};

use crate::{
    clock::EvalCtx,
    error::ExecError,
    scope::{ColumnBinding, Scope},
};

/// Scope qualifier for this module's hidden columns. `$` cannot begin an
/// unquoted identifier, so no user column can collide with one, and `*` skips
/// them. See [`is_hidden_binding`].
const GROUPING_QUALIFIER: &str = "$g";

/// Hidden scope name for the value of grouping expression `index`.
fn key_column(index: usize) -> String {
    format!("$g{index}")
}

/// Hidden scope name for the grouping-set ordinal.
const SET_COLUMN: &str = "$gset";

/// Is this binding one of the hidden columns the grouping-set rewrite adds?
/// `SELECT *` must not expand to them.
pub(crate) fn is_hidden_binding(c: &ColumnBinding) -> bool {
    c.qualifier.as_deref() == Some(GROUPING_QUALIFIER)
}

/// One expanded grouping set: the indices into the flattened grouping-expression
/// list that this set groups by, sorted and deduplicated. `PostgreSQL` treats a
/// grouping set as a *set* of sort/group references, so `ROLLUP(a), ROLLUP(a)`
/// contributes the set `{a}`, not `{a, a}`.
type GroupingSet = Vec<usize>;

/// A grouping-set query resolved against its input scope. It holds the grouping
/// expressions, with SQL92 output references substituted and their column
/// references canonicalized, their types, and the expanded grouping sets.
struct GroupingPlan<'a> {
    scope: &'a Scope,
    group_by: Vec<Expr>,
    key_types: Vec<ColumnType>,
    sets: Vec<GroupingSet>,
}

impl GroupingPlan<'_> {
    /// The position of `e` in the grouping expressions, comparing by the column
    /// each reference resolves to rather than by how it was spelled.
    fn position_of(&self, e: &Expr) -> Option<usize> {
        let canonical = canonicalize_columns(e, self.scope);
        self.group_by.iter().position(|g| *g == canonical)
    }
}

/// Does this SELECT need the aggregate/grouping pipeline at all?
///
/// This function extends [`crate::agg::is_aggregate_query`] with the two shapes
/// it cannot see. The first is a grouping-set clause with no aggregate call,
/// such as `SELECT a FROM t GROUP BY ROLLUP(a)`. The second is a bare
/// `GROUPING()` reference, which `PostgreSQL` rejects with 42803 rather than
/// treating as an unknown function.
pub(crate) fn is_grouping_query(s: &SelectStmt) -> bool {
    s.grouping.is_some() || crate::agg::is_aggregate_query(s) || mentions_grouping_call(s)
}

/// Reject `GROUPING(…)` in a clause evaluated BELOW the grouping, where it has
/// no meaning. `PostgreSQL` answers `42803` and names the clause.
pub(crate) fn reject_misplaced_calls(s: &SelectStmt) -> Result<(), ExecError> {
    let reject = |expr: Option<&Expr>, clause: &str| -> Result<(), ExecError> {
        if expr.is_some_and(contains_grouping_call) {
            return Err(ExecError::Grouping(format!(
                "grouping operations are not allowed in {clause}"
            )));
        }
        Ok(())
    };
    for table in &s.from {
        reject_in_join_tree(table, &reject)?;
    }
    reject(s.filter.as_ref(), "WHERE")?;
    for group in &s.group_by {
        reject(Some(group), "GROUP BY")?;
    }
    Ok(())
}

/// `JOIN … ON` is evaluated below the grouping, exactly like `WHERE`.
fn reject_in_join_tree(
    table: &crabka_pgparser::ast::TableExpr,
    reject: &impl Fn(Option<&Expr>, &str) -> Result<(), ExecError>,
) -> Result<(), ExecError> {
    let crabka_pgparser::ast::TableExpr::Join {
        left,
        right,
        constraint,
        ..
    } = table
    else {
        return Ok(());
    };
    if let crabka_pgparser::ast::JoinConstraint::On(on) = constraint {
        reject(Some(on), "JOIN conditions")?;
    }
    reject_in_join_tree(left, reject)?;
    reject_in_join_tree(right, reject)
}

/// Does any evaluated clause of `s` contain a `GROUPING(…)` call?
fn mentions_grouping_call(s: &SelectStmt) -> bool {
    s.projection.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => contains_grouping_call(expr),
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    }) || s.having.as_ref().is_some_and(contains_grouping_call)
        || s.order_by.iter().any(|o| contains_grouping_call(&o.expr))
}

/// Run an aggregate/grouping query over the already-`WHERE`-filtered `rows`.
///
/// For a plain aggregate this function delegates straight to
/// [`crate::agg::aggregate_rows`]. Otherwise it resolves output references,
/// expands the grouping sets, and runs the augmented-relation rewrite the module
/// docs describe.
pub(crate) fn aggregate_rows(
    s: &SelectStmt,
    scope: &Scope,
    rows: Vec<Vec<Datum>>,
    ctx: &EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let group_by = resolve_group_references(s, scope)?;
    if s.grouping.is_none() && group_by == s.group_by && !mentions_grouping_call(s) {
        return crate::agg::aggregate_rows(s, scope, rows, ctx);
    }
    for g in &group_by {
        if crate::agg::contains_aggregate(g) {
            return Err(ExecError::Grouping(
                "aggregate functions are not allowed in GROUP BY".into(),
            ));
        }
    }

    let sets = match &s.grouping {
        // No set structure: the one grouping set is the whole GROUP BY list.
        None => vec![(0..group_by.len()).collect::<GroupingSet>()],
        Some(clause) => expand(clause),
    };
    let key_types = group_by
        .iter()
        .map(|g| crate::eval::infer_type(g, scope))
        .collect::<Result<Vec<_>, ExecError>>()?;
    for ty in &key_types {
        crate::eval::require_equality_operator(*ty)?;
    }
    let plan = GroupingPlan {
        scope,
        group_by: group_by
            .iter()
            .map(|g| canonicalize_columns(g, scope))
            .collect(),
        key_types,
        sets,
    };
    // `*` expands against the INPUT relation, so it has to become explicit items
    // before the rewrite substitutes hidden columns for the grouping expressions.
    let s = &expanded_projection(s, scope)?;

    if rows.is_empty() {
        return empty_input_rows(s, scope, &plan, ctx);
    }
    let augmented_scope = augmented_scope(scope, &plan.key_types);
    let augmented = augmented_rows(&plan, scope, &rows, ctx)?;
    let rewritten = rewrite_statement(s, &plan)?;
    crate::agg::aggregate_rows(&rewritten, &augmented_scope, augmented, ctx)
}

/// `s` with every `*` / `t.*` in its select list replaced by the explicit column
/// items it stands for, each carrying the output label `PostgreSQL` gives it.
fn expanded_projection(s: &SelectStmt, scope: &Scope) -> Result<SelectStmt, ExecError> {
    if !s.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_)
        )
    }) {
        return Ok(s.clone());
    }
    let (fields, out_exprs, _tys) = crate::exec::resolve_projection(&s.projection, scope)?;
    let mut stmt = s.clone();
    stmt.projection = fields
        .iter()
        .zip(out_exprs)
        .map(|(field, expr)| SelectItem::Expr {
            expr,
            alias: Some(field.name.clone()),
        })
        .collect();
    Ok(stmt)
}

/// Rewrite every column reference in `e` to the spelling of the scope binding it
/// resolves to, so that `t.a` and a bare `a` naming the same column compare
/// equal.
///
/// `PostgreSQL` matches a select-list entry against the `GROUP BY` list by the
/// *variable* each names, not by how it was written. That is why
/// `SELECT t.a … GROUP BY a` is grouped-valid there. This function leaves a
/// reference that does not resolve against this scope exactly as written.
pub(crate) fn canonicalize_columns(e: &Expr, scope: &Scope) -> Expr {
    rewrite(
        e,
        &mut |node: &Expr| {
            let Expr::Column { table, name } = node else {
                return Ok::<Option<Expr>, ExecError>(None);
            };
            let Ok(index) = scope.resolve(table.as_deref(), name) else {
                return Ok(None);
            };
            let binding = &scope.columns[index];
            Ok(Some(Expr::Column {
                table: binding.qualifier.clone(),
                name: binding.name.clone(),
            }))
        },
        true,
    )
    .expect("the column-canonicalizing fold cannot fail")
}

/// The projected rows for an empty input.
///
/// Only an *empty* grouping set produces a group when there are no input rows.
/// `PostgreSQL` still emits the grand total of a `ROLLUP`/`CUBE` over an empty
/// table, but no per-key rows. Such a set runs as an ordinary bare aggregate,
/// with `GROUP BY` removed, which is exactly
/// [`crate::agg::aggregate_rows`]'s zero-row path.
fn empty_input_rows(
    s: &SelectStmt,
    scope: &Scope,
    plan: &GroupingPlan,
    ctx: &EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let empty_sets = plan.sets.iter().filter(|set| set.is_empty()).count();
    if empty_sets == 0 {
        return Ok(Vec::new());
    }
    // Every grouping expression reads NULL and GROUPING() sees every column
    // aggregated, so one rewrite serves every empty set.
    let mut stmt = s.clone();
    stmt.grouping = None;
    stmt.group_by = Vec::new();
    let mut fold = |e: &Expr| fold_for_empty_set(e, plan);
    rewrite_clauses(&mut stmt, s, &mut fold)?;
    if empty_sets == 1 {
        return crate::agg::aggregate_rows(&stmt, scope, Vec::new(), ctx);
    }
    // Several empty grouping sets (`GROUPING SETS ((), ())`) each emit the same
    // row, so the tail has to run over the repeated output rather than per set.
    // Identical rows cannot be reordered, so only duplicate elimination and
    // OFFSET/LIMIT are still observable.
    let (fields, out_exprs, out_types) = crate::exec::resolve_projection(&stmt.projection, scope)?;
    let require_output = matches!(
        stmt.distinct,
        crabka_pgparser::ast::DistinctClause::Distinct
    );
    let order_keys = crate::exec::resolve_select_order_keys(
        &stmt.order_by,
        scope,
        &fields,
        &out_exprs,
        require_output,
    )?;
    if require_output {
        for ty in &out_types {
            crate::eval::require_equality_operator(*ty)?;
        }
    }
    if let Some(plan) =
        crate::exec::distinct_on_plan(&stmt, scope, &fields, &out_exprs, &order_keys)?
    {
        for expr in &plan.group {
            crate::eval::require_equality_operator(crate::eval::infer_type(expr, scope)?)?;
        }
    }
    for key in &order_keys {
        let ty = match key {
            crate::exec::SelectOrderKey::Output(index) => out_types[*index],
            crate::exec::SelectOrderKey::SourceExpr(expr) => crate::eval::infer_type(expr, scope)?,
        };
        crate::eval::require_ordering_operator(ty)?;
    }
    let (distinct, order_by) = (stmt.distinct.clone(), std::mem::take(&mut stmt.order_by));
    stmt.distinct = crabka_pgparser::ast::DistinctClause::All;
    let one = crate::agg::aggregate_rows(&stmt, scope, Vec::new(), ctx)?;
    stmt.order_by = order_by;
    let mut repeated: Vec<Vec<Datum>> = Vec::with_capacity(one.len() * empty_sets);
    for _ in 0..empty_sets {
        repeated.extend(one.iter().cloned());
    }
    if distinct.dedups() {
        let mut seen: HashSet<Vec<Datum>> = HashSet::new();
        repeated.retain(|row| seen.insert(row.clone()));
    }
    let window = crate::exec::RowWindow {
        offset: crate::exec::eval_row_count(
            stmt.offset.as_ref(),
            crate::exec::RowCountClause::Offset,
            ctx,
        )?,
        limit: crate::exec::eval_row_count(
            stmt.limit.as_ref(),
            crate::exec::RowCountClause::Limit,
            ctx,
        )?,
        with_ties: false,
    };
    Ok(crate::exec::apply_row_window(
        repeated.into_iter().map(|row| (Vec::new(), row)).collect(),
        window,
        &[],
    ))
}

/// The augmented scope: the input columns plus one hidden column per grouping
/// expression and one for the grouping-set ordinal.
fn augmented_scope(scope: &Scope, key_types: &[ColumnType]) -> Scope {
    let mut columns = scope.columns.clone();
    for (index, ty) in key_types.iter().enumerate() {
        columns.push(ColumnBinding {
            qualifier: Some(GROUPING_QUALIFIER.to_string()),
            name: key_column(index),
            ty: *ty,
        });
    }
    columns.push(ColumnBinding {
        qualifier: Some(GROUPING_QUALIFIER.to_string()),
        name: SET_COLUMN.to_string(),
        ty: ColumnType::Int4,
    });
    Scope { columns }
}

/// One augmented row per (input row, grouping set) pair.
fn augmented_rows(
    plan: &GroupingPlan,
    scope: &Scope,
    rows: &[Vec<Datum>],
    ctx: &EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut out = Vec::with_capacity(rows.len().saturating_mul(plan.sets.len()));
    let mut bytes = 0usize;
    let group_by = crate::bind::bind_all(&plan.group_by, scope);
    for row in rows {
        // The grouping expressions are evaluated once per input row; a set only
        // decides which of those values survives into its key.
        let keys = group_by
            .iter()
            .map(|g| crate::eval::eval(g.expr(), scope, row, ctx))
            .collect::<Result<Vec<_>, ExecError>>()?;
        for (ordinal, set) in plan.sets.iter().enumerate() {
            let mut augmented = row.clone();
            for (index, key) in keys.iter().enumerate() {
                augmented.push(if set.contains(&index) {
                    key.clone()
                } else {
                    Datum::Null
                });
            }
            augmented.push(Datum::Int4(set_ordinal(ordinal)?));
            bytes = bytes.saturating_add(crate::scanner::datum_row_bytes(&augmented));
            if crate::scanner::exceeds_query_memory(bytes, crate::scanner::BLOCKING_QUERY_MEMORY) {
                return Err(crate::scanner::memory_budget_exceeded());
            }
            out.push(augmented);
        }
    }
    Ok(out)
}

fn set_ordinal(ordinal: usize) -> Result<i32, ExecError> {
    i32::try_from(ordinal)
        .map_err(|_| ExecError::Unsupported("grouping set ordinal is outside int4 range".into()))
}

/// The rewritten plain-aggregate statement over the augmented relation.
fn rewrite_statement(s: &SelectStmt, plan: &GroupingPlan) -> Result<SelectStmt, ExecError> {
    let mut stmt = s.clone();
    stmt.grouping = None;
    stmt.group_by = (0..plan.group_by.len())
        .map(|index| Expr::Column {
            table: None,
            name: key_column(index),
        })
        .chain(std::iter::once(Expr::Column {
            table: None,
            name: SET_COLUMN.to_string(),
        }))
        .collect();
    let mut fold = |e: &Expr| fold_over_sets(e, plan);
    rewrite_clauses(&mut stmt, s, &mut fold)?;
    Ok(stmt)
}

/// Apply `fold` to every clause of `s` that is evaluated *above* the grouping,
/// and write the results into `stmt`. Those clauses are the select list,
/// `HAVING`, `ORDER BY`, and the `DISTINCT ON` keys.
///
/// `DISTINCT ON` belongs in that list because `PostgreSQL` evaluates its keys
/// over the grouped output. So a key that names a grouping expression has to
/// read the same hidden column the select list does.
fn rewrite_clauses(
    stmt: &mut SelectStmt,
    s: &SelectStmt,
    fold: &mut impl FnMut(&Expr) -> Result<Option<Expr>, ExecError>,
) -> Result<(), ExecError> {
    stmt.projection = rewrite_projection(&s.projection, fold)?;
    stmt.having = s
        .having
        .as_ref()
        .map(|h| rewrite(h, fold, false))
        .transpose()?;
    for (item, original) in stmt.order_by.iter_mut().zip(&s.order_by) {
        item.expr = rewrite(&original.expr, fold, false)?;
    }
    if let crabka_pgparser::ast::DistinctClause::On(keys) = &s.distinct {
        stmt.distinct = crabka_pgparser::ast::DistinctClause::On(rewrite_all(keys, fold, false)?);
    }
    Ok(())
}

/// Rewrite the projection. This function pins each item's output label first.
///
/// The rewrite replaces a grouping expression with a hidden column, and a
/// `GROUPING(…)` call with a `CASE`. Either change would otherwise change the
/// name `PostgreSQL` derives for an unaliased item. So every expression item
/// carries its original label explicitly afterwards.
fn rewrite_projection(
    projection: &[SelectItem],
    fold: &mut impl FnMut(&Expr) -> Result<Option<Expr>, ExecError>,
) -> Result<Vec<SelectItem>, ExecError> {
    projection
        .iter()
        .map(|item| match item {
            SelectItem::Expr { expr, alias } => Ok(SelectItem::Expr {
                expr: rewrite(expr, fold, false)?,
                alias: Some(alias.clone().unwrap_or_else(|| output_label(expr))),
            }),
            other => Ok(other.clone()),
        })
        .collect()
}

/// The label `PostgreSQL` gives an unaliased select item, taken before the
/// rewrite replaces the expression with a hidden column or a `CASE`.
fn output_label(expr: &Expr) -> String {
    crate::exec::derived_name(expr)
}

/// Replacement for one node in the multi-set rewrite. A grouping expression
/// becomes its hidden column, and a `GROUPING(…)` call becomes a `CASE` over the
/// grouping-set ordinal.
fn fold_over_sets(e: &Expr, plan: &GroupingPlan<'_>) -> Result<Option<Expr>, ExecError> {
    if let Some(index) = plan.position_of(e) {
        return Ok(Some(Expr::Column {
            table: None,
            name: key_column(index),
        }));
    }
    let Some(args) = grouping_args(e) else {
        return Ok(None);
    };
    let indices = grouping_argument_indices(args, plan)?;
    let whens = plan
        .sets
        .iter()
        .enumerate()
        .map(|(ordinal, set)| {
            Ok((
                int4(set_ordinal(ordinal)?),
                int4(grouping_mask(&indices, set)),
            ))
        })
        .collect::<Result<Vec<_>, ExecError>>()?;
    Ok(Some(Expr::Case {
        operand: Some(Box::new(Expr::Column {
            table: None,
            name: SET_COLUMN.to_string(),
        })),
        whens,
        else_result: None,
    }))
}

/// Replacement for one node when the only grouping set is the empty one: every
/// grouping expression reads as a typed NULL and `GROUPING(…)` folds to the
/// constant "all arguments aggregated" bitmask.
fn fold_for_empty_set(e: &Expr, plan: &GroupingPlan<'_>) -> Result<Option<Expr>, ExecError> {
    if let Some(index) = plan.position_of(e) {
        return Ok(Some(Expr::Const {
            value: Datum::Null,
            ty: plan.key_types[index],
        }));
    }
    let Some(args) = grouping_args(e) else {
        return Ok(None);
    };
    let indices = grouping_argument_indices(args, plan)?;
    Ok(Some(int4(grouping_mask(&indices, &[]))))
}

fn int4(value: i32) -> Expr {
    Expr::Const {
        value: Datum::Int4(value),
        ty: ColumnType::Int4,
    }
}

/// `GROUPING(a, b, …)`'s value for one grouping set. There is one bit per
/// argument, set when that argument is *not* in the set, most significant bit
/// first.
fn grouping_mask(indices: &[usize], set: &[usize]) -> i32 {
    indices.iter().fold(0i32, |mask, index| {
        (mask << 1) | i32::from(!set.contains(index))
    })
}

/// The grouping-expression positions a `GROUPING(…)` call names, in argument
/// order.
fn grouping_argument_indices(
    args: &[Expr],
    plan: &GroupingPlan<'_>,
) -> Result<Vec<usize>, ExecError> {
    if args.is_empty() {
        // PostgreSQL's grammar requires at least one argument, so an empty list is
        // a syntax error there rather than a grouping error.
        return Err(ExecError::Syntax("syntax error at or near \")\"".into()));
    }
    if args.len() > 31 {
        return Err(ExecError::Grouping(
            "GROUPING must have fewer than 32 arguments".into(),
        ));
    }
    args.iter()
        .map(|arg| {
            plan.position_of(arg).ok_or_else(|| {
                ExecError::Grouping(
                    "arguments to GROUPING must be grouping expressions of the associated query \
                     level"
                        .into(),
                )
            })
        })
        .collect()
}

/// Is this call `GROUPING(…)`?
///
/// It is not a function. The grouping-set rewrite folds it to a `CASE` over the
/// grouping-set ordinal, so nothing ever evaluates it as one. Its static result
/// type is `int4`, which is what lets a select list that carries one type-check
/// before the rewrite runs.
pub(crate) fn is_grouping_call(call: &FuncCall) -> bool {
    !call.distinct
        && call.name.eq_ignore_ascii_case("grouping")
        && matches!(call.args, FuncArgs::Exprs(_))
}

/// The argument list of a `GROUPING(…)` call, or `None` for any other node.
fn grouping_args(e: &Expr) -> Option<&[Expr]> {
    let Expr::Func(call) = e else {
        return None;
    };
    if !is_grouping_call(call) {
        return None;
    }
    match &call.args {
        FuncArgs::Exprs(args) => Some(args),
        FuncArgs::Star => None,
    }
}

fn contains_grouping_call(e: &Expr) -> bool {
    let mut found = false;
    visit_expr(e, &mut |node| found |= grouping_args(node).is_some());
    found
}

/// Visit every node of `e`, outermost first.
///
/// [`rewrite`] is this crate's one exhaustive [`Expr`] match, so read-only walks
/// run through it as an identity fold rather than duplicating that match. This
/// walk visits a subquery node but does not descend into it. Its inner query is
/// a separate scope, and belongs to whatever walks query expressions.
pub(crate) fn visit_expr(e: &Expr, visit: &mut impl FnMut(&Expr)) {
    let walked = rewrite(
        e,
        &mut |node: &Expr| {
            visit(node);
            Ok::<Option<Expr>, ExecError>(None)
        },
        true,
    );
    debug_assert!(walked.is_ok(), "the identity fold cannot fail");
}

/// Resolve `PostgreSQL`'s SQL92 output references in a `GROUP BY` list. A bare
/// unsigned integer is an output-column position. A bare name that does *not*
/// resolve against the input relation may name an output column's label.
///
/// The input relation wins. `SELECT b AS a FROM t GROUP BY a` groups by `t.a`,
/// not by the output label, exactly as `PostgreSQL` does.
pub(crate) fn resolve_group_references(
    s: &SelectStmt,
    scope: &Scope,
) -> Result<Vec<Expr>, ExecError> {
    let needs_output = s.group_by.iter().any(|g| match g {
        Expr::Column { table: None, name } => scope.resolve(None, name).is_err(),
        // Any bare constant needs the output list — either to index it, or to be
        // rejected as a non-integer constant.
        other => !matches!(
            crate::sql92::position(other, crate::sql92::Sql92Clause::GroupBy),
            Ok(None)
        ),
    });
    if !needs_output {
        return Ok(s.group_by.clone());
    }
    let (fields, out_exprs, _tys) = crate::exec::resolve_projection(&s.projection, scope)?;
    substitute_group_references(&s.group_by, scope, &fields, &out_exprs)
}

/// [`resolve_group_references`] against a select list that is ALREADY resolved.
///
/// A window query resolves its projection against the source scope widened with
/// its window results, which the source scope alone cannot do, so it substitutes
/// its `GROUP BY` output references through this entry point instead.
pub(crate) fn substitute_group_references(
    group_by: &[Expr],
    scope: &Scope,
    fields: &[crabka_pgwire::engine::FieldDescription],
    out_exprs: &[Expr],
) -> Result<Vec<Expr>, ExecError> {
    group_by
        .iter()
        .map(|g| {
            if let Some(index) = crate::sql92::output_position(
                g,
                out_exprs.len(),
                crate::sql92::Sql92Clause::GroupBy,
            )? {
                return Ok(out_exprs[index].clone());
            }
            match g {
                Expr::Column { table: None, name } if scope.resolve(None, name).is_err() => {
                    Ok(fields
                        .iter()
                        .position(|f| f.name == *name)
                        .map_or_else(|| g.clone(), |index| out_exprs[index].clone()))
                }
                other => Ok(other.clone()),
            }
        })
        .collect()
}

/// Expand a `GROUP BY` clause to its grouping sets. Items combine by cross
/// product, so `GROUP BY a, ROLLUP(b)` is `{a,b}` then `{a}`. `DISTINCT` then
/// removes duplicate sets.
fn expand(clause: &GroupingClause) -> Vec<GroupingSet> {
    let mut sets: Vec<GroupingSet> = vec![Vec::new()];
    for item in &clause.items {
        let item_sets = expand_item(item);
        let mut next = Vec::with_capacity(sets.len().saturating_mul(item_sets.len()));
        for base in &sets {
            for extra in &item_sets {
                let mut combined = base.clone();
                combined.extend(extra.iter().copied());
                next.push(normalize(combined));
            }
        }
        sets = next;
    }
    if clause.distinct {
        let mut seen: HashSet<GroupingSet> = HashSet::new();
        sets.retain(|set| seen.insert(set.clone()));
    }
    sets
}

fn expand_item(item: &GroupItem) -> Vec<GroupingSet> {
    match item {
        GroupItem::Expr(index) => vec![vec![*index]],
        GroupItem::Empty => vec![Vec::new()],
        GroupItem::Composite(indices) => vec![normalize(indices.clone())],
        // ROLLUP(a, b, c) is the four prefixes of its element list, longest first.
        GroupItem::Rollup(elements) => (0..=elements.len())
            .rev()
            .map(|prefix| flatten(&elements[..prefix]))
            .collect(),
        // CUBE(a, b) is every subset of its element list; counting the bitmask
        // down puts the full set first and the grand total last.
        GroupItem::Cube(elements) => (0..(1u32 << elements.len()))
            .rev()
            .map(|bits| {
                flatten_selected(elements, |index| {
                    bits & (1u32 << u32::try_from(index).unwrap_or(u32::BITS)) != 0
                })
            })
            .collect(),
        GroupItem::GroupingSets(items) => items.iter().flat_map(expand_item).collect(),
    }
}

/// The union of the leaves of a `ROLLUP` prefix.
fn flatten(elements: &[GroupItem]) -> GroupingSet {
    flatten_selected(elements, |_| true)
}

fn flatten_selected(elements: &[GroupItem], keep: impl Fn(usize) -> bool) -> GroupingSet {
    let mut set = Vec::new();
    for (index, element) in elements.iter().enumerate() {
        if keep(index) {
            set.extend(leaf_indices(element));
        }
    }
    normalize(set)
}

/// The grouping-expression indices an element contributes. A `ROLLUP`/`CUBE`
/// element is an expression or a parenthesised tuple, so the nested
/// set-producing forms cannot appear here. This function treats them as their
/// full union, which keeps it total.
fn leaf_indices(item: &GroupItem) -> Vec<usize> {
    match item {
        GroupItem::Expr(index) => vec![*index],
        GroupItem::Empty => Vec::new(),
        GroupItem::Composite(indices) => indices.clone(),
        GroupItem::Rollup(elements) | GroupItem::Cube(elements) => flatten(elements),
        GroupItem::GroupingSets(items) => flatten(items),
    }
}

fn normalize(mut set: GroupingSet) -> GroupingSet {
    set.sort_unstable();
    set.dedup();
    set
}

/// Bottom-up expression rewrite. This function offers `fold` every node, the
/// node itself first and then its rewritten children, and `fold` replaces a node
/// by returning `Some`.
///
/// This function leaves an aggregate call's arguments alone. `PostgreSQL`
/// substitutes grouped columns only outside aggregates, so `sum(a)` still sums
/// the real values in a row whose own `a` reads NULL.
///
/// The `match` is exhaustive on purpose. A new [`Expr`] variant must be given a
/// rule here, rather than silently escaping the grouping-column substitution.
///
/// This is the crate's one exhaustive [`Expr`] fold, so other passes that need to
/// map every node of an expression drive it rather than repeating the match.
pub(crate) fn rewrite(
    e: &Expr,
    fold: &mut impl FnMut(&Expr) -> Result<Option<Expr>, ExecError>,
    into_aggregates: bool,
) -> Result<Expr, ExecError> {
    // Offer the node as written first: a grouping expression is matched by its
    // source form (`a + b`), not by its rewritten children.
    if let Some(replacement) = fold(e)? {
        return Ok(replacement);
    }
    let rebuilt = match e {
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Column { .. }
        | Expr::Param(_)
        | Expr::Default
        | Expr::Const { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_) => e.clone(),
        Expr::FieldSelect { base, field } => Expr::FieldSelect {
            base: Box::new(rewrite(base, fold, into_aggregates)?),
            field: field.clone(),
        },
        Expr::FieldSelectAll(base) => {
            Expr::FieldSelectAll(Box::new(rewrite(base, fold, into_aggregates)?))
        }
        Expr::Unary { op, expr } => Expr::Unary {
            op: *op,
            expr: Box::new(rewrite(expr, fold, into_aggregates)?),
        },
        Expr::Collate { expr, collation } => Expr::Collate {
            expr: Box::new(rewrite(expr, fold, into_aggregates)?),
            collation: collation.clone(),
        },
        Expr::Binary { op, left, right } => Expr::Binary {
            op: *op,
            left: Box::new(rewrite(left, fold, into_aggregates)?),
            right: Box::new(rewrite(right, fold, into_aggregates)?),
        },
        Expr::Func(call) if !into_aggregates && is_aggregate_call(call) => Expr::Func(call.clone()),
        Expr::Func(FuncCall {
            name,
            distinct,
            args,
            filter,
        }) => Expr::Func(FuncCall {
            name: name.clone(),
            distinct: *distinct,
            args: match args {
                FuncArgs::Star => FuncArgs::Star,
                FuncArgs::Exprs(args) => FuncArgs::Exprs(rewrite_all(args, fold, into_aggregates)?),
            },
            // A FILTER predicate is an ordinary expression over the same rows, so
            // it is rewritten exactly like the arguments.
            filter: match filter {
                Some(predicate) => Some(Box::new(rewrite(predicate, fold, into_aggregates)?)),
                None => None,
            },
        }),
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(rewrite(expr, fold, into_aggregates)?),
            negated: *negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(rewrite(expr, fold, into_aggregates)?),
            list: rewrite_all(list, fold, into_aggregates)?,
            negated: *negated,
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(rewrite(expr, fold, into_aggregates)?),
            low: Box::new(rewrite(low, fold, into_aggregates)?),
            high: Box::new(rewrite(high, fold, into_aggregates)?),
            negated: *negated,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
            kind,
            escape,
        } => Expr::Like {
            expr: Box::new(rewrite(expr, fold, into_aggregates)?),
            pattern: Box::new(rewrite(pattern, fold, into_aggregates)?),
            negated: *negated,
            kind: *kind,
            escape: escape
                .as_ref()
                .map(|escape| rewrite(escape, fold, into_aggregates).map(Box::new))
                .transpose()?,
        },
        Expr::Case {
            operand,
            whens,
            else_result,
        } => Expr::Case {
            operand: operand
                .as_ref()
                .map(|operand| rewrite(operand, fold, into_aggregates).map(Box::new))
                .transpose()?,
            whens: whens
                .iter()
                .map(|(when, then)| {
                    Ok((
                        rewrite(when, fold, into_aggregates)?,
                        rewrite(then, fold, into_aggregates)?,
                    ))
                })
                .collect::<Result<Vec<_>, ExecError>>()?,
            else_result: else_result
                .as_ref()
                .map(|result| rewrite(result, fold, into_aggregates).map(Box::new))
                .transpose()?,
        },
        Expr::Cast { expr, ty } => Expr::Cast {
            expr: Box::new(rewrite(expr, fold, into_aggregates)?),
            ty: *ty,
        },
        Expr::SqlJson(json) => Expr::SqlJson(Box::new(
            json.map_children(|child| rewrite(child, fold, into_aggregates))?,
        )),
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Expr::InSubquery {
            expr: Box::new(rewrite(expr, fold, into_aggregates)?),
            subquery: subquery.clone(),
            negated: *negated,
        },
        Expr::Quantified {
            expr,
            op,
            all,
            subquery,
        } => Expr::Quantified {
            expr: Box::new(rewrite(expr, fold, into_aggregates)?),
            op: *op,
            all: *all,
            subquery: subquery.clone(),
        },
        Expr::QuantifiedArray {
            expr,
            op,
            all,
            array,
        } => Expr::QuantifiedArray {
            expr: Box::new(rewrite(expr, fold, into_aggregates)?),
            op: *op,
            all: *all,
            array: Box::new(rewrite(array, fold, into_aggregates)?),
        },
        Expr::ArrayLiteral(elements) => {
            Expr::ArrayLiteral(rewrite_all(elements, fold, into_aggregates)?)
        }
        Expr::Row(elements) => Expr::Row(rewrite_all(elements, fold, into_aggregates)?),
        Expr::Subscript { base, index } => Expr::Subscript {
            base: Box::new(rewrite(base, fold, into_aggregates)?),
            index: Box::new(rewrite(index, fold, into_aggregates)?),
        },
        Expr::ArrayRef { base, subscripts } => Expr::ArrayRef {
            base: Box::new(rewrite(base, fold, into_aggregates)?),
            subscripts: subscripts
                .iter()
                .map(|subscript| rewrite_subscript(subscript, fold, into_aggregates))
                .collect::<Result<Vec<_>, ExecError>>()?,
        },
        // Its inner query is a separate scope, like every other subquery node.
        Expr::ArraySubquery(query) => Expr::ArraySubquery(query.clone()),
    };
    Ok(rebuilt)
}

fn rewrite_subscript(
    subscript: &ArraySubscript,
    fold: &mut impl FnMut(&Expr) -> Result<Option<Expr>, ExecError>,
    into_aggregates: bool,
) -> Result<ArraySubscript, ExecError> {
    Ok(match subscript {
        ArraySubscript::Index(index) => {
            ArraySubscript::Index(rewrite(index, fold, into_aggregates)?)
        }
        ArraySubscript::Slice { lower, upper } => {
            let mut bound = |e: &Option<Expr>| -> Result<Option<Expr>, ExecError> {
                e.as_ref()
                    .map(|e| rewrite(e, fold, into_aggregates))
                    .transpose()
            };
            ArraySubscript::Slice {
                lower: bound(lower)?,
                upper: bound(upper)?,
            }
        }
    })
}

/// Is this call an aggregate whose arguments the grouped-column substitution
/// must leave alone? A wrapping scalar function over an aggregate is not. Its
/// own arguments still need the rewrite.
fn is_aggregate_call(call: &FuncCall) -> bool {
    let whole = Expr::Func(call.clone());
    if !crate::agg::contains_aggregate(&whole) {
        return false;
    }
    match &call.args {
        FuncArgs::Star => true,
        FuncArgs::Exprs(args) => !args.iter().any(crate::agg::contains_aggregate),
    }
}

fn rewrite_all(
    exprs: &[Expr],
    fold: &mut impl FnMut(&Expr) -> Result<Option<Expr>, ExecError>,
    into_aggregates: bool,
) -> Result<Vec<Expr>, ExecError> {
    exprs
        .iter()
        .map(|e| rewrite(e, fold, into_aggregates))
        .collect()
}

#[cfg(test)]
mod tests;
