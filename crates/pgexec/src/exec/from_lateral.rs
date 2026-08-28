//! LATERAL FROM execution and memoization.

use super::*;

pub(crate) fn is_lateral_item(te: &crabka_pgparser::ast::TableExpr, outer: &Scope) -> bool {
    use crabka_pgparser::ast::TableExpr;
    match te {
        TableExpr::Derived { lateral, .. } => *lateral,
        TableExpr::Function {
            lateral, functions, ..
        } => {
            *lateral
                || functions
                    .iter()
                    .flat_map(|call| call.arguments())
                    .any(|arg| expr_references_scope(arg, outer))
        }
        // `JSON_TABLE` is implicitly lateral in PostgreSQL exactly as a function
        // item is, so a context or PASSING expression that reads an earlier item
        // makes it correlated whether or not the keyword was written.
        TableExpr::JsonTable(table) => crate::jsontable::references_scope(table, outer),
        TableExpr::XmlTable(table) => crate::xmltable::references_scope(table, outer),
        TableExpr::Table { .. } | TableExpr::Join { .. } => false,
    }
}

/// Does `expr` name a value that resolves in `scope`?
///
/// A whole-row reference is a value too: it makes a FROM function implicitly
/// lateral even when its relation has no column with the same name.
pub(crate) fn expr_references_scope(expr: &Expr, scope: &Scope) -> bool {
    if let Expr::Column { table, name } = expr {
        return scope.resolve(table.as_deref(), name).is_ok()
            || (table.is_none() && scope.whole_row(name).is_some())
            || (name == "*"
                && table
                    .as_deref()
                    .is_some_and(|qualifier| scope.whole_row(qualifier).is_some()));
    }
    expr_children(expr)
        .into_iter()
        .any(|child| expr_references_scope(child, scope))
}

/// Re-evaluate `te` once per accumulated row and concatenate the results.
///
/// Each iteration joins a one-row left relation against the specialized right
/// side, so `ON`/`USING`/`NATURAL` matching and LEFT-join NULL padding are the
/// ordinary join code. So `LEFT JOIN LATERAL` keeps an outer row whose lateral
/// side produced nothing, exactly as `PostgreSQL` does.
///
/// `RIGHT`/`FULL JOIN LATERAL` is only an error when the lateral item *does*
/// reference the other side: `PostgreSQL` accepts the keyword itself and runs
/// the join, because an item that reads nothing from the left needs no left row
/// to be evaluated against.
pub(crate) fn lateral_join(
    read_ctx: &crate::subquery::SubCtx<'_>,
    acc: Relation,
    te: &crabka_pgparser::ast::TableExpr,
    kind: crabka_pgparser::ast::JoinKind,
    constraint: &crabka_pgparser::ast::JoinConstraint,
) -> Result<Relation, ExecError> {
    use crabka_pgparser::ast::JoinKind;
    let ctx = read_ctx.eval_ctx;
    let mut binder =
        LateralBinder::new(read_ctx.catalog_kv, read_ctx.fctx.resolution, read_ctx.ctes);
    if matches!(kind, JoinKind::Right | JoinKind::Full) {
        let nulls = vec![Datum::Null; acc.scope.width()];
        let (specialized, referenced) = binder.bind(te, &acc.scope, &nulls);
        if let Some(relation) = referenced {
            // A lateral reference on the nullable side would have to be evaluated
            // for rows that do not exist yet, so PostgreSQL rejects the reference
            // rather than the join.
            return Err(ExecError::InvalidFromEntry {
                table: relation,
                note: crate::error::FromEntryNote::CombiningJoinType,
            });
        }
        // Nothing was correlated, so the item is an ordinary relation.
        let right = build_table_expr(read_ctx, &specialized, None, None, None)?;
        return join_relations(acc, right, kind, constraint, ctx, read_ctx.join_policy());
    }
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    let mut scope: Option<Scope> = None;
    let mut bytes = 0usize;
    struct CachedRight {
        specialized: crabka_pgparser::ast::TableExpr,
        relation: Relation,
        index: PreparedJoinIndex,
    }
    let mut cache: Vec<CachedRight> = Vec::new();
    let mut cache_bytes = 0usize;
    struct CachedRowsFromCall {
        rows: TableFunctionRows,
    }
    let rows_from_correlated = match te {
        crabka_pgparser::ast::TableExpr::Function {
            functions,
            rows_from: true,
            ..
        } => Some(
            functions
                .iter()
                .map(|call| {
                    call.arguments()
                        .any(|arg| expr_references_scope(arg, &acc.scope))
                })
                .collect::<Vec<_>>(),
        ),
        _ => None,
    };
    let mut rows_from_cache = rows_from_correlated.as_ref().map(|calls| {
        calls
            .iter()
            .map(|_| None)
            .collect::<Vec<Option<CachedRowsFromCall>>>()
    });
    for outer_row in &acc.rows {
        let (specialized, _) = binder.bind(te, &acc.scope, outer_row);
        let one = Relation {
            scope: acc.scope.clone(),
            rows: vec![outer_row.clone()],
        };
        let joined = if let Some(cached) = cache
            .iter()
            .find(|cached| cached.specialized == specialized)
        {
            join_relations_prepared(
                one,
                &cached.relation,
                kind,
                constraint,
                ctx,
                read_ctx.join_policy(),
                &cached.index,
            )?
        } else {
            let right = match (
                rows_from_correlated.as_ref(),
                rows_from_cache.as_mut(),
                &specialized,
            ) {
                (
                    Some(correlated),
                    Some(cache),
                    crabka_pgparser::ast::TableExpr::Function {
                        functions,
                        with_ordinality,
                        rows_from: true,
                        alias,
                        column_aliases,
                        ..
                    },
                ) => {
                    let mut calls = Vec::with_capacity(functions.len());
                    for (index, call) in functions.iter().enumerate() {
                        if correlated[index] {
                            calls.push(table_function_call_rows(read_ctx, call)?);
                            continue;
                        }
                        if let Some(cached) = &cache[index] {
                            calls.push(cached.rows.clone());
                            continue;
                        }
                        let rows = table_function_call_rows(read_ctx, call)?;
                        let bytes = rows
                            .1
                            .iter()
                            .map(|row| crate::scanner::datum_row_bytes(row))
                            .sum();
                        read_ctx.statement_memory.reserve().replace_with(bytes)?;
                        cache[index] = Some(CachedRowsFromCall { rows: rows.clone() });
                        calls.push(rows);
                    }
                    crate::srf::rows_from_function_relation(
                        &functions[0].name,
                        calls,
                        *with_ordinality,
                        alias.as_deref(),
                        column_aliases,
                    )?
                }
                _ => build_table_expr(read_ctx, &specialized, None, None, None)?,
            };
            let right_bytes = right
                .rows
                .iter()
                .map(|row| crate::scanner::datum_row_bytes(row))
                .sum::<usize>();
            // ponytail: cap memoization; a planner-level lateral flattening is
            // the upgrade if workloads need more than 64 stable variants.
            let retained_without_index = cache_bytes.saturating_add(right_bytes);
            let mut index = if lateral_cacheable(te)
                && cache.len() < 64
                && !crate::scanner::exceeds_query_memory(
                    retained_without_index,
                    read_ctx.blocking_query_memory,
                ) {
                let remaining = read_ctx
                    .blocking_query_memory
                    .bytes_u64()
                    .saturating_sub(u64::try_from(retained_without_index).unwrap_or(u64::MAX));
                Some(prepare_join_index(
                    &acc,
                    &right,
                    constraint,
                    ctx,
                    crabka_units::ByteSize::from_bytes(remaining),
                )?)
            } else {
                None
            };
            let retained_bytes = right_bytes
                .saturating_add(index.as_ref().map_or(0, PreparedJoinIndex::estimated_bytes));
            // Memoizing the inner relation deliberately does NOT require the
            // index over the outer one. Re-running `build_table_expr` is the
            // expensive half — for a lateral over `tenk1` it is a full scan per
            // outer row — while an index-less entry still skips that and probes
            // by scanning a relation that is usually a row or two. Requiring
            // the index meant that under the 20 MiB policy the certification
            // runs with, nothing was ever cached and `memoize` took 41% of the
            // suite's wall clock.
            let can_cache = lateral_cacheable(te)
                && cache.len() < 64
                && !crate::scanner::exceeds_query_memory(
                    cache_bytes.saturating_add(retained_bytes),
                    read_ctx.blocking_query_memory,
                );
            if can_cache {
                let index = index.take().unwrap_or_else(PreparedJoinIndex::none);
                cache.push(CachedRight {
                    specialized,
                    relation: right,
                    index,
                });
                cache_bytes = cache_bytes.saturating_add(retained_bytes);
                let cached = cache.last().expect("the cache entry was just pushed");
                join_relations_prepared(
                    one,
                    &cached.relation,
                    kind,
                    constraint,
                    ctx,
                    read_ctx.join_policy(),
                    &cached.index,
                )?
            } else if let Some(index) = index {
                let mut index = index;
                index.discard_index();
                join_relations_prepared(
                    one,
                    &right,
                    kind,
                    constraint,
                    ctx,
                    read_ctx.join_policy(),
                    &index,
                )?
            } else {
                join_relations(one, right, kind, constraint, ctx, read_ctx.join_policy())?
            }
        };
        for row in &joined.rows {
            bytes = bytes.saturating_add(crate::scanner::datum_row_bytes(row));
        }
        if crate::scanner::exceeds_query_memory(bytes, read_ctx.blocking_query_memory) {
            return Err(crate::scanner::memory_budget_exceeded());
        }
        rows.extend(joined.rows);
        scope = Some(joined.scope);
    }
    // With no outer rows there is nothing to correlate against, but the output
    // still needs the lateral side's columns: build it once against a row of
    // NULLs, which yields its schema without depending on any outer value.
    let scope = match scope {
        Some(scope) => scope,
        None => {
            let nulls = vec![Datum::Null; acc.scope.width()];
            let (specialized, _) = binder.bind(te, &acc.scope, &nulls);
            let right = build_table_expr(read_ctx, &specialized, None, None, None)?;
            join_relations(
                Relation {
                    scope: acc.scope.clone(),
                    rows: Vec::new(),
                },
                right,
                kind,
                constraint,
                ctx,
                read_ctx.join_policy(),
            )?
            .scope
        }
    };
    Ok(Relation { scope, rows })
}

pub(crate) fn lateral_cacheable(te: &crabka_pgparser::ast::TableExpr) -> bool {
    use crabka_pgparser::ast::{DistinctClause, QueryBody, SetExpr, TableExpr};
    let TableExpr::Derived {
        subquery,
        lateral: true,
        ..
    } = te
    else {
        return false;
    };
    let SetExpr::Query(QueryBody::Select(select)) = &subquery.body else {
        return false;
    };
    subquery.with.is_none()
        && subquery.order_by.is_empty()
        && subquery.limit.is_none()
        && subquery
            .offset
            .as_ref()
            .is_none_or(|offset| matches!(offset, Expr::IntLiteral(value) if value == "0"))
        && subquery.locking.is_none()
        && select.from.len() == 1
        && matches!(&select.from[0], TableExpr::Table { .. })
        && matches!(select.distinct, DistinctClause::All)
        && select.group_by.is_empty()
        && select.grouping.is_none()
        && select.having.is_none()
        && select.windows.is_empty()
        && select.window_calls.is_empty()
        && select.order_by.is_empty()
        && select.limit.is_none()
        && select.offset.is_none()
        && select.locking.is_none()
        && select.projection.iter().all(|item| match item {
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => true,
            SelectItem::Expr { expr, .. } => lateral_cacheable_expr(expr),
        })
        && select.filter.as_ref().is_none_or(lateral_cacheable_expr)
}

fn lateral_cacheable_expr(expr: &Expr) -> bool {
    match expr {
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BitStringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Column { .. }
        | Expr::Param(_)
        | Expr::Const { .. } => true,
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } | Expr::Collate { expr, .. } => {
            lateral_cacheable_expr(expr)
        }
        Expr::Binary { left, right, .. } => {
            lateral_cacheable_expr(left) && lateral_cacheable_expr(right)
        }
        _ => false,
    }
}
