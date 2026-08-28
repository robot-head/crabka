//! FROM relation column retention.

use super::*;

pub(crate) fn prune_relation_columns(
    mut relation: Relation,
    pruned_columns: Option<&[ColumnBinding]>,
) -> Relation {
    let Some(pruned_columns) = pruned_columns else {
        return relation;
    };
    for row in &mut relation.rows {
        for (column, datum) in relation.scope.columns.iter().zip(row) {
            if !pruned_columns.contains(column) {
                *datum = Datum::Null;
            }
        }
    }
    relation
}

/// Columns a simple FROM list must retain after its joins have materialized.
///
/// The legacy executor builds virtual catalogs as full rows before joining them.
/// Retaining just the columns the query reads keeps those joins within the
/// statement budget, without changing the relation's visible shape.
pub(crate) fn live_from_columns(
    read_ctx: &crate::subquery::SubCtx<'_>,
    select: &SelectStmt,
) -> Option<Vec<ColumnBinding>> {
    use crabka_pgparser::ast::TableExpr;

    if !select.order_by.is_empty()
        || matches!(select.distinct, crabka_pgparser::ast::DistinctClause::On(_))
        || crate::grouping::is_grouping_query(select)
        || crate::window::has_window_calls(select)
        || crate::srf::projection_contains_srf(&select.projection)
        || !select.from.iter().all(|item| {
            matches!(
                item,
                TableExpr::Table {
                    columns: None,
                    sample: None,
                    ..
                }
            )
        })
    {
        return None;
    }

    let scope = build_from_schema_of_select_with_context(
        read_ctx.catalog_kv,
        read_ctx.fctx.resolution,
        select,
        read_ctx.ctes,
        read_ctx.eval_ctx,
    )
    .ok()?
    .scope;
    let (_, mut expressions, _) = resolve_projection(&select.projection, &scope).ok()?;
    if let Some(filter) = &select.filter {
        expressions.push(filter.clone());
    }
    let expressions = crate::bind::bind_all(&expressions, &scope).ok()?;
    let mut positions = BTreeSet::new();
    for expression in expressions {
        let mut positional = true;
        crate::grouping::visit_expr(expression.expr(), &mut |node| {
            if let Expr::Column { table, name } = node {
                match table
                    .as_deref()
                    .filter(|table| *table == POSITION_QUALIFIER)
                {
                    Some(_) => match name.parse::<usize>() {
                        Ok(position) => {
                            positions.insert(position);
                        }
                        Err(_) => positional = false,
                    },
                    None => positional = false,
                }
            }
        });
        if !positional {
            return None;
        }
    }
    positions
        .into_iter()
        .map(|position| scope.columns.get(position).cloned())
        .collect()
}
