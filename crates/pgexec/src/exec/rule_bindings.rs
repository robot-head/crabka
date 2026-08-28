//! Bind rewrite-rule action trees to the OLD and NEW row images.

use super::{rule_images::*, *};

fn bind_rule_query(
    query: &mut crabka_pgparser::ast::QueryExpr,
    table: &Table,
    old: Option<&[Datum]>,
    new: Option<&[Datum]>,
) -> Result<(), ExecError> {
    bind_rule_query_values(query, table, old, new)?;
    let mut error = None;
    crate::viewwrite::map_query(
        query,
        &mut |node, _| match rule_image_expr(node, table, old, new) {
            Ok(replacement) => replacement,
            Err(err) => {
                error = Some(err);
                None
            }
        },
    );
    match error {
        Some(error) => Err(error),
        None => bind_rule_query_wildcards(query, table, old, new),
    }
}

fn bind_rule_query_values(
    query: &mut crabka_pgparser::ast::QueryExpr,
    table: &Table,
    old: Option<&[Datum]>,
    new: Option<&[Datum]>,
) -> Result<(), ExecError> {
    fn bind_set_expr(
        body: &mut crabka_pgparser::ast::SetExpr,
        table: &Table,
        old: Option<&[Datum]>,
        new: Option<&[Datum]>,
    ) -> Result<(), ExecError> {
        match body {
            crabka_pgparser::ast::SetExpr::Query(crabka_pgparser::ast::QueryBody::Values(
                values,
            )) => {
                for row in &mut values.rows {
                    bind_rule_values(row, table, old, new)?;
                }
            }
            crabka_pgparser::ast::SetExpr::Query(crabka_pgparser::ast::QueryBody::Nested(
                query,
            )) => bind_rule_query_values(query, table, old, new)?,
            crabka_pgparser::ast::SetExpr::SetOp { left, right, .. } => {
                bind_set_expr(left, table, old, new)?;
                bind_set_expr(right, table, old, new)?;
            }
            crabka_pgparser::ast::SetExpr::Query(crabka_pgparser::ast::QueryBody::Select(_)) => {}
        }
        Ok(())
    }
    bind_set_expr(&mut query.body, table, old, new)
}

fn bind_rule_query_wildcards(
    query: &mut crabka_pgparser::ast::QueryExpr,
    table: &Table,
    old: Option<&[Datum]>,
    new: Option<&[Datum]>,
) -> Result<(), ExecError> {
    fn bind_set_expr(
        body: &mut crabka_pgparser::ast::SetExpr,
        table: &Table,
        old: Option<&[Datum]>,
        new: Option<&[Datum]>,
    ) -> Result<(), ExecError> {
        match body {
            crabka_pgparser::ast::SetExpr::Query(crabka_pgparser::ast::QueryBody::Select(
                select,
            )) => {
                let mut projection = Vec::with_capacity(select.projection.len());
                for item in std::mem::take(&mut select.projection) {
                    let crabka_pgparser::ast::SelectItem::QualifiedWildcard(qualifier) = &item
                    else {
                        projection.push(item);
                        continue;
                    };
                    let values = match qualifier.as_str() {
                        "old" => old,
                        "new" => new,
                        _ => {
                            projection.push(item);
                            continue;
                        }
                    }
                    .ok_or_else(|| {
                        ExecError::Unsupported(format!(
                            "{qualifier} is not available for this rewrite rule event"
                        ))
                    })?;
                    projection.extend(values.iter().zip(&table.columns).map(|(value, column)| {
                        SelectItem::Expr {
                            expr: Expr::Const {
                                value: value.clone(),
                                ty: column.ty,
                            },
                            alias: Some(column.name.clone()),
                        }
                    }));
                }
                select.projection = projection;
            }
            crabka_pgparser::ast::SetExpr::Query(crabka_pgparser::ast::QueryBody::Nested(
                query,
            )) => bind_rule_query_wildcards(query, table, old, new)?,
            crabka_pgparser::ast::SetExpr::SetOp { left, right, .. } => {
                bind_set_expr(left, table, old, new)?;
                bind_set_expr(right, table, old, new)?;
            }
            crabka_pgparser::ast::SetExpr::Query(crabka_pgparser::ast::QueryBody::Values(_)) => {}
        }
        Ok(())
    }
    if let Some(with) = &mut query.with {
        for cte in &mut with.ctes {
            if let crabka_pgparser::ast::CteBody::Query(query) = &mut cte.body {
                bind_rule_query_wildcards(query, table, old, new)?;
            }
        }
    }
    bind_set_expr(&mut query.body, table, old, new)
}

fn bind_rule_values(
    values: &mut Vec<Expr>,
    table: &Table,
    old: Option<&[Datum]>,
    new: Option<&[Datum]>,
) -> Result<(), ExecError> {
    let mut bound = Vec::new();
    for expr in std::mem::take(values) {
        let Expr::Column {
            table: Some(qualifier),
            name,
        } = &expr
        else {
            bound.push(bind_rule_expr(&expr, table, old, new)?);
            continue;
        };
        let values = match qualifier.as_str() {
            "old" => old,
            "new" => new,
            _ => {
                bound.push(bind_rule_expr(&expr, table, old, new)?);
                continue;
            }
        }
        .ok_or_else(|| {
            ExecError::Unsupported(format!(
                "{qualifier} is not available for this rewrite rule event"
            ))
        })?;
        if name == "*" {
            bound.extend(
                values
                    .iter()
                    .zip(&table.columns)
                    .map(|(value, column)| Expr::Const {
                        value: value.clone(),
                        ty: column.ty,
                    }),
            );
        } else {
            bound.push(bind_rule_expr(&expr, table, old, new)?);
        }
    }
    *values = bound;
    Ok(())
}

fn bind_rule_returning(
    returning: &mut crabka_pgparser::ast::Returning,
    table: &Table,
    old: Option<&[Datum]>,
    new: Option<&[Datum]>,
) -> Result<(), ExecError> {
    let mut items = Vec::with_capacity(returning.items.len());
    for item in std::mem::take(&mut returning.items) {
        let SelectItem::QualifiedWildcard(qualifier) = &item else {
            let SelectItem::Expr { expr, alias } = item else {
                items.push(item);
                continue;
            };
            items.push(SelectItem::Expr {
                expr: bind_rule_expr(&expr, table, old, new)?,
                alias,
            });
            continue;
        };
        let values = match qualifier.as_str() {
            "old" => old,
            "new" => new,
            _ => {
                items.push(item);
                continue;
            }
        }
        .ok_or_else(|| {
            ExecError::Unsupported(format!(
                "{qualifier} is not available for this rewrite rule event"
            ))
        })?;
        items.extend(
            values
                .iter()
                .zip(&table.columns)
                .map(|(value, column)| SelectItem::Expr {
                    expr: Expr::Const {
                        value: value.clone(),
                        ty: column.ty,
                    },
                    alias: Some(column.name.clone()),
                }),
        );
    }
    returning.items = items;
    Ok(())
}

fn bind_rule_table_expr(
    item: &mut crabka_pgparser::ast::TableExpr,
    table: &Table,
    old: Option<&[Datum]>,
    new: Option<&[Datum]>,
) -> Result<(), ExecError> {
    use crabka_pgparser::ast::TableExpr;
    match item {
        TableExpr::Derived { subquery, .. } => bind_rule_query(subquery, table, old, new),
        TableExpr::Function { functions, .. } => {
            for function in functions {
                for argument in function.arguments_mut() {
                    *argument = bind_rule_expr(argument, table, old, new)?;
                }
            }
            Ok(())
        }
        TableExpr::JsonTable(json_table) => {
            for expr in json_table.exprs_mut() {
                *expr = bind_rule_expr(expr, table, old, new)?;
            }
            Ok(())
        }
        TableExpr::XmlTable(xml_table) => {
            for expr in xml_table.exprs_mut() {
                *expr = bind_rule_expr(expr, table, old, new)?;
            }
            Ok(())
        }
        TableExpr::Join {
            left,
            right,
            constraint,
            ..
        } => {
            bind_rule_table_expr(left, table, old, new)?;
            bind_rule_table_expr(right, table, old, new)?;
            if let crabka_pgparser::ast::JoinConstraint::On(expr) = constraint {
                *expr = bind_rule_expr(expr, table, old, new)?;
            }
            Ok(())
        }
        TableExpr::Table { .. } => Ok(()),
    }
}

/// Bind a rule action to the row images that fired it before re-entering the ordinary write executor.
pub(super) fn bind_rule_action(
    action: &mut Statement,
    table: &Table,
    old: Option<&[Datum]>,
    new: Option<&[Datum]>,
) -> Result<(), ExecError> {
    match action {
        Statement::Insert {
            source: crabka_pgparser::ast::InsertSource::Values(rows),
            returning,
            ..
        } => {
            for values in rows {
                bind_rule_values(values, table, old, new)?;
            }
            if let Some(returning) = returning {
                bind_rule_returning(returning, table, old, new)?;
            }
        }
        Statement::Insert {
            source: crabka_pgparser::ast::InsertSource::Query(query),
            returning,
            ..
        } => {
            bind_rule_query(query, table, old, new)?;
            if let Some(returning) = returning {
                bind_rule_returning(returning, table, old, new)?;
            }
        }
        Statement::Update {
            assignments,
            from,
            filter,
            returning,
            ..
        } => {
            for item in from {
                bind_rule_table_expr(item, table, old, new)?;
            }
            for assignment in assignments {
                match &mut assignment.value {
                    crabka_pgparser::ast::AssignmentValue::Expr(expr) => {
                        *expr = bind_rule_expr(expr, table, old, new)?
                    }
                    crabka_pgparser::ast::AssignmentValue::Row(exprs) => {
                        for expr in exprs {
                            *expr = bind_rule_expr(expr, table, old, new)?;
                        }
                    }
                    crabka_pgparser::ast::AssignmentValue::Subquery(query) => {
                        bind_rule_query(query, table, old, new)?
                    }
                }
            }
            if let Some(filter) = filter {
                *filter = bind_rule_expr(filter, table, old, new)?;
            }
            if let Some(returning) = returning {
                bind_rule_returning(returning, table, old, new)?;
            }
        }
        Statement::Delete {
            using,
            filter,
            returning,
            ..
        } => {
            for item in using {
                bind_rule_table_expr(item, table, old, new)?;
            }
            if let Some(filter) = filter {
                *filter = bind_rule_expr(filter, table, old, new)?;
            }
            if let Some(returning) = returning {
                bind_rule_returning(returning, table, old, new)?;
            }
        }
        Statement::Notify { .. } => {}
        Statement::Query(query) => bind_rule_query(query, table, old, new)?,
        Statement::Insert { returning, .. } => {
            if let Some(returning) = returning {
                bind_rule_returning(returning, table, old, new)?;
            }
        }
        _ => {
            return Err(ExecError::Unsupported(
                "rewrite rule actions must be INSERT, UPDATE, or DELETE".into(),
            ));
        }
    }
    Ok(())
}
