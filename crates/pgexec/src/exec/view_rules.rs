use super::*;

pub(super) fn is_view_ref(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    reference: &crabka_pgparser::ast::RelationRef,
) -> Result<bool, ExecError> {
    let name = resolve_relation(kv, resolution, reference, SchemaDisposition::Reference)?;
    match crabka_pgcatalog::get_view(kv, &name) {
        Ok(_) => Ok(true),
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn has_instead_view_rule(
    kv: &dyn Kv,
    view: &Table,
    event: crabka_pgcatalog::rule::RuleEvent,
) -> Result<bool, ExecError> {
    Ok(crabka_pgcatalog::rule::rules_for_table(kv, view.id)?
        .into_iter()
        .any(|rule| rule_is_enabled(rule.enabled) && rule.event == event && rule.instead))
}

pub(super) async fn execute_view_rewrite_rules(
    read_ctx: &WriteContext<'_>,
    action_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    stmt: &Statement,
    view: &Table,
    only_instead: bool,
    writes: &mut StatementWrites,
) -> Result<(WriteOutcome, Vec<crabka_pgkv::WriteOp>), ExecError> {
    let ctx = read_ctx.eval_ctx;
    match stmt {
        Statement::Insert {
            columns,
            indirections,
            source,
            returning,
            ..
        } => {
            let (targets, rows) =
                insert_source_rows(read_ctx, ctes, view, columns, indirections, source)?;
            let mut ops = Vec::new();
            let mut returned = None;
            let mut count = 0_u64;
            for row in rows {
                let mut proposed =
                    build_insert_row_with_subscripts(view, &targets, indirections, &row, ctx)?;
                finish_written_row(view, &mut proposed, ctx)?;
                let (matched, action_ops, action_returning) = fire_insert_rules(
                    action_ctx,
                    ctes,
                    view,
                    &targets,
                    &row,
                    &proposed,
                    Some(only_instead),
                    returning.is_some(),
                    writes,
                )
                .await?;
                if matched || !only_instead {
                    count += 1;
                    ops.extend(action_ops);
                    append_rule_returning(&mut returned, action_returning)?;
                }
            }
            Ok((
                WriteOutcome {
                    tag: format!("INSERT 0 {count}"),
                    returning: returned,
                },
                ops,
            ))
        }
        Statement::Update {
            alias,
            assignments,
            from,
            filter,
            returning,
            ..
        } => {
            let qualifier = table_qualifier(view, alias);
            let read = read_ctx.read_ctx(ctes);
            let target_expr = crabka_pgparser::ast::TableExpr::Table {
                name: crabka_pgparser::ast::RelationRef::qualified(
                    &view.name.schema,
                    &view.name.name,
                ),
                only: true,
                alias: alias.clone(),
                columns: None,
                sample: None,
            };
            let target_rows = build_from(
                &read,
                std::slice::from_ref(&target_expr),
                None,
                None,
                None,
                None,
            )?
            .rows;
            let source = DmlSource::build(read_ctx, ctes, view, qualifier, from, None)?;
            let filter = source.resolve_filter(&read, filter.as_ref())?;
            let mut binder = filter
                .as_ref()
                .map(|filter| validate_correlated_subqueries(&read, filter, &source.scope))
                .transpose()?
                .unwrap_or(false)
                .then(|| LateralBinder::new(read.catalog_kv, read.fctx.resolution, read.ctes));
            let bound_filter = if binder.is_none() {
                source.bind_filter(filter.as_ref())?
            } else {
                None
            };
            let targets = resolve_assignments(read_ctx, ctes, view, assignments)?;
            let mut ops = Vec::new();
            let mut returned = None;
            let mut count = 0_u64;
            for old in target_rows {
                let Some(joined) = (if let Some(binder) = &mut binder {
                    source.first_match_correlated(
                        &read,
                        filter.as_ref(),
                        &old,
                        NO_ROW_IDENTITY,
                        0,
                        binder,
                    )?
                } else {
                    source.first_match(bound_filter.as_ref(), &old, NO_ROW_IDENTITY, 0, ctx)?
                }) else {
                    continue;
                };
                let mut proposed = apply_assignments(view, &targets, &source.scope, &joined, ctx)?;
                finish_written_row(view, &mut proposed, ctx)?;
                let (matched, action_ops, action_returning) = fire_row_rules(
                    action_ctx,
                    ctes,
                    view,
                    crabka_pgcatalog::rule::RuleEvent::Update,
                    Some(&old),
                    Some(&proposed),
                    only_instead,
                    returning.is_some(),
                    writes,
                )
                .await?;
                if matched {
                    count += 1;
                    ops.extend(action_ops);
                    append_rule_returning(&mut returned, action_returning)?;
                }
            }
            Ok((
                WriteOutcome {
                    tag: format!("UPDATE {count}"),
                    returning: returned,
                },
                ops,
            ))
        }
        Statement::Delete {
            alias,
            using,
            filter,
            returning,
            ..
        } => {
            let qualifier = table_qualifier(view, alias);
            let read = read_ctx.read_ctx(ctes);
            let target_expr = crabka_pgparser::ast::TableExpr::Table {
                name: crabka_pgparser::ast::RelationRef::qualified(
                    &view.name.schema,
                    &view.name.name,
                ),
                only: true,
                alias: alias.clone(),
                columns: None,
                sample: None,
            };
            let target_rows = build_from(
                &read,
                std::slice::from_ref(&target_expr),
                None,
                None,
                None,
                None,
            )?
            .rows;
            let source = DmlSource::build(read_ctx, ctes, view, qualifier, using, None)?;
            let filter = source.resolve_filter(&read, filter.as_ref())?;
            let mut binder = filter
                .as_ref()
                .map(|filter| validate_correlated_subqueries(&read, filter, &source.scope))
                .transpose()?
                .unwrap_or(false)
                .then(|| LateralBinder::new(read.catalog_kv, read.fctx.resolution, read.ctes));
            let bound_filter = if binder.is_none() {
                source.bind_filter(filter.as_ref())?
            } else {
                None
            };
            let mut ops = Vec::new();
            let mut returned = None;
            let mut count = 0_u64;
            for old in target_rows {
                let Some(_) = (if let Some(binder) = &mut binder {
                    source.first_match_correlated(
                        &read,
                        filter.as_ref(),
                        &old,
                        NO_ROW_IDENTITY,
                        0,
                        binder,
                    )?
                } else {
                    source.first_match(bound_filter.as_ref(), &old, NO_ROW_IDENTITY, 0, ctx)?
                }) else {
                    continue;
                };
                let (matched, action_ops, action_returning) = fire_row_rules(
                    action_ctx,
                    ctes,
                    view,
                    crabka_pgcatalog::rule::RuleEvent::Delete,
                    Some(&old),
                    None,
                    only_instead,
                    returning.is_some(),
                    writes,
                )
                .await?;
                if matched {
                    count += 1;
                    ops.extend(action_ops);
                    append_rule_returning(&mut returned, action_returning)?;
                }
            }
            Ok((
                WriteOutcome {
                    tag: format!("DELETE {count}"),
                    returning: returned,
                },
                ops,
            ))
        }
        Statement::Merge { .. } => Err(ExecError::Unsupported(
            "MERGE into a view with rewrite rules is not supported".into(),
        )),
        _ => unreachable!("view rewrite rules only accept DML"),
    }
}
