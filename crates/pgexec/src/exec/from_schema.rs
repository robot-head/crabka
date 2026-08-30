use super::*;

/// A FROM item with any lateral reference to `outer` replaced by a NULL of that
/// column's type, so a schema-only describe can resolve it. Non-lateral items
/// pass through untouched.
fn lateral_schema_item(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    ctes: &crate::cte::CteContext,
    te: &crabka_pgparser::ast::TableExpr,
    outer: &Scope,
) -> crabka_pgparser::ast::TableExpr {
    if !is_lateral_item(te, outer) {
        return te.clone();
    }
    let nulls = vec![Datum::Null; outer.width()];
    LateralBinder::new(catalog_kv, resolution, ctes)
        .bind(te, outer, &nulls)
        .0
}

pub(crate) fn build_from_schema_with_ctes_and_context(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    from: &[crabka_pgparser::ast::TableExpr],
    ctes: &crate::cte::CteContext,
    ctx: Option<&crate::clock::EvalCtx>,
) -> Result<Relation, ExecError> {
    build_from_schema_described(catalog_kv, resolution, from, ctes, ctx, None)
}

/// The describe walk for one SELECT, which is therefore able to describe the
/// hidden `tableoid` that statement reads.
///
/// Describe and execute must agree about the column: a describe that omits it
/// answers 42703 for a statement the read path would have answered, which is
/// what `SELECT tableoid …` did under the extended protocol, inside `CREATE
/// VIEW`, inside `CREATE TABLE AS` and in a set-op branch, while the identical
/// text ran under the simple protocol.
pub(crate) fn build_from_schema_of_select(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    select: &SelectStmt,
    ctes: &crate::cte::CteContext,
) -> Result<Relation, ExecError> {
    let refs = crate::scope::StatementRefs::of_select(select);
    build_from_schema_described(
        catalog_kv,
        resolution,
        &select.from,
        ctes,
        None,
        Some(&refs),
    )
}

pub(crate) fn build_from_schema_of_select_with_context(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    select: &SelectStmt,
    ctes: &crate::cte::CteContext,
    ctx: &crate::clock::EvalCtx,
) -> Result<Relation, ExecError> {
    let refs = crate::scope::StatementRefs::of_select(select);
    build_from_schema_described(
        catalog_kv,
        resolution,
        &select.from,
        ctes,
        Some(ctx),
        Some(&refs),
    )
}

/// The describe walk, told which statement it is describing.
///
/// `refs` is the very thing the read path carries on its
/// [`crate::subquery::SubCtx`], and means the same here: `None` is "no statement
/// in hand", and then a stored relation is described without its `tableoid`
/// exactly as it is scanned without one. The plpgsql record and FOR-loop scopes
/// are the callers with nothing to pass.
pub(super) fn build_from_schema_described(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    from: &[crabka_pgparser::ast::TableExpr],
    ctes: &crate::cte::CteContext,
    ctx: Option<&crate::clock::EvalCtx>,
    refs: Option<&crate::scope::StatementRefs>,
) -> Result<Relation, ExecError> {
    let mut iter = from.iter();
    let first = iter
        .next()
        .ok_or_else(|| ExecError::Unsupported("build_from_schema on empty FROM".into()))?;
    let mut acc =
        build_table_expr_schema_with_ctes(catalog_kv, resolution, first, ctes, ctx, refs)?;
    for te in iter {
        // A lateral item references the accumulated columns, which no schema
        // description of it on its own can resolve. Substituting NULLs of the
        // right types leaves an item the ordinary describe understands and whose
        // output columns are unchanged.
        let te = &lateral_schema_item(catalog_kv, resolution, ctes, te, &acc.scope);
        let next = build_table_expr_schema_with_ctes(catalog_kv, resolution, te, ctes, ctx, refs)?;
        // Schema-only: no rows, so no ON predicate is ever evaluated — a default
        // (UTC/epoch) eval context is correct here.
        acc = join_relations(
            acc,
            next,
            crabka_pgparser::ast::JoinKind::Cross,
            &crabka_pgparser::ast::JoinConstraint::None,
            &crate::clock::EvalCtx::test_default(),
            crate::join::JoinPolicy::default(),
        )?;
    }
    Ok(acc)
}

fn build_table_expr_schema_with_ctes(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    te: &crabka_pgparser::ast::TableExpr,
    ctes: &crate::cte::CteContext,
    ctx: Option<&crate::clock::EvalCtx>,
    refs: Option<&crate::scope::StatementRefs>,
) -> Result<Relation, ExecError> {
    use crabka_pgparser::ast::TableExpr;
    match te {
        TableExpr::Table {
            name,
            alias,
            columns,
            ..
        } => {
            if let Some(names) = columns {
                let base = build_table_expr_schema_with_ctes(
                    catalog_kv,
                    resolution,
                    &TableExpr::Table {
                        name: name.clone(),
                        only: false,
                        alias: alias.clone(),
                        columns: None,
                        sample: None,
                    },
                    ctes,
                    ctx,
                    refs,
                )?;
                let qualifier = alias.clone().unwrap_or_else(|| name.to_string());
                return crate::values::requalify_derived(base, &qualifier, &Some(names.clone()));
            }
            if name.schema.is_none()
                && let Some(rel) = ctes.lookup(&name.name)
            {
                let qualifier = alias.as_deref().unwrap_or(&name.name);
                let mut rel = crate::cte::requalify_cte(rel, qualifier);
                rel.rows.clear();
                return Ok(rel);
            }
            if name.schema.is_none()
                && let Some(runtime) = ctx.and_then(|ctx| ctx.transition_relations.as_ref())
                && let Some(transition) = runtime
                    .lock()
                    .expect("transition relation mutex")
                    .get(&name.name)
                    .cloned()
            {
                let qualifier = alias.as_deref().unwrap_or(&name.name);
                return Ok(Relation {
                    scope: Scope {
                        columns: transition
                            .columns
                            .into_iter()
                            .map(|(name, ty)| ColumnBinding {
                                exposure: Exposure::Output,
                                qualifier: Some(qualifier.to_string()),
                                name,
                                ty,
                            })
                            .collect(),
                        ..Default::default()
                    },
                    rows: Vec::new(),
                });
            }
            let name =
                &resolve_relation(catalog_kv, resolution, name, SchemaDisposition::Reference)?;
            if let Some(rel) =
                virtual_catalog_relation_schema(catalog_kv, name, alias.as_deref(), refs)?
            {
                return Ok(rel);
            }
            match crabka_pgcatalog::get_view(catalog_kv, name) {
                Ok(view) => {
                    let qualifier = alias.as_deref().unwrap_or(&view.name.name);
                    let mut scope = Scope {
                        columns: view
                            .columns
                            .iter()
                            .map(|column| ColumnBinding {
                                exposure: Exposure::Output,
                                qualifier: Some(qualifier.to_string()),
                                name: column.name.clone(),
                                ty: column.ty,
                            })
                            .collect(),
                        ..Default::default()
                    };
                    scope.replace_row_type(
                        qualifier,
                        crate::catalog_rel::relation_rowtype(catalog_kv, &view.name)?,
                    );
                    return Ok(Relation {
                        scope,
                        rows: Vec::new(),
                    });
                }
                Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => {}
                Err(error) => return Err(error.into()),
            }
            let t = crabka_pgcatalog::get_table(catalog_kv, name).map_err(|error| {
                open_wrong_kind(catalog_kv, name).unwrap_or_else(|| error.into())
            })?;
            crate::exec::foreign_scan::require_handler(catalog_kv, &t)?;
            let qualifier = alias.as_deref().unwrap_or(&t.name.name);
            // A view, a CTE, a derived table and a VALUES list all return above
            // with no system column, which is what makes `SELECT tableoid FROM
            // v` the 42703 `PostgreSQL` raises rather than a column of nulls. A
            // virtual catalog relation returns above WITH its `ctid`, because
            // the engine numbers the rows it projects.
            let mut scope = relation_scope(catalog_kv, &t, qualifier)?;
            crate::scope::SystemColumns::of(refs, &t).extend_scope(&mut scope, qualifier);
            Ok(Relation {
                scope,
                rows: Vec::new(),
            })
        }
        TableExpr::Join {
            left,
            right,
            kind,
            constraint,
        } => {
            let l =
                build_table_expr_schema_with_ctes(catalog_kv, resolution, left, ctes, ctx, refs)?;
            let right = &lateral_schema_item(catalog_kv, resolution, ctes, right, &l.scope);
            let r =
                build_table_expr_schema_with_ctes(catalog_kv, resolution, right, ctes, ctx, refs)?;
            // Schema-only: no rows, so no ON predicate is ever evaluated.
            join_relations(
                l,
                r,
                *kind,
                constraint,
                &crate::clock::EvalCtx::test_default(),
                crate::join::JoinPolicy::default(),
            )
        }
        TableExpr::Derived {
            subquery,
            alias,
            columns,
            ..
        } => {
            let fields = crate::query::describe_query_expr_with_ctes(
                catalog_kv, resolution, subquery, ctes,
            )?;
            let bindings = fields
                .iter()
                .map(|f| {
                    Ok(ColumnBinding {
                        exposure: Exposure::Output,
                        qualifier: None,
                        name: f.name.clone(),
                        ty: column_type_from_oid(f.type_oid)?,
                    })
                })
                .collect::<Result<_, ExecError>>()?;
            let inner = Relation {
                scope: Scope {
                    columns: bindings,
                    ..Default::default()
                },
                rows: Vec::new(),
            };
            crate::values::requalify_derived(inner, alias, columns)
        }
        TableExpr::Function {
            functions,
            with_ordinality,
            rows_from: true,
            alias,
            column_aliases,
            ..
        } => {
            let functions = functions
                .iter()
                .map(|call| crate::routine::normalize_table_function_call(catalog_kv, call))
                .collect::<Result<Vec<_>, _>>()?;
            let mut calls = Vec::with_capacity(functions.len());
            for call in &functions {
                if let Some((_routine, columns)) =
                    crate::routine::plpgsql_table_function_schema(catalog_kv, call)?
                {
                    calls.push((columns, Vec::new()));
                } else {
                    calls.push((crate::srf::function_call_schema(call)?, Vec::new()));
                }
            }
            crate::srf::rows_from_function_relation(
                &functions[0].name,
                calls,
                *with_ordinality,
                alias.as_deref(),
                column_aliases,
            )
        }
        TableExpr::Function {
            functions,
            with_ordinality,
            rows_from,
            alias,
            column_aliases,
            ..
        } => {
            let functions = functions
                .iter()
                .map(|call| crate::routine::normalize_table_function_call(catalog_kv, call))
                .collect::<Result<Vec<_>, _>>()?;
            if functions.len() == 1
                && let Some((_routine, columns)) =
                    crate::routine::plpgsql_table_function_schema(catalog_kv, &functions[0])?
            {
                return crate::srf::user_function_relation(
                    &functions[0].name,
                    columns,
                    Vec::new(),
                    *with_ordinality,
                    alias.as_deref(),
                    column_aliases,
                    functions[0].column_defs.as_deref(),
                    None,
                );
            }
            if functions.len() == 1 && crate::routine::expands_as_table(catalog_kv, &functions) {
                let (query, _routine, names) =
                    crate::routine::table_function_expansion(catalog_kv, &functions[0])?;
                let fields = crate::query::describe_query_expr_with_ctes(
                    catalog_kv, resolution, &query, ctes,
                )?;
                let bindings = fields
                    .iter()
                    .map(|field| {
                        Ok(ColumnBinding {
                            exposure: Exposure::Output,
                            qualifier: None,
                            name: field.name.clone(),
                            ty: column_type_from_oid(field.type_oid)?,
                        })
                    })
                    .collect::<Result<_, ExecError>>()?;
                let mut relation = Relation {
                    scope: Scope {
                        columns: bindings,
                        ..Default::default()
                    },
                    rows: Vec::new(),
                };
                if *with_ordinality {
                    relation = crate::srf::with_ordinality(relation);
                }
                return crate::values::requalify_derived(
                    relation,
                    alias.as_deref().unwrap_or(&functions[0].name),
                    &column_aliases.clone().or(Some(names)),
                );
            }
            crate::srf::from_item_schema(
                &functions,
                *with_ordinality,
                *rows_from,
                alias.as_deref(),
                column_aliases,
            )
        }
        TableExpr::JsonTable(table) => crate::jsontable::from_item_schema(table),
        TableExpr::XmlTable(table) => crate::xmltable::from_item_schema(table),
    }
}
