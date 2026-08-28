use super::*;

/// Read one base-relation `FROM` item: a CTE, a transition relation, a virtual
/// catalog relation, a view, or a stored relation.
///
/// Split out of [`build_table_expr`]'s match rather than written inline. That
/// function recurses — through a join side, a derived table, and a `plpgsql`
/// set-returning function that calls itself — and in an unoptimized build a
/// frame holds slots for every arm's locals whether that arm ran or not, so an
/// arm this size is paid at every level of the recursion.
pub(super) fn build_base_table(
    read_ctx: &crate::subquery::SubCtx<'_>,
    te: &crabka_pgparser::ast::TableExpr,
    bounds: Option<&ScanBounds>,
    scan_plan: Option<&crate::plan_dist::DistributedScanPlan>,
) -> Result<Relation, ExecError> {
    use crabka_pgparser::ast::TableExpr;
    let TableExpr::Table {
        name,
        only,
        alias,
        columns,
        sample,
    } = te
    else {
        return Err(ExecError::Unsupported(
            "build_base_table expects a base relation".into(),
        ));
    };
    let catalog_kv = read_ctx.catalog_kv;
    let resolution = read_ctx.fctx.resolution;
    let ctes = read_ctx.ctes;
    let ctx = read_ctx.eval_ctx;
    // A base-table alias may rename the leading columns (`t AS q(x, y)`),
    // exactly like a derived table's. The rename applies to whatever the
    // name resolves to — a CTE, a view, a catalog relation, or a stored
    // table — so it wraps the ordinary build rather than duplicating it.
    if let Some(names) = columns {
        let base = build_table_expr(
            read_ctx,
            &TableExpr::Table {
                name: name.clone(),
                only: *only,
                alias: alias.clone(),
                columns: None,
                sample: sample.clone(),
            },
            bounds,
            scan_plan,
            None,
        )?;
        let qualifier = alias.clone().unwrap_or_else(|| name.to_string());
        return crate::values::requalify_derived(base, &qualifier, &Some(names.clone()));
    }
    if let Some(sample) = sample {
        let base = build_table_expr(
            read_ctx,
            &TableExpr::Table {
                name: name.clone(),
                only: *only,
                alias: alias.clone(),
                columns: columns.clone(),
                sample: None,
            },
            bounds,
            scan_plan,
            None,
        )?;
        return apply_tablesample(base, sample, ctx);
    }
    // A CTE is never schema-qualified, so `public.t` names the stored
    // relation even where a CTE `t` is in scope, as PostgreSQL does.
    if name.schema.is_none()
        && let Some(rel) = ctes.lookup(&name.name)
    {
        let qualifier = alias.as_deref().unwrap_or(&name.name);
        return Ok(crate::cte::requalify_cte(rel, qualifier));
    }
    if name.schema.is_none()
        && let Some(runtime) = &ctx.transition_relations
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
            rows: transition.rows,
        });
    }
    let name = &resolve_relation(catalog_kv, resolution, name, SchemaDisposition::Reference)?;
    if let Some(rel) =
        virtual_catalog_relation(catalog_kv, name, alias.as_deref(), ctx, read_ctx.refs)?
    {
        return Ok(rel);
    }
    match crabka_pgcatalog::get_view(catalog_kv, name) {
        Ok(view) => {
            // A view carries its own ACL, and it is checked *before* the
            // identity switch below — under whatever role reached this view,
            // which for a nested view is the outer view's owner. This check is
            // what makes owner rights safe: the body may read relations the
            // caller cannot, so the caller must have been granted the view.
            crate::privilege::require(
                &read_ctx.privileges(),
                &view.name,
                &view.owner,
                crate::privilege::RelationKind::View,
                crate::privilege::Privilege::Select,
            )?;
            let statement = crabka_pgparser::parse(&view.definition)?;
            let [Statement::Query(query)] = statement.as_slice() else {
                return Err(ExecError::Unsupported(
                    "stored view definition is not a query".into(),
                ));
            };
            // The body then runs as the view's owner — for privileges and for
            // which row-security policies apply, which move together because
            // they are the same field. `security_invoker` keeps the caller's
            // identity, which is the whole meaning of the option.
            //
            // `EvalCtx` is not touched, so `CURRENT_USER` inside the body
            // still names the invoker, as it does in PostgreSQL: only the
            // identity decisions are made under changes.
            let body_role = if view.options.security_invoker {
                read_ctx.security_role
            } else {
                view.owner.as_str()
            };
            // The body's unqualified names resolve with the view's own schema
            // searched first, not in the reader's scope. `CREATE VIEW s.v AS
            // SELECT … FROM t` records `t` as text, so reading `s.v` from a
            // session whose `search_path` does not name `s` would otherwise
            // fail to find the very relation the view was built on.
            let body_scope = read_ctx.fctx.resolution.for_stored_body(&view.name.schema);
            let role_ctx = read_ctx.with_security_role(body_role);
            let body_ctx = role_ctx.with_resolution(&body_scope);
            let relation = crate::query::query_to_relation(&body_ctx, query)?;
            let qualifier = alias.as_deref().unwrap_or(&view.name.name);
            return requalify_view_relation(
                relation,
                &view,
                qualifier,
                crate::catalog_rel::relation_rowtype(catalog_kv, &view.name)?,
            );
        }
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => {}
        Err(error) => return Err(error.into()),
    }
    scan_stored_base_table(read_ctx, te, name, bounds, scan_plan, None)
}

/// Whether `te` is the direct physical table shape the first `SeqScan` node
/// can own.  This is only planner eligibility: any catalog lookup failure
/// declines to the established read path, which reports the original error.
pub(crate) fn is_direct_stored_base_table(
    read_ctx: &crate::subquery::SubCtx<'_>,
    te: &crabka_pgparser::ast::TableExpr,
) -> bool {
    use crabka_pgparser::ast::TableExpr;
    let TableExpr::Table {
        name,
        columns,
        sample,
        ..
    } = te
    else {
        return false;
    };
    if columns.is_some() || sample.is_some() {
        return false;
    }
    if name.schema.is_none()
        && (read_ctx.ctes.lookup(&name.name).is_some()
            || read_ctx
                .eval_ctx
                .transition_relations
                .as_ref()
                .is_some_and(|runtime| {
                    runtime
                        .lock()
                        .expect("transition relation mutex")
                        .contains_key(&name.name)
                }))
    {
        return false;
    }
    let Ok(name) = resolve_relation(
        read_ctx.catalog_kv,
        read_ctx.fctx.resolution,
        name,
        SchemaDisposition::Reference,
    ) else {
        return false;
    };
    crabka_pgcatalog::get_view(read_ctx.catalog_kv, &name).is_err()
        && crabka_pgcatalog::get_table(read_ctx.catalog_kv, &name)
            .is_ok_and(|table| table.foreign.is_none() && !table.sharded)
}

/// Read the physical stored-relation leaf of a base-table item.
///
/// This is the one shared tail of the legacy base builder and `SeqScan`: the
/// permit is acquired before storage is touched, `RawScan` stays internal to
/// the security module, and row security is the only way out.
pub(crate) fn scan_stored_base_table(
    read_ctx: &crate::subquery::SubCtx<'_>,
    te: &crabka_pgparser::ast::TableExpr,
    name: &crabka_pgcatalog::RelationName,
    bounds: Option<&ScanBounds>,
    scan_plan: Option<&crate::plan_dist::DistributedScanPlan>,
    pruned_columns: Option<&[ColumnBinding]>,
) -> Result<Relation, ExecError> {
    let crabka_pgparser::ast::TableExpr::Table { only, alias, .. } = te else {
        return Err(ExecError::Unsupported(
            "scan_stored_base_table expects a base relation".into(),
        ));
    };
    // Consulted only once `get_table` has missed, so an ordinary read pays no
    // second catalog lookup for it.
    let t = crabka_pgcatalog::get_table(read_ctx.catalog_kv, name).map_err(|error| {
        open_wrong_kind(read_ctx.catalog_kv, name).unwrap_or_else(|| error.into())
    })?;
    // An unpopulated materialized view is refused here, at the one place every
    // stored-relation read passes, rather than in each of the planner's
    // pushdowns: the refusal has to hold whichever path a query takes, and a
    // pushdown that pruned the projection would otherwise return zero rows for
    // `count(*)` while the general path errored.
    require_populated(&t)?;
    let qualifier = alias.as_deref().unwrap_or(&t.name.name);
    // One scan, one gate, and one permit. Every stored-relation read the
    // executor does arrives here; `RawScan` cannot become a `Relation` any
    // other way, and `scan_stored_relation` cannot be called without the
    // permit. The permit is taken before the scan rather than after it so a
    // denied read reads nothing at all — PostgreSQL checks permissions at
    // executor start, and a check after the scan would let a leaky operator in
    // the `WHERE` observe rows the caller may not see.
    let permit = crate::privilege::ReadPermit::acquire(&read_ctx.privileges(), &t)?;
    let reach = if *only { Reach::OwnRows } else { Reach::Tree };
    let pruning_plan = pruned_columns.map(|columns| crate::plan_dist::DistributedScanPlan {
        projection: projected_scan_columns(&t, qualifier, columns),
        ..Default::default()
    });
    let raw = scan_stored_relation(
        read_ctx,
        &t,
        qualifier,
        reach,
        bounds,
        pruning_plan.as_ref().or(scan_plan),
        &permit,
    )?;
    crate::rls::apply_row_security(read_ctx, raw)
}

fn projected_scan_columns(
    table: &Table,
    qualifier: &str,
    pruned_columns: &[ColumnBinding],
) -> ProjectionPushdown {
    let scope = Scope::single(table, qualifier);
    ProjectionPushdown::Columns(
        scope
            .columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| pruned_columns.contains(column).then_some(index))
            .collect(),
    )
}
