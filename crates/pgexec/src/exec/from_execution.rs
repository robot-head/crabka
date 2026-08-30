use super::*;

pub(super) fn table_function_call_rows(
    read_ctx: &crate::subquery::SubCtx<'_>,
    call: &TableFuncCall,
) -> Result<TableFunctionRows, ExecError> {
    let call = crate::routine::normalize_table_function_call(read_ctx.catalog_kv, call)?;
    if crate::routine::plpgsql_table_function_schema(read_ctx.catalog_kv, &call)?.is_some() {
        crate::routine::eval_plpgsql_table_function(&call, read_ctx.eval_ctx, false)?.ok_or_else(
            || ExecError::Unsupported("table function requires a session executor".into()),
        )
    } else {
        crate::srf::function_call_rows_with_memory(
            &call,
            read_ctx.eval_ctx,
            &read_ctx.statement_memory,
        )
    }
}

pub(super) fn build_table_expr(
    read_ctx: &crate::subquery::SubCtx<'_>,
    te: &crabka_pgparser::ast::TableExpr,
    // SP40 Task 14: pushed-down offset bounds, `Some` only for a single foreign
    // base table. Applied verbatim to the foreign scan; `None` ⇒ full scan.
    bounds: Option<&ScanBounds>,
    scan_plan: Option<&crate::plan_dist::DistributedScanPlan>,
    // The statement's `WHERE`, so a join can pre-apply its single-relation
    // conjuncts to a null-preserved side. Only ever an optimization: the caller
    // applies the same predicate again to the relation this returns.
    filter: Option<&Expr>,
) -> Result<Relation, ExecError> {
    let ctx = read_ctx.eval_ctx;
    use crabka_pgparser::ast::TableExpr;
    match te {
        table @ TableExpr::Table { .. } => build_base_table(read_ctx, table, bounds, scan_plan),
        TableExpr::Join {
            left,
            right,
            kind,
            constraint,
        } => {
            if let Some(relation) =
                try_distributed_inner_equi_join(read_ctx, left, right, *kind, constraint)?
            {
                return Ok(relation);
            }
            // A join is never a single foreign table: each side scans in full and
            // the join predicate / residual WHERE filters locally.
            //
            // The `WHERE` only descends into the left side while every join it
            // passes through preserves that side. Under a `RIGHT`/`FULL` join
            // the left is nullable, and dropping its rows early would turn a
            // matched row into a NULL-padded one — which a predicate like
            // `a.x IS NULL` would then wrongly admit.
            let nested_filter = match kind {
                crabka_pgparser::ast::JoinKind::Inner
                | crabka_pgparser::ast::JoinKind::Cross
                | crabka_pgparser::ast::JoinKind::Left => filter,
                crabka_pgparser::ast::JoinKind::Right | crabka_pgparser::ast::JoinKind::Full => {
                    None
                }
            };
            let l = build_table_expr(read_ctx, left, None, None, nested_filter)?;
            // A lateral right side sees the left side's columns, so it is rebuilt
            // per left row instead of materialized once.
            append_from_item(
                read_ctx,
                l,
                right,
                *kind,
                constraint,
                filter,
                None,
                security_free_from_item(read_ctx, left),
                security_free_from_item(read_ctx, right),
            )
        }
        TableExpr::Derived {
            subquery,
            alias,
            columns,
            ..
        } => {
            let inner = crate::query::query_to_relation_with_ctes(read_ctx, subquery)?;
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
            let calls = functions
                .iter()
                .map(|call| table_function_call_rows(read_ctx, call))
                .collect::<Result<Vec<_>, _>>()?;
            crate::srf::rows_from_function_relation(
                &functions[0].name,
                calls,
                *with_ordinality,
                alias.as_deref(),
                column_aliases,
            )
        }
        // P2: a user-defined SQL function in FROM position is a parameterized
        // derived table — its body runs under the caller's own read context.
        // Built-in set-returning functions stay with the `srf` registry.
        TableExpr::Function {
            functions,
            with_ordinality,
            alias,
            column_aliases,
            ..
        } if crate::routine::expands_as_table(read_ctx.catalog_kv, functions) => {
            let functions = functions
                .iter()
                .map(|call| {
                    crate::routine::normalize_table_function_call(read_ctx.catalog_kv, call)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if let Some((columns, rows)) =
                crate::routine::eval_plpgsql_table_function(&functions[0], ctx, true)?
            {
                return crate::srf::user_function_relation(
                    &functions[0].name,
                    columns,
                    rows,
                    *with_ordinality,
                    alias.as_deref(),
                    column_aliases,
                    functions[0].column_defs.as_deref(),
                    Some(ctx),
                );
            }
            let (query, routine, names) =
                crate::routine::table_function_expansion(read_ctx.catalog_kv, &functions[0])?;
            let inner = crate::query::query_to_relation_with_ctes(read_ctx, &query)?;
            if let Some(defs) = functions[0].column_defs.as_deref() {
                crate::routine::validate_inlined_record_column_defs(
                    &routine,
                    &inner.scope.columns,
                    defs,
                )?;
                return crate::srf::user_function_relation(
                    &functions[0].name,
                    inner
                        .scope
                        .columns
                        .iter()
                        .map(|column| (column.name.clone(), column.ty))
                        .collect(),
                    crate::routine::table_function_result_rows(&routine, inner.rows),
                    *with_ordinality,
                    alias.as_deref(),
                    column_aliases,
                    Some(defs),
                    Some(ctx),
                );
            }
            let inner = if *with_ordinality {
                crate::srf::with_ordinality(inner)
            } else {
                inner
            };
            let columns = column_aliases.clone().or(Some(names));
            crate::values::requalify_derived(
                inner,
                alias.as_deref().unwrap_or(&functions[0].name),
                &columns,
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
                .map(|call| {
                    crate::routine::normalize_table_function_call(read_ctx.catalog_kv, call)
                })
                .collect::<Result<Vec<_>, _>>()?;
            crate::srf::from_item_with_memory(
                &functions,
                *with_ordinality,
                *rows_from,
                alias.as_deref(),
                column_aliases,
                ctx,
                &read_ctx.statement_memory,
            )
        }
        TableExpr::JsonTable(table) => crate::jsontable::from_item(table, ctx),
        TableExpr::XmlTable(table) => crate::xmltable::from_item(table, ctx),
    }
}

/// Apply a `TABLESAMPLE` clause to an already-materialized base-table relation.
///
/// `PostgreSQL` samples physical pages (`SYSTEM`) or individual rows
/// (`BERNOULLI`); crabka has no page layout to sample, so both methods draw rows
/// independently at the given probability. The percentage checks, the `42704`
/// for an unknown method, and the deterministic 0% / 100% ends match
/// `PostgreSQL` exactly; which rows a partial sample returns does not.
pub(crate) fn apply_tablesample(
    relation: Relation,
    sample: &crabka_pgparser::ast::TableSample,
    ctx: &crate::clock::EvalCtx,
) -> Result<Relation, ExecError> {
    if !matches!(sample.method.as_str(), "system" | "bernoulli") {
        return Err(ExecError::FunctionError {
            sqlstate: "42704",
            message: format!("tablesample method {} does not exist", sample.method),
        });
    }
    let percent = crate::eval::eval(&sample.percent, &Scope::empty(), &[], ctx)?;
    if percent.is_null() {
        return Err(ExecError::FunctionError {
            sqlstate: "2202H",
            message: "TABLESAMPLE parameter cannot be null".into(),
        });
    }
    let Datum::Float8(percent) =
        crabka_pgtypes::cast::cast(&percent, ColumnType::Float8, &ctx.time_zone)?
    else {
        return Err(ExecError::TypeMismatch(
            "TABLESAMPLE percentage must be numeric".into(),
        ));
    };
    if !(0.0..=100.0).contains(&percent) {
        return Err(ExecError::FunctionError {
            sqlstate: "2202H",
            message: "sample percentage must be between 0 and 100".into(),
        });
    }
    let seed = match &sample.repeatable {
        Some(expr) => {
            let value = crate::eval::eval(expr, &Scope::empty(), &[], ctx)?;
            if value.is_null() {
                // A null seed is invalid_tablesample_repeat, NOT the
                // invalid_tablesample_argument a null/out-of-range percentage is.
                return Err(ExecError::FunctionError {
                    sqlstate: "2202G",
                    message: "TABLESAMPLE REPEATABLE parameter cannot be null".into(),
                });
            }
            let Datum::Float8(seed) =
                crabka_pgtypes::cast::cast(&value, ColumnType::Float8, &ctx.time_zone)?
            else {
                return Err(ExecError::TypeMismatch(
                    "TABLESAMPLE seed must be numeric".into(),
                ));
            };
            seed.to_bits()
        }
        None => 0x9E37_79B9_7F4A_7C15,
    };
    let Relation { scope, rows } = relation;
    // A xorshift over (seed, row ordinal): repeatable across runs for a given
    // seed, and independent of how the rows were physically stored.
    let threshold = percent / 100.0;
    let sampled = rows
        .into_iter()
        .enumerate()
        .filter(|(index, _)| {
            let ordinal = u64::try_from(*index).unwrap_or(u64::MAX);
            let mut state = seed ^ ordinal.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            // The top 32 bits convert to f64 exactly, so the draw needs no
            // lossy cast.
            let bucket = u32::try_from(state >> 32).unwrap_or(u32::MAX);
            f64::from(bucket) / f64::from(u32::MAX) < threshold
        })
        .map(|(_, row)| row)
        .collect();
    Ok(Relation {
        scope,
        rows: sampled,
    })
}

fn try_distributed_inner_equi_join(
    read_ctx: &crate::subquery::SubCtx<'_>,
    left_expr: &crabka_pgparser::ast::TableExpr,
    right_expr: &crabka_pgparser::ast::TableExpr,
    kind: crabka_pgparser::ast::JoinKind,
    constraint: &crabka_pgparser::ast::JoinConstraint,
) -> Result<Option<Relation>, ExecError> {
    let catalog_kv = read_ctx.catalog_kv;
    let resolution = read_ctx.fctx.resolution;
    let gsnap = read_ctx.gsnap;
    let snapshot = read_ctx.snapshot;
    let own = read_ctx.own;
    let ctes = read_ctx.ctes;
    let range_scanner = read_ctx.range_scanner;
    use crabka_pgparser::ast::{BinaryOp, Expr, JoinConstraint, JoinKind, TableExpr};

    if kind != JoinKind::Inner {
        return Ok(None);
    }
    let (
        TableExpr::Table {
            name: left_name,
            alias: left_alias,
            columns: None,
            sample: None,
            ..
        },
        TableExpr::Table {
            name: right_name,
            alias: right_alias,
            columns: None,
            sample: None,
            ..
        },
    ) = (left_expr, right_expr)
    else {
        return Ok(None);
    };
    if (left_name.schema.is_none() && ctes.lookup(&left_name.name).is_some())
        || (right_name.schema.is_none() && ctes.lookup(&right_name.name).is_some())
    {
        return Ok(None);
    }
    let left_name = &resolve_relation(
        catalog_kv,
        resolution,
        left_name,
        SchemaDisposition::Reference,
    )?;
    let right_name = &resolve_relation(
        catalog_kv,
        resolution,
        right_name,
        SchemaDisposition::Reference,
    )?;
    let JoinConstraint::On(Expr::Binary {
        op: BinaryOp::Eq,
        left: key_left,
        right: key_right,
    }) = constraint
    else {
        return Ok(None);
    };
    let table =
        |name: &crabka_pgcatalog::RelationName| match crabka_pgcatalog::get_table(catalog_kv, name)
        {
            Ok(table) => Ok(Some(table)),
            Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => Ok(None),
            Err(error) => Err(ExecError::from(error)),
        };
    let Some(left_table) = table(left_name)? else {
        return Ok(None);
    };
    let Some(right_table) = table(right_name)? else {
        return Ok(None);
    };
    if !left_table.sharded
        || !right_table.sharded
        || left_table.foreign.is_some()
        || right_table.foreign.is_some()
        // The scanner joins remotely on the rows as they sit in storage, where
        // a virtual generated column is a NULL placeholder. Such a relation
        // falls back to the ordinary scan, which materializes it first.
        || has_virtual_generated(&left_table)
        || has_virtual_generated(&right_table)
    {
        return Ok(None);
    }
    // The scanner joins both relations remotely and returns joined tuples, so
    // neither side's rows ever pass a row-security qual. Both sides must be
    // proven unrestricted, and the proofs shadow the raw tables so the rest of
    // this function cannot reach around them.
    let rls = read_ctx.rls();
    let privileges = read_ctx.privileges();
    let (Some(left_table), Some(right_table)) = (
        crate::rls::UnrestrictedTable::read(&privileges, &rls, &left_table)?,
        crate::rls::UnrestrictedTable::read(&privileges, &rls, &right_table)?,
    ) else {
        return Ok(None);
    };
    let (left_table, right_table) = (left_table.get(), right_table.get());
    let left_qualifier = left_alias.as_deref().unwrap_or(&left_table.name.name);
    let right_qualifier = right_alias.as_deref().unwrap_or(&right_table.name.name);
    fn qualified_key(expr: &Expr) -> Option<(&str, &str)> {
        let Expr::Column {
            table: Some(table),
            name,
        } = expr
        else {
            return None;
        };
        Some((table.as_str(), name.as_str()))
    }
    let (Some((first_table, first_column)), Some((second_table, second_column))) =
        (qualified_key(key_left), qualified_key(key_right))
    else {
        return Ok(None);
    };
    let (left_column, right_column) =
        if first_table == left_qualifier && second_table == right_qualifier {
            (first_column, second_column)
        } else if first_table == right_qualifier && second_table == left_qualifier {
            (second_column, first_column)
        } else {
            return Ok(None);
        };
    let Some(left_key) = left_table
        .columns
        .iter()
        .position(|column| column.name == left_column)
    else {
        return Ok(None);
    };
    let Some(right_key) = right_table
        .columns
        .iter()
        .position(|column| column.name == right_column)
    else {
        return Ok(None);
    };
    let planned =
        range_scanner.join_strategy_for_keys(left_table, right_table, &[left_key], &[right_key]);
    let strategy = match planned {
        crate::plan_dist::JoinStrategy::Broadcast { small_table_id }
            if small_table_id == u64::from(left_table.id) =>
        {
            JoinExecutionStrategy::BroadcastLeft
        }
        crate::plan_dist::JoinStrategy::Broadcast { small_table_id }
            if small_table_id == u64::from(right_table.id) =>
        {
            JoinExecutionStrategy::BroadcastRight
        }
        crate::plan_dist::JoinStrategy::Broadcast { .. } => JoinExecutionStrategy::Gather,
        crate::plan_dist::JoinStrategy::CoPartitioned
            if hash_sharding_matches_join_keys(
                left_table,
                right_table,
                left_column,
                right_column,
            ) =>
        {
            JoinExecutionStrategy::CoPartitioned
        }
        crate::plan_dist::JoinStrategy::CoPartitioned => JoinExecutionStrategy::Gather,
        crate::plan_dist::JoinStrategy::Gather => JoinExecutionStrategy::Gather,
    };
    // Folded onto the enclosing read span rather than carried on one of this
    // join's own: the strategy is a property of how the statement ran, and a
    // span that declares no such field ignores this.
    tracing::Span::current().record("pg.join_strategy", strategy.as_str());
    let join_snapshot = |source: &crabka_pgmvcc::visibility::Snapshot| JoinSnapshot {
        xmin: source.xmin,
        xmax: source.xmax,
        xip: source.xip.clone(),
    };
    let request = JoinRangeRequest {
        local_snapshot: join_snapshot(snapshot),
        global_snapshot: join_snapshot(gsnap),
        read_ts: 1,
        own_xid: own,
        own_start_ts: None,
        kind: ScannerJoinKind::Inner,
        left_keys: vec![left_key],
        right_keys: vec![right_key],
        strategy,
        left: JoinTableInterval {
            table_id: u64::from(left_table.id),
            table_name: left_table.name.to_string(),
            interval: RowInterval::ALL,
        },
        right: JoinTableInterval {
            table_id: u64::from(right_table.id),
            table_name: right_table.name.to_string(),
            interval: RowInterval::ALL,
        },
        broadcast_rows: matches!(
            strategy,
            JoinExecutionStrategy::BroadcastLeft | JoinExecutionStrategy::BroadcastRight
        )
        .then(Vec::new),
        left_filter: PredicatePushdown::FullScan,
        right_filter: PredicatePushdown::FullScan,
        projection: Vec::new(),
    };
    let result = match range_scanner.join(request) {
        Ok(result) => result,
        Err(ExecError::Unsupported(_)) => return Ok(None),
        Err(error) => return Err(error),
    };
    result
        .validate()
        .map_err(|error| ExecError::Unsupported(error.to_string()))?;
    let rows = result
        .rows
        .into_iter()
        .map(|JoinRow { tuple }| {
            crabka_pgmvcc::version::decode_tuple(&tuple)
                .map(|(_, _, row)| row)
                .map_err(ExecError::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    // The join result is the left table's columns followed by the right's, so
    // the right table's `regclass` columns sit past the left's width.
    let mut rows = rows;
    let mut regclass_columns = regclass_column_indexes(left_table, 0);
    regclass_columns.extend(regclass_column_indexes(
        right_table,
        left_table.columns.len(),
    ));
    resolve_regclass_at(
        read_ctx.catalog_kv,
        read_ctx.eval_ctx.resolution(),
        &regclass_columns,
        &mut rows,
    )?;
    let mut scope = relation_scope(read_ctx.catalog_kv, left_table, left_qualifier)?;
    scope.extend(&relation_scope(
        read_ctx.catalog_kv,
        right_table,
        right_qualifier,
    )?);
    Ok(Some(Relation { scope, rows }))
}

fn hash_sharding_matches_join_keys(
    left: &Table,
    right: &Table,
    left_column: &str,
    right_column: &str,
) -> bool {
    use crabka_pgcatalog::ShardingStrategy;

    let (Some(ShardingStrategy::Hash(left_hash)), Some(ShardingStrategy::Hash(right_hash))) =
        (&left.sharding, &right.sharding)
    else {
        return false;
    };
    left_hash.columns.as_slice() == [left_column] && right_hash.columns.as_slice() == [right_column]
}

/// An equality or full-text probe over a local index, for a relation proven not
/// to be under row security.
///
/// It takes an [`crate::rls::UnrestrictedTable`] rather than a `&Table` because
/// an index probe returns the rows the index points at, which is not the same
/// set as the rows a policy admits. A relation with policies has no
/// `UnrestrictedTable` to offer and falls through to the ordinary gated scan.
pub(super) fn try_scan_with_local_index(
    read_ctx: &crate::subquery::SubCtx<'_>,
    unrestricted: crate::rls::UnrestrictedTable<'_>,
    plan: &crate::plan_dist::DistributedScanPlan,
) -> Result<Option<Vec<ScannedRow>>, ExecError> {
    let table = unrestricted.get();
    if table.sharded || plan.partial_aggregate.is_some() {
        return Ok(None);
    }
    if let Some(predicate) = &plan.text_search
        && let Some(index) = choose_local_gin_index(read_ctx.catalog_kv, table, predicate.column)?
        && let Some(rows) = lookup_local_gin(
            &MvccReadContext {
                kv: read_ctx.kv,
                global: read_ctx.global,
                global_snapshot: read_ctx.gsnap,
                snapshot: read_ctx.snapshot,
                own: read_ctx.own,
                command_id: read_ctx.command_id,
            },
            table,
            &index,
            &predicate.query,
        )?
    {
        return Ok(Some(rows));
    }
    let Some((index, value)) =
        choose_local_index_equality(read_ctx.catalog_kv, table, &plan.predicate)?
    else {
        return Ok(None);
    };
    let rows = lookup_local_index_equal(
        &MvccReadContext {
            kv: read_ctx.kv,
            global: read_ctx.global,
            global_snapshot: read_ctx.gsnap,
            snapshot: read_ctx.snapshot,
            own: read_ctx.own,
            command_id: read_ctx.command_id,
        },
        table,
        &index,
        &[value],
    )?;
    crate::scanner::apply_executable_scan_pushdown(
        rows,
        &plan.predicate,
        &plan.projection,
        None,
        plan.top_k.as_ref(),
    )
    .map(Some)
}

fn choose_local_gin_index(
    catalog_kv: &dyn Kv,
    table: &Table,
    column: usize,
) -> Result<Option<crabka_pgcatalog::Index>, ExecError> {
    Ok(
        crabka_pgcatalog::list_table_indexes(catalog_kv, &table.name)?
            .into_iter()
            .find(|index| {
                index.placement == crabka_pgcatalog::IndexPlacement::Local
                    && index.method == crabka_pgcatalog::IndexMethod::Gin
                    && index.predicate.is_none()
                    && index.columns.len() == 1
                    && table.column_index(&index.columns[0]) == Some(column)
            }),
    )
}

pub(super) fn choose_local_index_equality(
    catalog_kv: &dyn Kv,
    table: &Table,
    predicate: &PredicatePushdown,
) -> Result<Option<(crabka_pgcatalog::Index, Datum)>, ExecError> {
    let PredicatePushdown::Conjunctive(predicates) = predicate else {
        return Ok(None);
    };
    let indexes = crabka_pgcatalog::list_table_indexes(catalog_kv, &table.name)?;
    for predicate in predicates
        .iter()
        .filter(|predicate| predicate.op == crate::PredicateOp::Eq && !predicate.value.is_null())
    {
        let Some(index) = indexes.iter().find(|index| {
            index.placement == crabka_pgcatalog::IndexPlacement::Local
                && matches!(
                    index.method,
                    crabka_pgcatalog::IndexMethod::Btree | crabka_pgcatalog::IndexMethod::Hash
                )
                // A partial index cannot prove the query's row set without
                // predicate implication, which the planner does not have yet.
                && index.predicate.is_none()
                && index.columns.len() == 1
                && table.column_index(&index.columns[0]) == Some(predicate.column)
        }) else {
            continue;
        };
        return Ok(Some((index.clone(), predicate.value.clone())));
    }
    Ok(None)
}

pub(super) fn should_retry_without_scan_pushdown(
    error: &ExecError,
    plan: &crate::plan_dist::DistributedScanPlan,
) -> bool {
    if plan.partial_aggregate.is_some() || plan.top_k.is_some() {
        return false;
    }
    if *plan == crate::plan_dist::DistributedScanPlan::default() {
        return false;
    }
    let message = error.clone().into_pg().message;
    message.contains("pushdown") || message.contains("full scans")
}
