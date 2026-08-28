use super::*;

pub(crate) fn reject_nested_relation_locking(s: &SelectStmt) -> Result<(), ExecError> {
    if s.locking.is_some() {
        return Err(ExecError::Unsupported(
            "FOR UPDATE/SHARE is not supported in CTEs or derived tables".into(),
        ));
    }
    Ok(())
}

/// `PostgreSQL`'s `CheckSelectLocking`: a locking read may not be combined with
/// any clause that turns rows into aggregates or a computed set, because there
/// would be no base-table row left to lock.
fn check_select_locking(
    s: &SelectStmt,
    strength: crabka_pgparser::ast::RowLockStrength,
) -> Result<(), ExecError> {
    let refuse = |what: &str| {
        Err(ExecError::Unsupported(format!(
            "{} is not allowed with {what}",
            strength.as_sql()
        )))
    };
    if s.distinct.dedups() {
        return refuse("DISTINCT clause");
    }
    if !s.group_by.is_empty() {
        return refuse("GROUP BY clause");
    }
    if s.having.is_some() {
        return refuse("HAVING clause");
    }
    if crate::agg::is_aggregate_query(s) {
        return refuse("aggregate functions");
    }
    // A window result is not a row of any table, so there is nothing for the
    // lock to name. PostgreSQL checks this after the aggregate test, so a
    // grouped window query still reports its GROUP BY clause first.
    if crate::window::has_window_calls(s) {
        return refuse("window functions");
    }
    Ok(())
}

/// The row-lock mode a strength maps onto.
///
/// Divergence: crabka's lock table has two modes, so `FOR NO KEY UPDATE` folds
/// onto the exclusive mode and `FOR KEY SHARE` onto the shared one. Every pair
/// `PostgreSQL` lets proceed concurrently still does, except that `FOR KEY
/// SHARE` blocks against `FOR NO KEY UPDATE` here where `PostgreSQL` lets both
/// through.
fn lock_mode_for(strength: crabka_pgparser::ast::RowLockStrength) -> crate::lockmgr::LockMode {
    use crabka_pgparser::ast::RowLockStrength;
    match strength {
        RowLockStrength::ForUpdate | RowLockStrength::ForNoKeyUpdate => {
            crate::lockmgr::LockMode::Exclusive
        }
        RowLockStrength::ForShare | RowLockStrength::ForKeyShare => {
            crate::lockmgr::LockMode::Shared
        }
    }
}

pub(crate) fn execute_read(
    read_ctx: &crate::subquery::SubCtx<'_>,
    stmt: &Statement,
) -> Result<QueryResult, ExecError> {
    let span = exec_read_span(read_ctx);
    let _guard = span.enter();
    crate::session::check_query_canceled()?;
    let Statement::Query(q) = stmt else {
        return Err(ExecError::Unsupported("not a query statement".into()));
    };
    let rel = crate::query::query_to_relation(read_ctx, q)?;
    crate::session::check_query_canceled()?;
    let result = crate::query::relation_to_rows_result(rel, read_ctx.eval_ctx);
    if let QueryResult::Rows { rows, .. } = &result {
        span.record("pg.rows_out", crate::telemetry::integer(rows.len()));
    }
    Ok(result)
}

/// Build the span covering a read statement's execution inside the executor.
///
/// The scans, joins and locks the read performs attach to this, so it is the
/// level at which "the query itself was slow" separates from "getting a read
/// timestamp was slow". `pg.join_strategy` stays empty unless the planner
/// actually chose a distributed join. See [`try_distributed_inner_equi_join`].
fn exec_read_span(read_ctx: &crate::subquery::SubCtx<'_>) -> tracing::Span {
    tracing::debug_span!(
        target: crate::telemetry::EXEC_TARGET,
        "gres.exec_read",
        otel.kind = "internal",
        pg.rows_out = tracing::field::Empty,
        pg.blocking_query_memory_bytes =
            crate::telemetry::integer(read_ctx.blocking_query_memory.bytes_usize()),
        pg.join_strategy = tracing::field::Empty,
    )
}

fn locking_read_body_relation(
    read_ctx: &crate::subquery::SubCtx<'_>,
    s: &SelectStmt,
) -> Result<Relation, ExecError> {
    let mut unlocked = s.clone();
    unlocked.locking = None;
    select_to_relation_with_ctes(read_ctx, &unlocked)
}

/// Locking SELECT (FOR UPDATE / FOR SHARE). Takes a row lock on each visible
/// row before rechecking it via EvalPlanQual (same semantics as UPDATE/DELETE).
/// The snapshot and xid must already be established by the caller.
pub(crate) async fn execute_read_locking(
    read_ctx: &crate::subquery::SubCtx<'_>,
    procarray: &crate::procarray::ProcArray,
    lockmgr: &crate::lockmgr::RowLockManager,
    lock_owner: crate::lockmgr::LockOwner,
    repeatable_read: bool,
    lock_wait_cap: Option<std::time::Duration>,
    s: &SelectStmt,
) -> Result<QueryResult, ExecError> {
    let relation = execute_read_locking_relation(
        read_ctx,
        procarray,
        lockmgr,
        lock_owner,
        repeatable_read,
        lock_wait_cap,
        s,
    )
    .await?;
    Ok(crate::query::relation_to_rows_result(
        relation,
        read_ctx.eval_ctx,
    ))
}

pub(super) async fn execute_read_locking_relation(
    read_ctx: &crate::subquery::SubCtx<'_>,
    procarray: &crate::procarray::ProcArray,
    lockmgr: &crate::lockmgr::RowLockManager,
    lock_owner: crate::lockmgr::LockOwner,
    repeatable_read: bool,
    lock_wait_cap: Option<std::time::Duration>,
    s: &SelectStmt,
) -> Result<Relation, ExecError> {
    let original = s;
    let catalog_kv = read_ctx.catalog_kv;
    let resolution = read_ctx.fctx.resolution;
    let kv = read_ctx.kv;
    let global = read_ctx.global;
    let gsnap = read_ctx.gsnap;
    let snapshot = read_ctx.snapshot;
    let xid = read_ctx.own.ok_or_else(|| {
        ExecError::Unsupported("locking SELECT requires a transaction xid".into())
    })?;
    let ctx = read_ctx.eval_ctx;
    let locking = s
        .locking
        .clone()
        .ok_or_else(|| ExecError::Unsupported("locking SELECT has no locking clause".into()))?;
    // Ahead of subquery resolution, which evaluates the statement's expressions:
    // PostgreSQL refuses these shapes during parse analysis, so a query it will
    // not run must not be part-run to report it.
    check_select_locking(s, locking.strength)?;
    let mode = lock_mode_for(locking.strength);
    // FOR UPDATE/SHARE names base-table rows. A FROM with none — a FROM-less
    // SELECT, a set-returning function, a derived table — has nothing to lock,
    // and PostgreSQL simply runs the query.
    // `OF <rel>` restricts locking to the relations it names; one that is not in
    // the FROM clause at all is PostgreSQL's 42P01.
    let mut qualifiers = Vec::new();
    collect_qualifiers(&s.from, &mut qualifiers);
    for named in &locking.of {
        if !qualifiers
            .iter()
            .any(|qualifier| qualifier.eq_ignore_ascii_case(named))
        {
            return Err(ExecError::MissingFromEntry(named.clone()));
        }
    }
    let (t, qualifier, only) = match s.from.as_slice() {
        [
            crabka_pgparser::ast::TableExpr::Table {
                name,
                only,
                alias,
                columns: None,
                sample: None,
            },
        ] if name.schema.is_none() && read_ctx.ctes.lookup(&name.name).is_none() => {
            let table = crabka_pgcatalog::get_table(
                catalog_kv,
                &resolve_relation(catalog_kv, resolution, name, SchemaDisposition::Reference)?,
            )?;
            let qualifier = alias.clone().unwrap_or_else(|| table.name.name.clone());
            if locking.of.is_empty()
                || locking
                    .of
                    .iter()
                    .any(|named| named.eq_ignore_ascii_case(&qualifier))
            {
                (table, qualifier, *only)
            } else {
                // The clause names other relations only, so this one is read
                // without locking.
                return locking_read_body_relation(read_ctx, original);
            }
        }
        // A FROM with nothing lockable — no FROM at all, a set-returning
        // function, a derived table — just runs the query, as in PostgreSQL.
        [] => return locking_read_body_relation(read_ctx, original),
        [item] if !matches!(item, crabka_pgparser::ast::TableExpr::Table { .. }) => {
            return locking_read_body_relation(read_ctx, original);
        }
        _ => {
            return Err(ExecError::Unsupported(format!(
                "{} with a join is not supported",
                locking.strength.as_sql()
            )));
        }
    };
    // The relations this read covers: the named one, plus every inheritance
    // descendant unless the statement said `ONLY`. `SELECT * FROM parent FOR
    // UPDATE` locks and returns a child's rows in `PostgreSQL`, and this path
    // used to scan the named relation alone — a silent wrong answer of exactly
    // the kind [`inherited_scan`] exists to prevent on the ordinary read side.
    let mut relations = vec![t.name.clone()];
    if !only {
        relations.extend(crate::inheritance::descendants(catalog_kv, &t.name)?);
    }
    // Under `ONLY` a partitioned parent contributes nothing, so there is no lock
    // to spread over its leaves and nothing to refuse: the scan below reads its
    // own empty row space and returns no rows, as `PostgreSQL` does. The refusal
    // is for the reads that really would have to reach the leaves.
    for relation in &relations {
        if !(only && *relation == t.name) && crate::partition::is_partitioned(catalog_kv, relation)?
        {
            return Err(ExecError::Unsupported(format!(
                "{} on a partitioned table is not supported: the lock would have to be taken on \
                 every partition that contributes rows",
                locking.strength.as_sql()
            )));
        }
    }
    // Every other stored-relation read takes a permit before it reads a byte
    // (see `build_base_table`); this one took none, so a role holding no
    // `SELECT` at all could read a relation by asking to lock it — and a role
    // holding `SELECT` alone could reserve its rows against everyone else. The
    // whole tree is read under the named relation's permit, which is
    // `PostgreSQL`'s rule for a tree read — see
    // [`crate::privilege::ReadPermit::inherited`].
    let _permit = crate::privilege::ReadPermit::acquire_for_row_lock(&read_ctx.privileges(), &t)?;
    // Resolve only after proving this is a lockable base-table shape. Fallbacks
    // execute through the ordinary read path, which owns their single subquery
    // resolution; doing it before the shape decision would run volatile
    // uncorrelated subqueries twice.
    let PlannedSubqueries {
        select: resolved,
        correlated_filter: correlated,
        initplans,
        scalar_lookups,
        row_exprs,
    } = resolve_select_subqueries(read_ctx, s)?;
    let s = &resolved;
    let mut scope = Scope::single(&t, &qualifier);
    let mut binder = correlated.then(|| {
        LateralBinder::new(catalog_kv, read_ctx.fctx.resolution, read_ctx.ctes)
            .with_initplans(initplans)
            .with_scalar_lookups(scalar_lookups)
    });

    // The uncorrelated filter is the same expression for every scanned row.
    let bound_filter = if binder.is_none() {
        crate::bind::bind_optional(s.filter.as_ref(), &scope)?
    } else {
        None
    };
    let bound_filter = bound_filter.as_ref().map(crate::bind::BoundExpr::expr);
    // The named relation's policies govern every row of the tree, exactly as
    // they do for `inherited_scan`, and they are asked before the lock rather
    // than after the scan — see [`crate::rls::LockingReadGate`].
    let gate = crate::rls::LockingReadGate::compile(read_ctx, &t)?;
    let bound_gate = crate::bind::bind_optional(gate.qual(), &scope)?;
    let bound_gate = bound_gate.as_ref().map(crate::bind::BoundExpr::expr);

    // Scan visible rows, then lock and EvalPlanQual-recheck each one.
    let mut kept: Vec<Vec<Datum>> = Vec::new();
    for relation in &relations {
        // The named relation is already open; only a descendant costs a second
        // catalog read, and only a descendant needs permuting, since the
        // parent's mapping to itself is the identity.
        let scanned = if *relation == t.name {
            None
        } else {
            let child = crabka_pgcatalog::get_table(catalog_kv, relation)?;
            let ordinals = column_mapping(&t, &child)?;
            Some((child, ordinals))
        };
        let (from, ordinals) = match &scanned {
            Some((child, ordinals)) => (child, Some(ordinals.as_slice())),
            None => (&t, None),
        };
        for ScannedRow {
            rowid,
            xmin: scanned_xmin,
            row: mut scanned_row,
            ..
        } in read_ctx.range_scanner.scan(ScanRequest {
            local: kv,
            global,
            global_snapshot: gsnap,
            snapshot,
            own_xid: Some(xid),
            command_id: read_ctx.command_id,
            read_ts: None,
            own_start_ts: None,
            table: from,
            interval: RowInterval::ALL,
            predicate: PredicatePushdown::FullScan,
            projection: crate::ProjectionPushdown::All,
            partial_aggregate: None,
            top_k: None,
        })? {
            // A generated column is expanded in the relation the row came out
            // of, before the row is reshaped into the one the query named.
            expand_virtual_generated_row(
                from,
                &mut scanned_row,
                ctx,
                crate::scope::GeneratedReads::every(),
            )?;
            let scanned_row = reshape_row(scanned_row, ordinals);
            // 0. Row security first: a row a policy hides must not be locked,
            //    because a lock is observable through NOWAIT, SKIP LOCKED and
            //    an ordinary waiter.
            if !row_matches(bound_gate, &scope, &scanned_row, ctx)? {
                continue;
            }
            // 1. Filter on the snapshot-visible row before locking — only lock
            //    rows that match the WHERE clause (a FOR UPDATE/SHARE with no
            //    WHERE still locks all rows because row_matches(None, ..)
            //    returns true).
            let matches = if let Some(binder) = &mut binder {
                row_matches_correlated(read_ctx, s.filter.as_ref(), &scope, &scanned_row, binder)?
            } else {
                row_matches(bound_filter, &scope, &scanned_row, ctx)?
            };
            if !matches {
                continue;
            }

            // 2. Lock only matching candidates (40P01 on deadlock or expired
            //    cap). NOWAIT and SKIP LOCKED both take the non-blocking path
            //    and differ only in what a conflict means: an error, or a row
            //    that is skipped. The lock names the relation the row lives in,
            //    never the parent it is reported under.
            match locking.wait {
                crabka_pgparser::ast::LockWaitPolicy::Wait => {
                    lockmgr
                        .acquire_as(from.id, rowid, mode, lock_owner, lock_wait_cap)
                        .await
                        .map_err(lock_acquire_error)?;
                }
                policy => {
                    if let crate::lockmgr::Acquire::Conflict(_) =
                        lockmgr.try_acquire_as(from.id, rowid, mode, lock_owner)
                    {
                        if policy == crabka_pgparser::ast::LockWaitPolicy::SkipLocked {
                            continue;
                        }
                        return Err(ExecError::FunctionError {
                            sqlstate: "55P03",
                            message: format!(
                                "could not obtain lock on row in relation \"{}\"",
                                from.name
                            ),
                        });
                    }
                }
            }

            // 3. EvalPlanQual: re-read the row under the lock (40001 under RR
            //    if changed since our snapshot; RC re-finds the latest live
            //    version).
            let Some((_cur_rowid, _cur_key_xid, cur_xmin, _cur_cmin, _cur_cmax, cur_row)) =
                eval_plan_qual(
                    &MutationContext {
                        kv,
                        global,
                        procarray,
                        snapshot,
                        xid,
                        command_id: read_ctx.command_id,
                        repeatable_read,
                        eval_ctx: ctx,
                    },
                    from,
                    rowid,
                    crate::scope::GeneratedReads::every(),
                )?
            else {
                continue; // deleted by a concurrent committed txn — skip
            };
            let cur_row = reshape_row(cur_row, ordinals);

            // 4. Re-apply the filters only when EvalPlanQual found a newer
            //    tuple version. Re-running a volatile predicate against the
            //    same version would evaluate it twice even without a concurrent
            //    update.
            if cur_xmin != scanned_xmin {
                if !row_matches(bound_gate, &scope, &cur_row, ctx)? {
                    continue;
                }
                let matches = if let Some(binder) = &mut binder {
                    row_matches_correlated(read_ctx, s.filter.as_ref(), &scope, &cur_row, binder)?
                } else {
                    row_matches(bound_filter, &scope, &cur_row, ctx)?
                };
                if !matches {
                    continue; // no longer matches
                }
            }
            kept.push(cur_row);
        }
    }

    resolve_scanned_regclass(
        read_ctx.catalog_kv,
        read_ctx.eval_ctx.resolution(),
        &t,
        &mut kept,
    )?;
    if let Some(row_exprs) = row_exprs {
        materialize_correlated_row_exprs(read_ctx, row_exprs, &mut scope, &mut kept)?;
    }
    project_order_limit_relation(s, &scope, kept, ctx, &read_ctx.statement_memory)
}
