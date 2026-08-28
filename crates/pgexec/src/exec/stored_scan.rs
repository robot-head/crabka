use super::*;

/// Read a partitioned parent as the append of its leaf partitions.
///
/// Each leaf is scanned through the ordinary base-table path and its rows are
/// permuted into the parent's column order. A leaf attached by `ATTACH
/// PARTITION` may declare the same columns in a different order, and
/// `PostgreSQL` maps them by name.
fn partitioned_scan(
    read_ctx: &crate::subquery::SubCtx<'_>,
    parent: &Table,
    qualifier: &str,
    permit: &crate::privilege::ReadPermit,
) -> Result<crate::rls::RawScan, ExecError> {
    let tree_system = crate::scope::SystemColumns::of(read_ctx.refs, parent);
    let mut tree = crate::rls::RawScan::tree_of(parent, qualifier, tree_system);
    for leaf in crate::partition::leaves_of(read_ctx.catalog_kv, &parent.name)? {
        let leaf_table = crabka_pgcatalog::get_table(read_ctx.catalog_kv, &leaf)?;
        let ordinals = tree_ordinals(parent, &leaf_table, tree_system, read_ctx.refs)?;
        // Straight to the scan, not back through `build_table_expr`: re-entering
        // there would run each leaf through the row-security gate under the
        // LEAF's policies, and then run the whole append through it again under
        // the parent's. PostgreSQL applies the parent's policies to the tree and
        // none of the children's, so the leaf's rows must arrive here ungated.
        // Its privileges likewise: the leaf is read under the parent's permit,
        // because reading a partitioned relation is not a read of each leaf.
        let leaf_scan = scan_stored_relation(
            read_ctx,
            &leaf_table,
            &leaf.name,
            Reach::Storage,
            None,
            None,
            &crate::privilege::ReadPermit::inherited(permit),
        )?;
        tree.absorb(leaf_scan, &ordinals);
    }
    Ok(tree)
}

/// Read a table and all inheritance descendants as rows shaped like the parent.
fn inherited_scan(
    read_ctx: &crate::subquery::SubCtx<'_>,
    parent: &Table,
    qualifier: &str,
    permit: &crate::privilege::ReadPermit,
) -> Result<crate::rls::RawScan, ExecError> {
    let mut relations = vec![parent.name.clone()];
    relations.extend(crate::inheritance::descendants(
        read_ctx.catalog_kv,
        &parent.name,
    )?);
    let tree_system = crate::scope::SystemColumns::of(read_ctx.refs, parent);
    let mut tree = crate::rls::RawScan::tree_of(parent, qualifier, tree_system);
    for relation_name in relations {
        let table = crabka_pgcatalog::get_table(read_ctx.catalog_kv, &relation_name)?;
        let ordinals = tree_ordinals(parent, &table, tree_system, read_ctx.refs)?;
        // See `partitioned_scan`: the child's rows are governed by the parent's
        // policies and read under the parent's permit, so they must not pass
        // through either gate on their own.
        let child = scan_stored_relation(
            read_ctx,
            &table,
            &relation_name.name,
            Reach::Storage,
            None,
            None,
            &crate::privilege::ReadPermit::inherited(permit),
        )?;
        tree.absorb(child, &ordinals);
    }
    Ok(tree)
}

/// Every way the executor reads the rows of one stored relation, behind one
/// signature.
///
/// The `TableExpr::Table` arm used to have five separate `return Ok(Relation)`
/// exits for a stored relation — the inheritance scan, the partition scan, the
/// foreign scan, the local-index fast path and the ordinary MVCC scan — and a
/// sixth would have been added the same way. They all funnel through here now,
/// and here is the only place that can produce a [`crate::rls::RawScan`], whose
/// only exit in turn is [`crate::rls::apply_row_security`]. A new scan path
/// either goes through the gate or does not compile.
///
/// Wrapping `build_table_expr` instead would have been wrong: `inherited_scan`
/// and `partitioned_scan` read each child relation, so a wrapper would apply the
/// gate once per child under that child's policies and then again over the
/// append under the parent's.
///
/// The [`crate::privilege::ReadPermit`] is the same idea one layer up: it is
/// unforgeable outside `privilege`, so this function cannot be reached without
/// someone having asked whether the session may read the relation at all. The
/// tree scans hand their parent's permit down rather than taking one of their
/// own, because `PostgreSQL` checks the ACL of the relation the query named and
/// none of its descendants'.
pub(super) fn scan_stored_relation(
    read_ctx: &crate::subquery::SubCtx<'_>,
    t: &Table,
    qualifier: &str,
    reach: Reach,
    bounds: Option<&ScanBounds>,
    scan_plan: Option<&crate::plan_dist::DistributedScanPlan>,
    permit: &crate::privilege::ReadPermit,
) -> Result<crate::rls::RawScan, ExecError> {
    let catalog_kv = read_ctx.catalog_kv;
    if reach == Reach::Tree && !crate::inheritance::children_of(catalog_kv, &t.name)?.is_empty() {
        return inherited_scan(read_ctx, t, qualifier, permit);
    }
    // A partitioned parent owns no rows: reading it is an append over its
    // leaves. Doing this before every other scan path is what keeps a
    // partitioned relation from silently answering empty.
    //
    // Under `ONLY` there is nothing to append, and the fall-through below reads
    // the parent's own — empty — row space, which is the answer `PostgreSQL`
    // gives. This test used to run whatever the statement said, so `SELECT *
    // FROM ONLY parted` returned the leaves' rows and `DELETE FROM ONLY parted`
    // destroyed them.
    if reach.spans_partitions() && crate::partition::is_partitioned(catalog_kv, &t.name)? {
        return partitioned_scan(read_ctx, t, qualifier, permit);
    }
    let mut scope = relation_scope(catalog_kv, t, qualifier)?;
    // The hidden columns this scan carries and the values they take, decided
    // once for the scope and for every row of it together. The relation's oid
    // is resolved here, before the rows: it is a catalog fact about the
    // relation, not about any row of it. `ctid` is the opposite — a fact about
    // the row — so it is read off each row where that row is decoded, in
    // `scanned_rows`.
    let stamp = crate::scope::SystemColumns::of(read_ctx.refs, t).stamp(t.id)?;
    stamp.extend_scope(&mut scope, qualifier);
    let reads = read_generated_reads(t, read_ctx.refs, qualifier);
    // SP40: a foreign table reads through the registered scanner, not the local
    // MVCC version store. `build_from` materializes BEFORE WHERE, so this scan
    // runs even for `WHERE false` — there is no skip path.
    if let Some(meta) = &t.foreign {
        let scanner = read_ctx.fctx.scanner.ok_or_else(|| {
            ExecError::Unsupported("foreign tables require the `kafka` feature".into())
        })?;
        let server = crabka_pgcatalog::get_server(catalog_kv, &meta.server)?;
        // A per-user mapping is optional: fall back to no credentials when the
        // current user has none registered for this server.
        let mapping = crabka_pgcatalog::get_user_mapping(
            catalog_kv,
            read_ctx.fctx.current_user,
            &meta.server,
        )
        .ok();
        // SP40 Task 14: pass the pushed-down slice when present (single foreign
        // table). The residual WHERE still re-filters locally, so results are
        // identical whether or not the scan honors `bounds`.
        let default_bounds = ScanBounds::default();
        let scan_bounds = bounds.unwrap_or(&default_bounds);
        let mut rows =
            scanner.scan(t, &server, mapping.as_ref(), scan_bounds, read_ctx.eval_ctx)?;
        resolve_scanned_regclass(catalog_kv, read_ctx.eval_ctx.resolution(), t, &mut rows)?;
        expand_virtual_generated(t, &mut rows, read_ctx.eval_ctx, reads)?;
        return Ok(crate::rls::RawScan::of_relation(
            t,
            scope,
            stamped(rows, &stamp),
        ));
    }
    let default_scan_plan = crate::plan_dist::DistributedScanPlan::default();
    // A pushed-down predicate, projection, aggregate or top-K reads the row as
    // it sits in storage, where a `VIRTUAL` generated column is a NULL
    // placeholder. Such a relation is scanned whole and filtered above the
    // scan, after `expand_virtual_generated` has produced the real values.
    let requested = if has_virtual_generated(t) {
        &default_scan_plan
    } else {
        scan_plan.unwrap_or(&default_scan_plan)
    };
    let decision = crate::rls::decide(
        &read_ctx.rls(),
        t,
        crabka_pgcatalog::policy::PolicyCommand::Select,
    )?;
    // Sanitize where the `ScanRequest` is built rather than trusting each caller
    // to have stripped its own plan: an aggregate folded inside the scanner sums
    // rows the qual has not removed yet, and a top-K truncates before it runs.
    let sanitized = crate::rls::sanitize_scan_plan(&decision, requested);
    let distributed_plan = sanitized.as_ref().unwrap_or(requested);
    if let Some(unrestricted) = crate::rls::UnrestrictedTable::from_decision(permit, &decision, t)
        && let Some(rows) = try_scan_with_local_index(read_ctx, unrestricted, distributed_plan)?
    {
        let rows = scanned_rows(read_ctx, t, rows, &stamp, reads)?;
        return Ok(crate::rls::RawScan::of_relation(t, scope, rows));
    }
    let scan_request = ScanRequest {
        local: read_ctx.kv,
        global: read_ctx.global,
        global_snapshot: read_ctx.gsnap,
        snapshot: read_ctx.snapshot,
        own_xid: read_ctx.own,
        command_id: read_ctx.command_id,
        read_ts: None,
        own_start_ts: None,
        table: t,
        interval: RowInterval::ALL,
        predicate: distributed_plan.predicate.clone(),
        projection: distributed_plan.projection.clone(),
        partial_aggregate: distributed_plan.partial_aggregate.clone(),
        top_k: distributed_plan.top_k.clone(),
    };
    let scan_attempt = read_ctx.statement_memory.reserve();
    let (mut rows, scan_attempt, projection) = match crate::scanner::collect_cursor_bounded(
        read_ctx.range_scanner,
        scan_request,
        scan_attempt.memory(),
    ) {
        Ok(rows) => (rows, scan_attempt, distributed_plan.projection.clone()),
        Err(error) if should_retry_without_scan_pushdown(&error, distributed_plan) => {
            drop(scan_attempt);
            let fallback_attempt = read_ctx.statement_memory.reserve();
            let rows = crate::scanner::collect_cursor_bounded(
                read_ctx.range_scanner,
                ScanRequest {
                    local: read_ctx.kv,
                    global: read_ctx.global,
                    global_snapshot: read_ctx.gsnap,
                    snapshot: read_ctx.snapshot,
                    own_xid: read_ctx.own,
                    command_id: read_ctx.command_id,
                    read_ts: None,
                    own_start_ts: None,
                    table: t,
                    interval: RowInterval::ALL,
                    predicate: PredicatePushdown::FullScan,
                    projection: crate::ProjectionPushdown::All,
                    partial_aggregate: None,
                    top_k: None,
                },
                fallback_attempt.memory(),
            )?;
            (rows, fallback_attempt, ProjectionPushdown::All)
        }
        Err(error) => return Err(error),
    };
    restore_scan_projection(&mut rows, &projection, t.columns.len())?;
    scan_attempt.replace_with(
        rows.iter()
            .map(|row| crate::scanner::datum_row_bytes(&row.row))
            .sum(),
    )?;
    let rows = scanned_rows(read_ctx, t, rows, &stamp, reads)?;
    Ok(crate::rls::RawScan::of_relation(t, scope, rows))
}

/// `scanned` as the rows a relation yields: the `regclass` columns resolved,
/// the `VIRTUAL` generated columns expanded, and the hidden system columns this
/// scan carries appended.
///
/// It takes the [`ScannedRow`]s whole rather than a row list beside a list of
/// identities, so the identity a row's `ctid` is derived from can only ever be
/// that row's own. The two passes over the rows run first: both read a row by
/// the ordinals of the relation's declared columns and would mistake a longer
/// row for a corrupt one.
fn scanned_rows(
    read_ctx: &crate::subquery::SubCtx<'_>,
    t: &Table,
    scanned: Vec<ScannedRow>,
    stamp: &crate::scope::SystemStamp,
    reads: crate::scope::GeneratedReads<'_>,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows: Vec<Vec<Datum>> = Vec::with_capacity(scanned.len());
    let mut identities: Vec<(u64, u64, u32, u32)> = Vec::with_capacity(scanned.len());
    for row in scanned {
        rows.push(row.row);
        identities.push((row.rowid, row.xmin, row.cmin, row.cmax));
    }
    resolve_scanned_regclass(
        read_ctx.catalog_kv,
        read_ctx.eval_ctx.resolution(),
        t,
        &mut rows,
    )?;
    expand_virtual_generated(t, &mut rows, read_ctx.eval_ctx, reads)?;
    for (row, (identity, xmin, cmin, cmax)) in rows.iter_mut().zip(identities) {
        stamp.extend_row(row, identity, xmin, 0, cmin, cmax);
    }
    Ok(rows)
}

fn restore_scan_projection(
    rows: &mut [ScannedRow],
    projection: &ProjectionPushdown,
    width: usize,
) -> Result<(), ExecError> {
    let ProjectionPushdown::Columns(columns) = projection else {
        return Ok(());
    };
    for row in rows {
        if row.row.len() != columns.len() {
            return Err(ExecError::Unsupported(
                "projection pushdown returned an unexpected row width".into(),
            ));
        }
        let mut restored = vec![Datum::Null; width];
        for (value, column) in row.row.drain(..).zip(columns) {
            let Some(slot) = restored.get_mut(*column) else {
                return Err(ExecError::Unsupported(
                    "projection pushdown column is outside the table row".into(),
                ));
            };
            *slot = value;
        }
        row.row = restored;
    }
    Ok(())
}

/// `rows`, each extended by the columns `stamp` carries.
///
/// The foreign path's half of [`scanned_rows`], for rows a remote system
/// returned with no identity attached — which is why the identity passed here
/// is a placeholder no row is stamped with, and why
/// [`crate::scope::SystemColumns::of`] carries no `ctid` for a foreign table.
fn stamped(mut rows: Vec<Vec<Datum>>, stamp: &crate::scope::SystemStamp) -> Vec<Vec<Datum>> {
    for row in &mut rows {
        stamp.extend_row(row, NO_ROW_IDENTITY, 0, 0, 0, 0);
    }
    rows
}

/// The identity a relation that stores no row of its own passes to
/// [`crate::scope::SystemStamp::extend_row`].
///
/// It is never read: such a relation is refused a `ctid` by
/// [`crate::scope::SystemColumns::of`], which is the one column the identity
/// feeds. Zero rather than any other number because it is the identity
/// [`crate::scope::row_ctid`] documents as never handed out, so a value derived
/// from it would be visibly the invalid item pointer rather than a plausible
/// row.
pub(crate) const NO_ROW_IDENTITY: u64 = 0;

/// An ordinal no row has, which [`permuted_row`] reads as NULL.
const NO_SUCH_COLUMN: usize = usize::MAX;

/// [`column_mapping`] for a tree scan, extended by the hidden system columns
/// the tree carries.
///
/// `source.columns.len()` is where the child's own scan appended the first of
/// its own, whether the child was a plain relation ([`Scope::single`]) or a
/// nested tree ([`crate::rls::RawScan::tree_of`]) — both scopes are exactly the
/// child's declared width before a system column goes on. Reading them through
/// the same permutation as every user column is what makes an inheritance
/// grandchild and a sub-partitioned leaf report THEMSELVES: each level stamps
/// its own, and the level above only carries them up. Two rows of one
/// inheritance tree can therefore share a `ctid`, exactly as they can in
/// `PostgreSQL`, where the pair that identifies a row across a tree is
/// `(tableoid, ctid)`.
///
/// A child can carry less than the tree does — [`crate::scope::SystemColumns`]
/// declines a relation that declares a column of its own by the name, and
/// declines `ctid` to a foreign one — so the child's own set decides where each
/// column is read from, and a column it does not have reads as NULL rather than
/// as whichever value sits at that offset in its row.
fn tree_ordinals(
    parent: &Table,
    source: &Table,
    tree: crate::scope::SystemColumns,
    refs: Option<&crate::scope::StatementRefs>,
) -> Result<Vec<usize>, ExecError> {
    let child = crate::scope::SystemColumns::of(refs, source);
    let mut ordinals = column_mapping(parent, source)?;
    let base = source.columns.len();
    if tree.tableoid {
        ordinals.push(if child.tableoid { base } else { NO_SUCH_COLUMN });
    }
    if tree.xmin {
        ordinals.push(if child.xmin {
            base + usize::from(child.tableoid)
        } else {
            NO_SUCH_COLUMN
        });
    }
    if tree.ctid {
        ordinals.push(if child.ctid {
            base + usize::from(child.tableoid) + usize::from(child.xmin)
        } else {
            NO_SUCH_COLUMN
        });
    }
    Ok(ordinals)
}
