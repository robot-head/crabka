//! CLUSTER planning, catalog updates, and heap rewrites.

use super::*;

struct ClusterUnit {
    table: Table,
    index: crabka_pgcatalog::Index,
}

/// `there is no previously clustered index for table "…"` (42704) — what
/// `PostgreSQL` raises for `CLUSTER <table>` with no `USING` and no recorded
/// index.
pub(crate) fn no_clustered_index(name: &crabka_pgcatalog::RelationName) -> ExecError {
    ExecError::UndefinedObject(format!(
        "there is no previously clustered index for table \"{}\"",
        name.name
    ))
}

/// `CLUSTER [ <table> [ USING <index> ] ]`.
///
/// `PostgreSQL` rewrites the heap so that a sequential scan returns the rows in
/// the index's order. Gres stores a row at `/<table_id>/1/<rowid>` and every
/// scan hands rows back in rowid order — that is [`crate::scanner::RangeScanner`]'s
/// documented contract, not an accident of the map — so the same effect is a
/// *renumbering*: read the live rows, sort them by the index key, and rewrite
/// them over a fresh ascending block of rowids.
///
/// The renumbering stays inside the rowid contract because the new block comes
/// from [`crate::seq::SequenceManager::alloc`], whose invariant is that the
/// persisted counter is at or above every rowid ever handed out. The rows move
/// strictly *upward* into rowids that were never used; the vacated ones are
/// abandoned, never recycled. Nothing is minted by hand.
///
/// The rewrite is an ordinary MVCC delete-and-reinsert under each row's
/// exclusive lock, not a physical move: the old version is tombstoned with this
/// transaction's `xmax` and the new one written with its `xmin`, so a snapshot
/// that cannot see this transaction still reads the table exactly as it was,
/// and `ROLLBACK` leaves the heap untouched. That is also why the secondary
/// indexes need no special handling — the entries for the old rowids stay valid
/// for as long as the old versions are visible to somebody, and the ordinary
/// chain prune reclaims them afterwards.
///
/// Sharded relations are refused, as `TRUNCATE` refuses them: their hidden
/// rowids are timestamps drawn from a lease rather than a per-table counter, so
/// there is no ascending block to move rows into.
pub(super) async fn execute_cluster(
    write_ctx: &WriteContext<'_>,
    target: Option<&crabka_pgparser::ast::ClusterTarget>,
    writes: &mut StatementWrites,
    ops: &mut Vec<crabka_pgkv::WriteOp>,
) -> Result<(), ExecError> {
    let units = match target {
        Some(target) => cluster_units_for_target(write_ctx, target)?,
        None => cluster_units_for_role(write_ctx)?,
    };
    for unit in &units {
        cluster_one_relation(write_ctx, unit, writes, ops).await?;
    }
    Ok(())
}

/// The catalog writes `CLUSTER <table> USING <index>` makes: the relation's
/// `pg_index.indisclustered` moves to the named index.
///
/// Raised separately from the reordering because a catalog record carries no
/// MVCC header — it has to go through the session's catalog seam, which records
/// the before-images `ROLLBACK` undoes it with, rather than riding the row
/// batch, whose undo is the aborted xid on each tuple.
///
/// Empty for `CLUSTER <table>` (which reuses the recorded index rather than
/// choosing one) and for a partitioned parent, whose indexes `PostgreSQL` never
/// marks.
pub(crate) fn cluster_mark_ops(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    target: &crabka_pgparser::ast::ClusterTarget,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let Some(index_name) = target.index.as_deref() else {
        return Ok(Vec::new());
    };
    let name = crate::relname::resolve_relation(
        catalog_kv,
        resolution,
        &target.table,
        crate::relname::SchemaDisposition::Utility,
    )?;
    if crate::partition::is_partitioned(catalog_kv, &name)? {
        return Ok(Vec::new());
    }
    let table = crabka_pgcatalog::get_table(catalog_kv, &name)?;
    let index = cluster_index_named(catalog_kv, &table, index_name)?;
    record_clustered_index_ops(catalog_kv, &table, Some(&index.name))
}

/// Mark `index` as `table`'s clustered index, clearing the flag from every other
/// index on the relation. `None` clears all of them (`SET WITHOUT CLUSTER`).
///
/// Only the records whose flag actually moves are rewritten, so the batch is
/// empty — and the statement writes nothing — when the mark is already where it
/// is being asked to go.
pub(crate) fn record_clustered_index_ops(
    catalog_kv: &dyn Kv,
    table: &Table,
    index: Option<&str>,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let mut ops = Vec::new();
    for mut candidate in crabka_pgcatalog::list_table_indexes(catalog_kv, &table.name)? {
        let wanted = index.is_some_and(|name| name == candidate.name);
        if candidate.clustered == wanted {
            continue;
        }
        candidate.clustered = wanted;
        ops.extend(crabka_pgcatalog::put_index_ops(&candidate));
    }
    Ok(ops)
}

/// Resolve, authorize and expand the relation one `CLUSTER <table> …` names.
fn cluster_units_for_target(
    write_ctx: &WriteContext<'_>,
    target: &crabka_pgparser::ast::ClusterTarget,
) -> Result<Vec<ClusterUnit>, ExecError> {
    let catalog_kv = write_ctx.catalog_kv;
    let name = crate::relname::resolve_relation(
        catalog_kv,
        write_ctx.eval_ctx.resolution(),
        &target.table,
        crate::relname::SchemaDisposition::Utility,
    )?;
    let table = crabka_pgcatalog::get_table(catalog_kv, &name)?;
    require_cluster_privilege(write_ctx, &table)?;
    if crate::partition::is_partitioned(catalog_kv, &name)? {
        // A partitioned parent holds no rows: `PostgreSQL` reorders each leaf by
        // the leaf's own copy of the index and leaves every partitioned index
        // unmarked, which is why `CLUSTER <partitioned parent>` with no `USING`
        // still reports that there is no previously clustered index.
        let Some(index_name) = target.index.as_deref() else {
            return Err(no_clustered_index(&name));
        };
        let parent_index = cluster_index_named(catalog_kv, &table, index_name)?;
        let mut units = Vec::new();
        for leaf in crate::partition::leaves_of(catalog_kv, &name)? {
            let leaf_table = crabka_pgcatalog::get_table(catalog_kv, &leaf)?;
            // The leaf's copy is matched by key, not by name: a partition's
            // index carries its own generated name.
            let Some(index) = crabka_pgcatalog::list_table_indexes(catalog_kv, &leaf)?
                .into_iter()
                .find(|index| index.columns == parent_index.columns)
            else {
                continue;
            };
            units.push(ClusterUnit {
                table: leaf_table,
                index,
            });
        }
        return Ok(units);
    }
    let index = match target.index.as_deref() {
        Some(index_name) => cluster_index_named(catalog_kv, &table, index_name)?,
        None => crabka_pgcatalog::list_table_indexes(catalog_kv, &name)?
            .into_iter()
            .find(|index| index.clustered)
            .ok_or_else(|| no_clustered_index(&name))?,
    };
    Ok(vec![ClusterUnit { table, index }])
}

/// Every relation the bare `CLUSTER` reorders: the ones that already record a
/// clustered index and that the current role may cluster.
///
/// `PostgreSQL` warns about, and then skips, a relation the role cannot
/// cluster; Gres has no `NoticeResponse` path from here, so the skip happens
/// silently. The regression suite runs this statement under
/// `client_min_messages = ERROR` for exactly that reason.
fn cluster_units_for_role(write_ctx: &WriteContext<'_>) -> Result<Vec<ClusterUnit>, ExecError> {
    let catalog_kv = write_ctx.catalog_kv;
    let mut units = Vec::new();
    for index in marked_clustered_indexes(catalog_kv)? {
        let table = crabka_pgcatalog::get_table(catalog_kv, &index.table)?;
        if require_cluster_privilege(write_ctx, &table).is_err() {
            continue;
        }
        units.push(ClusterUnit { table, index });
    }
    Ok(units)
}

/// Every index carrying `pg_index.indisclustered`, in catalog order.
///
/// The session reads this to know which relations a bare `CLUSTER` will reach,
/// so it can lock them before the executor starts moving their rows — the
/// executor's own view of the set has to agree with the one that was locked,
/// which is why both come from here.
pub(crate) fn marked_clustered_indexes(
    catalog_kv: &dyn Kv,
) -> Result<Vec<crabka_pgcatalog::Index>, ExecError> {
    Ok(crabka_pgcatalog::list_indexes(catalog_kv)?
        .into_iter()
        .filter(|index| index.clustered)
        .collect())
}

/// `PostgreSQL` authorizes `CLUSTER` with the relation's `MAINTAIN` privilege,
/// which only the owner (and the superuser) holds here, and reports the refusal
/// as `permission denied for table <name>` rather than `must be owner of`.
fn require_cluster_privilege(write_ctx: &WriteContext<'_>, table: &Table) -> Result<(), ExecError> {
    let role = write_ctx.fctx.effective_role();
    if crabka_pgcatalog::role_has_privs_of(write_ctx.catalog_kv, role, &table.owner)?
        || crate::rls::role_is_superuser(write_ctx.catalog_kv, role)?
    {
        return Ok(());
    }
    Err(ExecError::PermissionDenied {
        kind: "table",
        relation: table.name.name.clone(),
    })
}

/// Look up the index a `CLUSTER … USING <name>` (or `CLUSTER <name> ON …`)
/// asked for, and refuse the ones whose order cannot drive a heap rewrite.
pub(crate) fn cluster_index_named(
    catalog_kv: &dyn Kv,
    table: &Table,
    index_name: &str,
) -> Result<crabka_pgcatalog::Index, ExecError> {
    let index = crabka_pgcatalog::list_table_indexes(catalog_kv, &table.name)?
        .into_iter()
        .find(|index| index.name == index_name)
        .ok_or_else(|| {
            ExecError::UndefinedObject(format!(
                "index \"{index_name}\" for table \"{}\" does not exist",
                table.name.name
            ))
        })?;
    if index.method != crabka_pgcatalog::IndexMethod::Btree {
        return Err(ExecError::Unsupported(format!(
            "cannot cluster on index \"{index_name}\" because access method does not support \
             clustering"
        )));
    }
    Ok(index)
}

/// Reorder one relation's live rows into `unit.index`'s order.
async fn cluster_one_relation(
    write_ctx: &WriteContext<'_>,
    unit: &ClusterUnit,
    writes: &mut StatementWrites,
    ops: &mut Vec<crabka_pgkv::WriteOp>,
) -> Result<(), ExecError> {
    let table = &unit.table;
    if table_uses_global_visibility(table) {
        return Err(ExecError::Unsupported(
            "CLUSTER on sharded tables is not supported: a hidden rowid there is a leased \
             timestamp, not a position in a per-table sequence"
                .into(),
        ));
    }
    reject_cluster_with_pending_checks(write_ctx, table)?;
    let scanned = scan_live(
        write_ctx.kv,
        write_ctx.global,
        write_ctx.global_snapshot,
        write_ctx.snapshot,
        Some(write_ctx.xid),
        table,
    )?;
    // Reclustering an already-clustered relation is the common case — the bare
    // `CLUSTER` reruns every marked relation — and it moves nothing. Decide that
    // from the snapshot rows, before taking a single row lock: the alternative
    // is to lock and re-read the whole table to discover there is no work, which
    // measured three times the cost of the rewrite it was avoiding.
    if cluster_scan_is_ordered(table, &unit.index, &scanned, write_ctx.eval_ctx)? {
        return Ok(());
    }
    // Lock in ascending rowid order — the order every other statement takes
    // this table's row locks in — then re-read each row under its lock, exactly
    // as `DELETE` does, so a concurrent writer is serialized against rather
    // than read around.
    let mut locked = Vec::with_capacity(scanned.len());
    for (rowid, _, _) in &scanned {
        write_ctx
            .lockmgr
            .acquire_as(
                table.id,
                *rowid,
                crate::lockmgr::LockMode::Exclusive,
                write_ctx.lock_owner,
                write_ctx.lock_wait_cap,
            )
            .await
            .map_err(lock_acquire_error)?;
        let Some((cur_rowid, cur_key_xid, cur_xmin, cur_cmin, _cur_cmax, cur_row)) =
            eval_plan_qual(
                &write_ctx.mutation(),
                table,
                *rowid,
                crate::scope::GeneratedReads::every(),
            )?
        else {
            continue; // already deleted by a concurrent committed transaction
        };
        let key = cluster_sort_key(table, &unit.index, &cur_row, write_ctx.eval_ctx)?;
        locked.push((key, cur_rowid, cur_key_xid, cur_xmin, cur_cmin, cur_row));
    }
    // Stable, so rows sharing an index key keep the order they already have —
    // the only deterministic tie-break available, and the one that makes
    // reclustering idempotent.
    locked.sort_by(|a, b| compare_cluster_key(&a.0, &b.0));
    if locked.windows(2).all(|pair| pair[0].1 < pair[1].1) {
        // The pre-check said the same thing about the snapshot rows, but under
        // READ COMMITTED the versions re-read under the locks can be newer.
        // Confirming it here is what keeps `CLUSTER` from burning a block of
        // rowids to rewrite a heap into the order it is already in.
        return Ok(());
    }
    let local_indexes = writable_local_indexes(write_ctx.catalog_kv, table)?;
    let (start, seq_op) = write_ctx.seq.alloc(
        write_ctx.kv,
        table.id,
        u64::try_from(locked.len()).map_err(|_| {
            ExecError::Unsupported("CLUSTER row count exceeds the rowid domain".into())
        })?,
    )?;
    ops.extend(seq_op);
    // `(start..)` walks the reserved block, the way the INSERT path walks the
    // block it reserved: the rows land at consecutive ascending rowids in the
    // order they were just sorted into, which is what makes the next scan read
    // them back in index order.
    for (new_rowid, (_, rowid, cur_key_xid, cur_xmin, cur_cmin, cur_row)) in
        (start..).zip(locked.iter())
    {
        apply_locked_row_delete(
            write_ctx,
            table,
            &local_indexes,
            &LockedRowDelete {
                rowid: *rowid,
                cur_key_xid: *cur_key_xid,
                cur_xmin: *cur_xmin,
                cur_cmin: *cur_cmin,
                cur_row,
            },
            writes,
            ops,
        )?;
        ops.push(crabka_pgkv::WriteOp::Put {
            key: crabka_pgmvcc::version::version_key_xid(table.id, new_rowid, write_ctx.xid),
            value: encode_table_tuple(
                table,
                write_ctx.xid,
                crabka_pgmvcc::xid::INVALID_XID,
                write_ctx.command_id,
                0,
                cur_row,
            ),
        });
        ops.extend(local_index_entry_ops(
            table,
            &local_indexes,
            new_rowid,
            cur_row,
        )?);
    }
    Ok(())
}

/// Refuse to reorder a relation the open transaction still owes a deferred
/// referential check on, as `PostgreSQL` does (55006).
///
/// A deferred check identifies its row by rowid and re-derives the key from
/// whatever version that rowid holds at `COMMIT`. Reordering moves the row to a
/// different rowid and tombstones the old one, so the check would read no
/// version at all, conclude the row is gone, and pass — committing the
/// violation it existed to catch. `PostgreSQL` reaches the same conclusion from
/// its own direction and reports it as pending trigger events, which is what a
/// deferred check is there.
fn reject_cluster_with_pending_checks(
    write_ctx: &WriteContext<'_>,
    table: &Table,
) -> Result<(), ExecError> {
    let owed = write_ctx
        .deferred_fk
        .is_some_and(|slot| slot.lock().expect("deferred fk").touches_table(table.id));
    if owed {
        return Err(ExecError::ObjectInUse(format!(
            "cannot CLUSTER \"{}\" because it has pending trigger events",
            table.name.name
        )));
    }
    Ok(())
}

/// Whether `scanned` — which arrives in rowid order, so in the order an
/// unqualified scan reads the heap — is already in `index`'s order.
fn cluster_scan_is_ordered(
    table: &Table,
    index: &crabka_pgcatalog::Index,
    scanned: &[(u64, u64, Vec<Datum>)],
    ctx: &crate::clock::EvalCtx,
) -> Result<bool, ExecError> {
    let mut previous: Option<Vec<Datum>> = None;
    for (_, _, row) in scanned {
        let key = cluster_sort_key(table, index, row, ctx)?;
        if let Some(previous) = &previous
            && compare_cluster_key(previous, &key) == std::cmp::Ordering::Greater
        {
            return Ok(false);
        }
        previous = Some(key);
    }
    Ok(true)
}

/// The values one row sorts by under `index`.
///
/// An expression key is evaluated against the row the way a generated column's
/// expression is; a plain key is the stored column value.
fn cluster_sort_key(
    table: &Table,
    index: &crabka_pgcatalog::Index,
    row: &[Datum],
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<Datum>, ExecError> {
    let mut key = Vec::with_capacity(index.columns.len());
    for column in &index.columns {
        if let Some(source) = crabka_pgcatalog::index_key_expression(column) {
            let expr = crabka_pgparser::parser::parse_expression(source)?;
            let scope = Scope::single(table, &table.name.name);
            key.push(crate::eval::eval(&expr, &scope, row, ctx)?);
            continue;
        }
        let ordinal = table
            .column_index(column)
            .ok_or_else(|| ExecError::UndefinedColumn(column.clone()))?;
        key.push(row[ordinal].clone());
    }
    Ok(key)
}

/// Compare two index keys the way a btree orders them: ascending, NULLs last.
///
/// Gres's catalog records an index's key columns as names without a per-column
/// `ASC`/`DESC` or `NULLS` clause, so there is exactly one order an index can
/// have and it is `PostgreSQL`'s default one.
fn compare_cluster_key(a: &[Datum], b: &[Datum]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (x, y) in a.iter().zip(b.iter()) {
        let ord = match (x.is_null(), y.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => crabka_pgtypes::ops::compare(x, y)
                .ok()
                .flatten()
                .unwrap_or(Ordering::Equal),
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}
