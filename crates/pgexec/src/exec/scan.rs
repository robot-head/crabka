//! MVCC scanning, row qualification, and scan-bound extraction.

use super::*;

/// The ordinary local relation a single-item `FROM` names, or `None` when it is
/// anything a scan plan cannot be built over.
pub(crate) fn scan_plan_table(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    reference: &crabka_pgparser::ast::RelationRef,
) -> Result<Option<Table>, ExecError> {
    let name = resolve_relation(
        catalog_kv,
        resolution,
        reference,
        SchemaDisposition::Reference,
    )?;
    Ok(crabka_pgcatalog::get_table(catalog_kv, &name)
        .ok()
        .filter(|table| table.foreign.is_none()))
}

/// Scan a table's visible rows under `snapshot` (and the caller's own xid for
/// read-your-writes). Returns `(rowid, xmin, row)` for the one visible version
/// of each live row, in heap order.
pub(crate) fn scan_live(
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &crabka_pgmvcc::visibility::Snapshot,
    snapshot: &crabka_pgmvcc::visibility::Snapshot,
    own: Option<u64>,
    table: &crabka_pgcatalog::Table,
) -> Result<Vec<(u64, u64, Vec<crabka_pgtypes::Datum>)>, ExecError> {
    scan_live_interval(kv, global, gsnap, snapshot, own, table, RowInterval::ALL).map(|rows| {
        rows.into_iter()
            .map(|row| (row.rowid, row.xmin, row.row))
            .collect()
    })
}

/// Scan visible rows within a rowid interval under `snapshot`, in heap order.
pub(crate) fn scan_live_interval(
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &crabka_pgmvcc::visibility::Snapshot,
    snapshot: &crabka_pgmvcc::visibility::Snapshot,
    own: Option<u64>,
    table: &crabka_pgcatalog::Table,
    interval: RowInterval,
) -> Result<Vec<ScannedRow>, ExecError> {
    scan_live_interval_at_command(kv, global, gsnap, snapshot, own, None, table, interval)
}

/// Scan visible rows within a rowid interval, including command-counter
/// visibility for tuples owned by `own` when supplied.
pub(crate) fn scan_live_interval_at_command(
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &crabka_pgmvcc::visibility::Snapshot,
    snapshot: &crabka_pgmvcc::visibility::Snapshot,
    own: Option<u64>,
    command_id: Option<u32>,
    table: &crabka_pgcatalog::Table,
    interval: RowInterval,
) -> Result<Vec<ScannedRow>, ExecError> {
    let scanned = scan_table_for_catalog_interval(kv, table, interval)?;
    let mut out: Vec<ScannedRow> = Vec::new();
    let mut i = 0;
    while i < scanned.len() {
        crate::session::check_query_canceled()?;
        let prefix = crabka_pgmvcc::version::row_prefix_of(&scanned[i].0)?.to_vec();
        let rowid = physical_rowid(table, &prefix)?;
        if !interval.contains(rowid) {
            while i < scanned.len()
                && crabka_pgmvcc::version::row_prefix_of(&scanned[i].0)? == prefix.as_slice()
            {
                i += 1;
            }
            continue;
        }
        let mut visible: Option<(u64, u32, u32, Vec<crabka_pgtypes::Datum>)> = None;
        let mut live_count: usize = 0;
        while i < scanned.len()
            && crabka_pgmvcc::version::row_prefix_of(&scanned[i].0)? == prefix.as_slice()
        {
            let (xmin, xmax, cmin, cmax, row) =
                crabka_pgmvcc::version::decode_tuple_with_command_ids(&scanned[i].1)?;
            if self::mvcc::satisfies_mvcc_at_command(
                xmin,
                xmax,
                cmin,
                cmax,
                snapshot,
                own,
                command_id,
                global_status(kv, global, gsnap),
            )? {
                live_count += 1;
                // `is_none_or`, NOT `map_or(true, …)` — see find_visible_one above.
                if visible.as_ref().is_none_or(|(cur, _, _, _)| xmin > *cur) {
                    visible = Some((xmin, cmin, cmax, row));
                }
            }
            i += 1;
        }
        debug_assert!(
            live_count <= 1,
            "scan_live: {live_count} live versions for rowid {rowid} under one snapshot \
             — MVCC at-most-one-live invariant violated"
        );
        if let Some((xmin, cmin, cmax, row)) = visible {
            out.push(ScannedRow {
                rowid,
                xmin,
                cmin,
                cmax,
                row,
            });
        }
    }
    // An UPDATE creates a new tuple version at the heap's tail. This engine's
    // MVCC xid is that version's durable creation order; rowid keeps insertion
    // order for versions born in the same command.
    out.sort_by_key(|row| (row.xmin, row.rowid));
    Ok(out)
}

/// Scan timestamp-transaction versions within a rowid interval under `read_ts`.
pub(crate) fn scan_ts_live_interval(
    kv: &dyn Kv,
    primary_kv: &dyn Kv,
    table: &crabka_pgcatalog::Table,
    read_ts: ReadTimestamp,
    own_start_ts: Option<TimestampTransactionId>,
    interval: RowInterval,
) -> Result<Vec<ScannedRow>, ExecError> {
    let scanned = scan_table_for_catalog_interval(kv, table, interval)?;
    let mut out = Vec::new();
    let mut i = 0;
    while i < scanned.len() {
        crate::session::check_query_canceled()?;
        let prefix = crabka_pgmvcc::version::row_prefix_of(&scanned[i].0)?.to_vec();
        let rowid = physical_rowid(table, &prefix)?;
        let bucket = physical_bucket(table, &prefix)?;
        if !interval.contains(rowid) {
            while i < scanned.len()
                && crabka_pgmvcc::version::row_prefix_of(&scanned[i].0)? == prefix.as_slice()
            {
                i += 1;
            }
            continue;
        }

        let mut visible: Option<(u64, u64, Option<Vec<crabka_pgtypes::Datum>>)> = None;
        while i < scanned.len()
            && crabka_pgmvcc::version::row_prefix_of(&scanned[i].0)? == prefix.as_slice()
        {
            let version = crabka_pgmvcc::version::decode_ts_tuple(&scanned[i].1)?;
            let start_ts = TimestampTransactionId::new(version.start_ts).map_err(|error| {
                ExecError::Unsupported(format!("invalid timestamp intent start timestamp: {error}"))
            })?;
            // Corrupt or unreadable descriptor metadata is never a legacy version:
            // failing closed prevents an unverified participant write becoming visible.
            let descriptor =
                crate::timestamp_txn::read_timestamp_txn_descriptor(primary_kv, start_ts)?;
            let verified_distributed_intent = match descriptor.as_ref() {
                Some(descriptor) => crate::timestamp_txn::local_intent_matches_descriptor(
                    kv, descriptor, table.id, bucket, rowid,
                )?,
                None => false,
            };
            let descriptor_operation = descriptor.as_ref().is_some_and(|descriptor| {
                crate::timestamp_txn::local_terminal_operation_matches_descriptor(
                    descriptor, table.id, bucket, rowid,
                )
            });
            // Per-range local sequences reuse stamp values, so an unrelated
            // transaction's descriptor can sit at this version's start
            // timestamp. A descriptor that names this row neither as a
            // verified intent nor as a terminal operation belongs to such a
            // colliding transaction; treating it as authoritative would fence
            // a committed single-shard row invisible.
            let primary_decision = (verified_distributed_intent || descriptor_operation)
                .then(|| descriptor.as_ref().map(|descriptor| descriptor.decision))
                .flatten();
            let candidate = match (version.state, primary_decision, verified_distributed_intent) {
                (
                    crabka_pgmvcc::version::TsVersionState::Intent,
                    Some(PrimaryTxnDecision::Pending),
                    true,
                ) if own_start_ts == Some(start_ts) => Some((u64::MAX, Some(version.row))),
                // A range-0 commit decision makes every prewritten intent logically
                // visible, even if this particular participant has not yet completed
                // its idempotent physical resolution.
                (
                    crabka_pgmvcc::version::TsVersionState::Intent,
                    Some(PrimaryTxnDecision::Committed(commit_ts)),
                    true,
                ) if commit_ts.get() <= read_ts.get() => Some((commit_ts.get(), Some(version.row))),
                (
                    crabka_pgmvcc::version::TsVersionState::Committed { commit_ts },
                    Some(PrimaryTxnDecision::Committed(primary_commit_ts)),
                    _,
                ) if commit_ts == primary_commit_ts.get() && commit_ts <= read_ts.get() => {
                    descriptor_operation.then_some((commit_ts, Some(version.row)))
                }
                (
                    crabka_pgmvcc::version::TsVersionState::Deleted { commit_ts },
                    Some(PrimaryTxnDecision::Committed(primary_commit_ts)),
                    _,
                ) if commit_ts == primary_commit_ts.get() && commit_ts <= read_ts.get() => {
                    descriptor_operation.then_some((commit_ts, None))
                }
                // Legacy/single-range timestamp versions have no descriptor.
                (crabka_pgmvcc::version::TsVersionState::Committed { commit_ts }, None, _)
                    if commit_ts <= read_ts.get() =>
                {
                    Some((commit_ts, Some(version.row)))
                }
                (crabka_pgmvcc::version::TsVersionState::Deleted { commit_ts }, None, _)
                    if commit_ts <= read_ts.get() =>
                {
                    Some((commit_ts, None))
                }
                _ => None,
            };
            if let Some((commit_ts, row)) = candidate
                && visible
                    .as_ref()
                    .is_none_or(|(_, current_commit_ts, _)| commit_ts > *current_commit_ts)
            {
                visible = Some((version.start_ts, commit_ts, row));
            }
            i += 1;
        }
        if let Some((start_ts, _commit_ts, Some(row))) = visible {
            out.push(ScannedRow {
                rowid,
                xmin: start_ts,
                cmin: 0,
                cmax: 0,
                row,
            });
        }
    }
    out.sort_by_key(|row| (row.rowid, row.xmin));
    Ok(out)
}

pub(crate) fn physical_rowid(table: &Table, row_prefix: &[u8]) -> Result<u64, ExecError> {
    if matches!(
        table.sharding,
        Some(crabka_pgcatalog::ShardingStrategy::Hash(_))
    ) {
        return Ok(crabka_pgkv::key::bucket_rowid_of(table.id, row_prefix)?.1);
    }
    Ok(crabka_pgkv::key::rowid_of(table.id, row_prefix)?)
}

fn physical_bucket(table: &Table, row_prefix: &[u8]) -> Result<Option<u32>, ExecError> {
    if matches!(
        table.sharding,
        Some(crabka_pgcatalog::ShardingStrategy::Hash(_))
    ) {
        return Ok(Some(
            crabka_pgkv::key::bucket_rowid_of(table.id, row_prefix)?.0,
        ));
    }
    Ok(None)
}

fn scan_table_for_catalog_interval(
    kv: &dyn Kv,
    table: &Table,
    interval: RowInterval,
) -> Result<crabka_pgkv::KvScan, ExecError> {
    if matches!(
        table.sharding,
        Some(crabka_pgcatalog::ShardingStrategy::Hash(_))
    ) {
        return scan_table_interval(kv, table.id, RowInterval::ALL);
    }
    scan_table_interval(kv, table.id, interval)
}

pub(crate) fn scan_table_interval(
    kv: &dyn Kv,
    table_id: u32,
    interval: RowInterval,
) -> Result<crabka_pgkv::KvScan, ExecError> {
    let start = interval.start.map_or_else(
        || crabka_pgkv::key::table_prefix(table_id),
        |rowid| crabka_pgkv::key::row_key(table_id, rowid),
    );
    let end = interval.end.map_or_else(
        || {
            let mut end = crabka_pgkv::key::table_prefix(table_id);
            let last = end.last_mut().expect("table prefix is non-empty");
            *last = last.checked_add(1).expect("primary index has a successor");
            end
        },
        |rowid| crabka_pgkv::key::row_key(table_id, rowid),
    );
    Ok(kv.scan_range(&start, &end)?)
}

/// Evaluate an optional WHERE predicate against a row (NULL => false, like SELECT).
pub(crate) fn row_matches(
    filter: Option<&Expr>,
    scope: &Scope,
    row: &[crabka_pgtypes::Datum],
    ctx: &crate::clock::EvalCtx,
) -> Result<bool, ExecError> {
    match filter {
        None => Ok(true),
        Some(f) => match crate::eval::eval(f, scope, row, ctx)? {
            crabka_pgtypes::Datum::Bool(b) => Ok(b),
            crabka_pgtypes::Datum::Null => Ok(false),
            _ => Err(ExecError::TypeMismatch(
                "argument of WHERE must be type boolean".into(),
            )),
        },
    }
}

/// Fold a literal object-identifier cast before a relation scan.
///
/// `reg*` input can consult the user catalog. Leaving `'f'::regproc` inside a
/// row predicate consequently rebuilt that catalog for every scanned `pg_proc`
/// row. The cast has no row dependency, so evaluate it once after binding.
pub(crate) fn fold_literal_reg_casts(
    expr: &Expr,
    ctx: &crate::clock::EvalCtx,
) -> Result<Expr, ExecError> {
    crate::grouping::rewrite(
        expr,
        &mut |node| {
            let Expr::Cast { expr, ty } = node else {
                return Ok(None);
            };
            if crate::reg_fn::RegKind::of(*ty).is_none()
                || !matches!(
                    expr.as_ref(),
                    Expr::StringLiteral(_) | Expr::IntLiteral(_) | Expr::NumericLiteral(_)
                )
            {
                return Ok(None);
            }
            Ok(Some(Expr::Const {
                value: crate::eval::eval(node, &Scope::empty(), &[], ctx)?,
                ty: *ty,
            }))
        },
        false,
    )
}

/// SP40 Task 14: extract per-partition offset bounds from a single-foreign-table
/// query's top-level `WHERE` for pushdown into the Kafka foreign scan.
///
/// Walks the top-level `AND` chain of the filter and, for every `_partition = N`
/// constraint, collects the `_offset` range comparisons scoped to that partition
/// into [`ScanBounds`]. This is a PURE OPTIMIZATION: anything not representable in
/// `ScanBounds` (a bare `_offset` with no `_partition =`, a `_timestamp`/`LIMIT`
/// constraint, an `OR`, a non-envelope predicate) is simply omitted here and
/// remains a residual `WHERE` filter applied locally after the scan. Callers MUST
/// keep evaluating the full `WHERE`; pushed bounds must never change results.
///
/// Conversions (the scan reads `[start, end)` per partition):
/// - `_offset >= a` → start `a`; `_offset > a` → start `a + 1` (inclusive lower).
/// - `_offset <= b` → end `b + 1`; `_offset < b` → end `b` (exclusive upper).
/// - `_offset BETWEEN a AND b` → start `a`, end `b + 1` (PG bounds are inclusive).
///
/// Only offset bounds anchored to a concrete `_partition = N` are emitted: under
/// this `ScanBounds` shape (`Vec<(partition, offset)>`) a partition-less offset
/// cannot target a partition, so it stays residual.
#[must_use]
pub(crate) fn extract_scan_bounds(filter: Option<&Expr>) -> ScanBounds {
    let mut bounds = ScanBounds::default();
    let Some(filter) = filter else {
        return bounds;
    };

    // Flatten the top-level AND chain into its conjuncts. An OR or any other
    // shape is left intact (and thus never matches a comparison below), so it
    // contributes nothing — it remains a residual filter.
    let mut conjuncts = Vec::new();
    collect_conjuncts(filter, &mut conjuncts);

    // Resolve the single `_partition = N` anchor, if exactly one is present. With
    // zero (or conflicting/multiple) partition equalities we cannot scope offsets
    // to a partition, so we push nothing and let WHERE do all the work.
    let mut partition: Option<i32> = None;
    for c in &conjuncts {
        if let Some(p) = match_partition_eq(c) {
            match partition {
                None => partition = Some(p),
                Some(prev) if prev == p => {}
                // Two different `_partition =` values → unsatisfiable as written;
                // don't try to push, let the residual WHERE return zero rows.
                Some(_) => return ScanBounds::default(),
            }
        }
    }
    let Some(partition) = partition else {
        return bounds;
    };

    // Tightest inclusive-start / exclusive-end across all offset conjuncts.
    let mut start: Option<i64> = None;
    let mut end: Option<i64> = None;
    let mut tighten_start = |v: i64| {
        start = Some(start.map_or(v, |cur: i64| cur.max(v)));
    };
    let mut tighten_end = |v: i64| {
        end = Some(end.map_or(v, |cur: i64| cur.min(v)));
    };

    for c in &conjuncts {
        match match_offset_bound(c) {
            Some(OffsetBound::StartIncl(v)) => tighten_start(v),
            Some(OffsetBound::EndExcl(v)) => tighten_end(v),
            Some(OffsetBound::Between { start: s, end: e }) => {
                tighten_start(s);
                tighten_end(e);
            }
            None => {}
        }
    }

    if let Some(s) = start {
        bounds.start_offsets.push((partition, s));
    }
    if let Some(e) = end {
        bounds.end_offsets.push((partition, e));
    }
    bounds
}

/// Flatten a top-level `AND` chain into its leaf conjuncts (depth-first). A node
/// that is not an `AND` is itself one conjunct.
pub(crate) fn collect_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let Expr::Binary {
        op: crabka_pgparser::ast::BinaryOp::And,
        left,
        right,
    } = expr
    {
        collect_conjuncts(left, out);
        collect_conjuncts(right, out);
    } else {
        out.push(expr);
    }
}

/// An envelope-column reference by bare name (`_partition`/`_offset`/…). Envelope
/// columns are unqualified in practice; a table-qualified `t._offset` also matches
/// on the bare name (the qualifier is the single foreign table in scope).
fn envelope_col(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column { name, .. } => Some(name.as_str()),
        _ => None,
    }
}

/// Parse an integer literal expression to `i64`. Only bare/negated integer
/// literals are recognized (offsets/partitions are integers); anything else
/// (params, casts, non-integers) is not pushable and returns `None`.
fn int_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::IntLiteral(s) => s.parse::<i64>().ok(),
        Expr::Unary {
            op: crabka_pgparser::ast::UnaryOp::Neg,
            expr,
        } => int_literal(expr).map(|v| -v),
        _ => None,
    }
}

/// Match `_partition = N` (either operand order) and return `N`.
fn match_partition_eq(expr: &Expr) -> Option<i32> {
    let Expr::Binary {
        op: crabka_pgparser::ast::BinaryOp::Eq,
        left,
        right,
    } = expr
    else {
        return None;
    };
    let v = if envelope_col(left) == Some("_partition") {
        int_literal(right)?
    } else if envelope_col(right) == Some("_partition") {
        int_literal(left)?
    } else {
        return None;
    };
    i32::try_from(v).ok()
}

/// An offset constraint normalized to the scan's `[start, end)` convention.
enum OffsetBound {
    /// Inclusive lower offset.
    StartIncl(i64),
    /// Exclusive upper offset.
    EndExcl(i64),
    /// `BETWEEN a AND b` → inclusive `start`, exclusive `end`.
    Between { start: i64, end: i64 },
}

/// Match an `_offset` comparison / BETWEEN and normalize it to an [`OffsetBound`].
/// Returns `None` for anything that is not an `_offset` range constraint. The
/// comparison is recognized with the column on either side (the operator is
/// mirrored when the column is on the right).
fn match_offset_bound(expr: &Expr) -> Option<OffsetBound> {
    use crabka_pgparser::ast::BinaryOp;
    match expr {
        Expr::Binary { op, left, right } => {
            // Normalize to `_offset <op> literal` by mirroring when reversed.
            let (op, lit) = if envelope_col(left) == Some("_offset") {
                (*op, int_literal(right)?)
            } else if envelope_col(right) == Some("_offset") {
                (mirror_op(*op)?, int_literal(left)?)
            } else {
                return None;
            };
            match op {
                BinaryOp::Ge => Some(OffsetBound::StartIncl(lit)),
                BinaryOp::Gt => Some(OffsetBound::StartIncl(lit + 1)),
                BinaryOp::Le => Some(OffsetBound::EndExcl(lit + 1)),
                BinaryOp::Lt => Some(OffsetBound::EndExcl(lit)),
                _ => None,
            }
        }
        Expr::Between {
            expr,
            low,
            high,
            negated: false,
        } if envelope_col(expr) == Some("_offset") => {
            let lo = int_literal(low)?;
            let hi = int_literal(high)?;
            Some(OffsetBound::Between {
                start: lo,
                end: hi + 1,
            })
        }
        _ => None,
    }
}

/// Mirror a comparison operator for the reversed-operand form (`5 < _offset`
/// means `_offset > 5`). Only the inequalities used for offset bounds are mapped.
fn mirror_op(op: crabka_pgparser::ast::BinaryOp) -> Option<crabka_pgparser::ast::BinaryOp> {
    use crabka_pgparser::ast::BinaryOp;
    match op {
        BinaryOp::Lt => Some(BinaryOp::Gt),
        BinaryOp::Le => Some(BinaryOp::Ge),
        BinaryOp::Gt => Some(BinaryOp::Lt),
        BinaryOp::Ge => Some(BinaryOp::Le),
        _ => None,
    }
}
