//! Table scan seam used by local and range-aware executors.

use std::{cmp::Ordering, ops::RangeBounds};

use crabka_pgcatalog::Table;
use crabka_pgmvcc::visibility::Snapshot;

use crate::ExecError;

/// Half-open rowid interval for a table scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RowInterval {
    /// Inclusive lower rowid bound.
    pub start: Option<u64>,
    /// Exclusive upper rowid bound.
    pub end: Option<u64>,
}

impl RowInterval {
    /// Every rowid in table order.
    pub const ALL: Self = Self {
        start: None,
        end: None,
    };

    /// Return whether `rowid` falls in this interval.
    #[must_use]
    pub fn contains(self, rowid: u64) -> bool {
        if self.start.is_some_and(|start| rowid < start) {
            return false;
        }
        if self.end.is_some_and(|end| rowid >= end) {
            return false;
        }
        true
    }
}

impl<R> From<R> for RowInterval
where
    R: RangeBounds<u64>,
{
    fn from(value: R) -> Self {
        let start = match value.start_bound() {
            std::ops::Bound::Included(v) => Some(*v),
            std::ops::Bound::Excluded(v) => v.checked_add(1),
            std::ops::Bound::Unbounded => None,
        };
        let end = match value.end_bound() {
            std::ops::Bound::Included(v) => v.checked_add(1),
            std::ops::Bound::Excluded(v) => Some(*v),
            std::ops::Bound::Unbounded => None,
        };
        Self { start, end }
    }
}

/// Predicate pushdown requested by the executor.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum PredicatePushdown {
    /// Remote scanner must return every visible row in the requested interval.
    #[default]
    FullScan,
    /// Conjunction of supported column/literal predicates.
    Conjunctive(Vec<ColumnPredicate>),
}

/// One supported column/literal predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnPredicate {
    /// Zero-based table column index.
    pub column: usize,
    /// Comparison operator.
    pub op: PredicateOp,
    /// Literal value to compare against.
    pub value: crabka_pgtypes::Datum,
}

/// Supported predicate comparison operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateOp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

impl PredicateOp {
    #[must_use]
    pub const fn from_binary(op: crabka_pgparser::ast::BinaryOp) -> Option<Self> {
        match op {
            crabka_pgparser::ast::BinaryOp::Eq => Some(Self::Eq),
            crabka_pgparser::ast::BinaryOp::Lt => Some(Self::Lt),
            crabka_pgparser::ast::BinaryOp::Le => Some(Self::Le),
            crabka_pgparser::ast::BinaryOp::Gt => Some(Self::Gt),
            crabka_pgparser::ast::BinaryOp::Ge => Some(Self::Ge),
            _ => None,
        }
    }

    #[must_use]
    pub const fn from_reversed_binary(op: crabka_pgparser::ast::BinaryOp) -> Option<Self> {
        match op {
            crabka_pgparser::ast::BinaryOp::Eq => Some(Self::Eq),
            crabka_pgparser::ast::BinaryOp::Lt => Some(Self::Gt),
            crabka_pgparser::ast::BinaryOp::Le => Some(Self::Ge),
            crabka_pgparser::ast::BinaryOp::Gt => Some(Self::Lt),
            crabka_pgparser::ast::BinaryOp::Ge => Some(Self::Le),
            _ => None,
        }
    }
}

/// Projection pushdown requested by the executor.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ProjectionPushdown {
    /// Return every column in table order.
    #[default]
    All,
    /// Return only these zero-based columns, in this order.
    Columns(Vec<usize>),
}

/// Partial aggregate requested from range owners.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartialAggregateSpec {
    pub function: PartialAggregateFunction,
    pub column: Option<usize>,
}

impl PartialAggregateSpec {
    #[must_use]
    pub fn from_function(name: &str, column: Option<usize>) -> Option<Self> {
        let function = match name {
            "count" => PartialAggregateFunction::Count,
            "sum" => PartialAggregateFunction::Sum,
            "min" => PartialAggregateFunction::Min,
            "max" => PartialAggregateFunction::Max,
            "avg" => PartialAggregateFunction::AvgParts,
            _ => return None,
        };
        Some(Self { function, column })
    }
}

/// Supported partial aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartialAggregateFunction {
    Count,
    Sum,
    Min,
    Max,
    AvgParts,
}

/// Per-range top-K request shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopKSpec {
    pub order_by: Vec<TopKColumn>,
    pub limit: u64,
}

/// One top-K ordering key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopKColumn {
    pub column: usize,
    pub asc: bool,
}

/// One visible row returned by a table scanner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannedRow {
    /// MVCC row identifier, used for deterministic table-order scans and locks.
    pub rowid: u64,
    /// Creating xid of the visible tuple version.
    pub xmin: u64,
    /// Decoded tuple payload.
    pub row: Vec<crabka_pgtypes::Datum>,
}

/// Inputs for one table scan.
pub struct ScanRequest<'a> {
    /// This range's local MVCC/clog store.
    pub local: &'a dyn crabka_pgkv::Kv,
    /// Range-0 global clog store.
    pub global: &'a dyn crabka_pgkv::Kv,
    /// Caller global visibility snapshot.
    pub global_snapshot: &'a Snapshot,
    /// Caller local visibility snapshot.
    pub snapshot: &'a Snapshot,
    /// Optional caller xid for read-your-writes.
    pub own_xid: Option<u64>,
    /// Optional timestamp read point for sharded-table G-9 visibility.
    pub read_ts: Option<crate::timestamp_txn::ReadTimestamp>,
    /// Table metadata.
    pub table: &'a Table,
    /// Rowid interval to scan.
    pub interval: RowInterval,
    /// Predicate pushdown contract.
    pub predicate: PredicatePushdown,
    /// Projection pushdown contract.
    pub projection: ProjectionPushdown,
    /// Optional partial aggregate request.
    pub partial_aggregate: Option<PartialAggregateSpec>,
    /// Optional top-K request.
    pub top_k: Option<TopKSpec>,
}

/// One bounded batch returned by a [`RangeCursor`].
#[derive(Debug)]
pub struct ScanPage {
    /// Visible rows in deterministic `(rowid, xmin)` table order.
    pub rows: Box<[ScannedRow]>,
    /// True when this page exhausted the cursor.
    pub is_last: bool,
}

/// Pull-based scan result. A page is not produced until the consumer asks for
/// it, which provides backpressure without a detached producer task.
#[async_trait::async_trait]
pub trait RangeCursor: Send {
    /// Return at most `max_rows` rows. Dropping the future or cursor cancels the
    /// scan and releases its snapshot/resources through ordinary drop.
    async fn next_page(&mut self, max_rows: usize) -> Result<ScanPage, ExecError>;
}

/// Compatibility cursor for scanners which still return a complete vector.
///
/// This type deliberately says "materialized": it bounds page delivery but
/// does not make its backing scan incremental. New scanner implementations
/// should override [`RangeScanner::scan_cursor`] with a native cursor.
pub struct MaterializedRangeCursor {
    rows: std::collections::VecDeque<ScannedRow>,
}

impl MaterializedRangeCursor {
    /// Wrap an already materialized scan result.
    #[must_use]
    pub fn new(rows: Vec<ScannedRow>) -> Self {
        Self { rows: rows.into() }
    }
}

#[async_trait::async_trait]
impl RangeCursor for MaterializedRangeCursor {
    async fn next_page(&mut self, max_rows: usize) -> Result<ScanPage, ExecError> {
        if max_rows == 0 {
            return Err(ExecError::Unsupported(
                "range cursor page size must be greater than zero".into(),
            ));
        }
        let take = max_rows.min(self.rows.len());
        let rows = self.rows.drain(..take).collect::<Vec<_>>().into_boxed_slice();
        Ok(ScanPage {
            rows,
            is_last: self.rows.is_empty(),
        })
    }
}

/// Seam for local or scatter-gather table scans.
pub trait RangeScanner: Send + Sync + 'static {
    /// Return visible rows in deterministic `(rowid, xmin)` table order.
    fn scan(&self, request: ScanRequest<'_>) -> Result<Vec<ScannedRow>, ExecError>;

    /// Open a pull-based cursor. The default is a compatibility adapter which
    /// materializes through [`RangeScanner::scan`].
    fn scan_cursor<'a>(
        &'a self,
        request: ScanRequest<'a>,
    ) -> Result<Box<dyn RangeCursor + 'a>, ExecError> {
        Ok(Box::new(MaterializedRangeCursor::new(self.scan(request)?)))
    }
}

/// Decorates a scanner with the read point allocated once for a SQL statement.
/// Retries and every participating range receive the same typed timestamp.
#[derive(Clone)]
pub struct TimestampedRangeScanner {
    inner: std::sync::Arc<dyn RangeScanner>,
    read_ts: crate::timestamp_txn::ReadTimestamp,
}

impl TimestampedRangeScanner {
    /// Build a statement-scoped scanner.
    #[must_use]
    pub fn new(
        inner: std::sync::Arc<dyn RangeScanner>,
        read_ts: crate::timestamp_txn::ReadTimestamp,
    ) -> Self {
        Self { inner, read_ts }
    }
}

impl RangeScanner for TimestampedRangeScanner {
    fn scan(&self, mut request: ScanRequest<'_>) -> Result<Vec<ScannedRow>, ExecError> {
        if request.table.sharded {
            request.read_ts = Some(self.read_ts);
        }
        self.inner.scan(request)
    }
}

/// Scanner used by default: reads only the local MVCC store.
#[derive(Debug, Default)]
pub struct LocalRangeScanner;

impl RangeScanner for LocalRangeScanner {
    fn scan(&self, request: ScanRequest<'_>) -> Result<Vec<ScannedRow>, ExecError> {
        if request.table.sharded {
            let read_ts = request.read_ts.ok_or_else(|| {
                ExecError::Unsupported(
                    "sharded scans require a finite statement read timestamp".into(),
                )
            })?;
            let rows = crate::exec::scan_ts_live_interval(
                request.local,
                request.global,
                request.table,
                read_ts,
                request.interval,
            )?;
            return apply_executable_scan_pushdown(
                rows,
                &request.predicate,
                &request.projection,
                request.partial_aggregate.as_ref(),
                request.top_k.as_ref(),
            );
        }
        let rows = crate::exec::scan_live_interval(
            request.local,
            request.global,
            request.global_snapshot,
            request.snapshot,
            request.own_xid,
            request.table,
            request.interval,
        )?;
        apply_executable_scan_pushdown(
            rows,
            &request.predicate,
            &request.projection,
            request.partial_aggregate.as_ref(),
            request.top_k.as_ref(),
        )
    }
}

/// Apply row-level pushdowns to visible rows returned by a backing scanner.
pub fn apply_scan_pushdown(
    rows: Vec<ScannedRow>,
    predicate: &PredicatePushdown,
    projection: &ProjectionPushdown,
) -> Result<Vec<ScannedRow>, ExecError> {
    rows.into_iter()
        .filter(|row| row_satisfies_predicate(&row.row, predicate))
        .map(|row| project_scanned_row(row, projection))
        .collect()
}

/// Apply all executable scanner pushdowns to visible rows returned by a backing scanner.
pub fn apply_executable_scan_pushdown(
    rows: Vec<ScannedRow>,
    predicate: &PredicatePushdown,
    projection: &ProjectionPushdown,
    partial_aggregate: Option<&PartialAggregateSpec>,
    top_k: Option<&TopKSpec>,
) -> Result<Vec<ScannedRow>, ExecError> {
    if partial_aggregate.is_some() && top_k.is_some() {
        return Err(ExecError::Unsupported(
            "range scanner cannot combine partial aggregate and top-k pushdown".into(),
        ));
    }
    if let Some(spec) = partial_aggregate {
        return apply_partial_aggregate_pushdown(rows, predicate, projection, spec);
    }
    if top_k.is_some() && *projection != ProjectionPushdown::All {
        return Err(ExecError::Unsupported(
            "top-k pushdown cannot be combined with projection pushdown".into(),
        ));
    }
    let mut rows = apply_scan_pushdown(rows, predicate, projection)?;
    if let Some(spec) = top_k {
        apply_top_k_pushdown(&mut rows, spec)?;
    }
    Ok(rows)
}

fn apply_partial_aggregate_pushdown(
    rows: Vec<ScannedRow>,
    predicate: &PredicatePushdown,
    projection: &ProjectionPushdown,
    spec: &PartialAggregateSpec,
) -> Result<Vec<ScannedRow>, ExecError> {
    if *projection != ProjectionPushdown::All {
        return Err(ExecError::Unsupported(
            "partial aggregate pushdown cannot be combined with projection pushdown".into(),
        ));
    }
    let rows = rows
        .into_iter()
        .filter(|row| row_satisfies_predicate(&row.row, predicate));
    let row = match spec.function {
        PartialAggregateFunction::AvgParts => compute_partial_avg_parts(rows, spec.column)?,
        _ => vec![compute_partial_aggregate_value(rows, spec)?],
    };
    Ok(vec![ScannedRow {
        rowid: 0,
        xmin: 0,
        row,
    }])
}

/// Merge per-range partial aggregate rows into one gateway-visible partial row.
pub fn merge_partial_aggregate_rows(
    rows: Vec<ScannedRow>,
    spec: &PartialAggregateSpec,
) -> Result<Vec<ScannedRow>, ExecError> {
    if spec.function == PartialAggregateFunction::AvgParts {
        let parts = merge_partial_avg_parts(rows)?;
        return Ok(vec![ScannedRow {
            rowid: 0,
            xmin: 0,
            row: parts,
        }]);
    }
    let partial_rows = rows.into_iter().map(|row| {
        let [value] = row.row.as_slice() else {
            return Err(ExecError::Unsupported(
                "remote partial aggregate returned an invalid row shape".into(),
            ));
        };
        Ok(ScannedRow {
            rowid: row.rowid,
            xmin: row.xmin,
            row: vec![value.clone()],
        })
    });
    let merge_spec = match spec.function {
        PartialAggregateFunction::Count => {
            let value = merge_partial_counts(partial_rows.collect::<Result<Vec<_>, _>>()?)?;
            return Ok(vec![ScannedRow {
                rowid: 0,
                xmin: 0,
                row: vec![value],
            }]);
        }
        PartialAggregateFunction::Sum
        | PartialAggregateFunction::Min
        | PartialAggregateFunction::Max => PartialAggregateSpec {
            function: spec.function,
            column: Some(0),
        },
        PartialAggregateFunction::AvgParts => unreachable!("AVG parts are handled above"),
    };
    let value = compute_partial_aggregate_value(
        partial_rows.collect::<Result<Vec<_>, _>>()?.into_iter(),
        &merge_spec,
    )?;
    Ok(vec![ScannedRow {
        rowid: 0,
        xmin: 0,
        row: vec![value],
    }])
}

/// Merge range-local aggregate state and produce the SQL-visible aggregate value.
pub fn finalize_partial_aggregate_rows(
    rows: Vec<ScannedRow>,
    spec: &PartialAggregateSpec,
) -> Result<Vec<ScannedRow>, ExecError> {
    let merged = merge_partial_aggregate_rows(rows, spec)?;
    if spec.function != PartialAggregateFunction::AvgParts {
        return Ok(merged);
    }
    let [row] = merged.as_slice() else {
        return Err(ExecError::Unsupported(
            "partial AVG pushdown returned no merged parts".into(),
        ));
    };
    Ok(vec![ScannedRow {
        rowid: 0,
        xmin: 0,
        row: vec![finalize_avg_parts(&row.row)?],
    }])
}

fn compute_partial_avg_parts(
    rows: impl Iterator<Item = ScannedRow>,
    column: Option<usize>,
) -> Result<Vec<crabka_pgtypes::Datum>, ExecError> {
    let Some(column) = column else {
        return Err(ExecError::Unsupported(
            "AVG(*) is not a supported aggregate".into(),
        ));
    };
    let mut sum = None;
    let mut count = 0_i64;
    for row in rows {
        let Some(value) = row.row.get(column) else {
            return Err(ExecError::Unsupported(format!(
                "partial AVG column {column} is outside the scanned row"
            )));
        };
        if value.is_null() {
            continue;
        }
        let value = avg_numeric_value(value)?;
        sum = Some(match sum {
            Some(current) => crabka_pgtypes::ops::add(&current, &value)?,
            None => value,
        });
        count = count
            .checked_add(1)
            .ok_or_else(|| ExecError::Unsupported("partial AVG count exceeds int8 range".into()))?;
    }
    Ok(vec![
        sum.unwrap_or(crabka_pgtypes::Datum::Null),
        crabka_pgtypes::Datum::Int8(count),
    ])
}

fn merge_partial_avg_parts(rows: Vec<ScannedRow>) -> Result<Vec<crabka_pgtypes::Datum>, ExecError> {
    let mut sum = None;
    let mut count = 0_i64;
    for row in rows {
        let [partial_sum, crabka_pgtypes::Datum::Int8(partial_count)] = row.row.as_slice() else {
            return Err(ExecError::Unsupported(
                "remote partial AVG returned an invalid parts shape".into(),
            ));
        };
        if *partial_count < 0 {
            return Err(ExecError::Unsupported(
                "remote partial AVG returned a negative count".into(),
            ));
        }
        if *partial_count == 0 {
            if !partial_sum.is_null() {
                return Err(ExecError::Unsupported(
                    "remote partial AVG returned a sum without input values".into(),
                ));
            }
            continue;
        }
        let partial_sum = avg_numeric_value(partial_sum)?;
        sum = Some(match sum {
            Some(current) => crabka_pgtypes::ops::add(&current, &partial_sum)?,
            None => partial_sum,
        });
        count = count.checked_add(*partial_count).ok_or_else(|| {
            ExecError::Unsupported("merged partial AVG count exceeds int8 range".into())
        })?;
    }
    Ok(vec![
        sum.unwrap_or(crabka_pgtypes::Datum::Null),
        crabka_pgtypes::Datum::Int8(count),
    ])
}

fn finalize_avg_parts(parts: &[crabka_pgtypes::Datum]) -> Result<crabka_pgtypes::Datum, ExecError> {
    let [sum, crabka_pgtypes::Datum::Int8(count)] = parts else {
        return Err(ExecError::Unsupported(
            "merged partial AVG returned an invalid parts shape".into(),
        ));
    };
    if *count == 0 {
        if sum.is_null() {
            return Ok(crabka_pgtypes::Datum::Null);
        }
        return Err(ExecError::Unsupported(
            "merged partial AVG returned a sum without input values".into(),
        ));
    }
    if *count < 0 {
        return Err(ExecError::Unsupported(
            "merged partial AVG returned a negative count".into(),
        ));
    }
    Ok(crabka_pgtypes::ops::div(
        &avg_numeric_value(sum)?,
        &crabka_pgtypes::Datum::Int8(*count),
    )?)
}

fn avg_numeric_value(value: &crabka_pgtypes::Datum) -> Result<crabka_pgtypes::Datum, ExecError> {
    use bigdecimal::BigDecimal;

    match value {
        crabka_pgtypes::Datum::Int4(value) => {
            Ok(crabka_pgtypes::Datum::Numeric(BigDecimal::from(*value)))
        }
        crabka_pgtypes::Datum::Int8(value) => {
            Ok(crabka_pgtypes::Datum::Numeric(BigDecimal::from(*value)))
        }
        crabka_pgtypes::Datum::Numeric(value) => Ok(crabka_pgtypes::Datum::Numeric(value.clone())),
        _ => Err(ExecError::Unsupported(
            "partial AVG pushdown supports only int4/int8/numeric inputs".into(),
        )),
    }
}

fn merge_partial_counts(rows: Vec<ScannedRow>) -> Result<crabka_pgtypes::Datum, ExecError> {
    let mut count = 0_i64;
    for row in rows {
        let [crabka_pgtypes::Datum::Int8(partial)] = row.row.as_slice() else {
            return Err(ExecError::Unsupported(
                "remote partial COUNT returned an invalid row shape".into(),
            ));
        };
        count = count.checked_add(*partial).ok_or_else(|| {
            ExecError::Unsupported("merged partial COUNT result exceeds int8 range".into())
        })?;
    }
    Ok(crabka_pgtypes::Datum::Int8(count))
}

fn compute_partial_aggregate_value(
    rows: impl Iterator<Item = ScannedRow>,
    spec: &PartialAggregateSpec,
) -> Result<crabka_pgtypes::Datum, ExecError> {
    match spec.function {
        PartialAggregateFunction::Count => compute_partial_count(rows, spec.column),
        PartialAggregateFunction::Sum => compute_partial_sum(rows, spec.column),
        PartialAggregateFunction::Min => {
            compute_partial_min_max(rows, spec.column, PartialAggregateFunction::Min)
        }
        PartialAggregateFunction::Max => {
            compute_partial_min_max(rows, spec.column, PartialAggregateFunction::Max)
        }
        PartialAggregateFunction::AvgParts => unreachable!("AVG parts are handled separately"),
    }
}

fn compute_partial_count(
    rows: impl Iterator<Item = ScannedRow>,
    column: Option<usize>,
) -> Result<crabka_pgtypes::Datum, ExecError> {
    let mut count = 0_i64;
    for row in rows {
        if column.is_some_and(|column| {
            row.row
                .get(column)
                .is_none_or(crabka_pgtypes::Datum::is_null)
        }) {
            continue;
        }
        count = count.checked_add(1).ok_or_else(|| {
            ExecError::Unsupported("partial COUNT result exceeds int8 range".into())
        })?;
    }
    Ok(crabka_pgtypes::Datum::Int8(count))
}

fn compute_partial_sum(
    rows: impl Iterator<Item = ScannedRow>,
    column: Option<usize>,
) -> Result<crabka_pgtypes::Datum, ExecError> {
    let Some(column) = column else {
        return Err(ExecError::Unsupported(
            "SUM(*) is not a supported aggregate".into(),
        ));
    };
    let mut acc: Option<PartialSum> = None;
    for row in rows {
        let Some(value) = row.row.get(column) else {
            return Err(ExecError::Unsupported(format!(
                "partial SUM column {column} is outside the scanned row"
            )));
        };
        if value.is_null() {
            continue;
        }
        match acc.as_mut() {
            Some(acc) => acc.add(value)?,
            None => acc = Some(PartialSum::try_from_first(value)?),
        }
    }
    Ok(acc.map_or(crabka_pgtypes::Datum::Null, PartialSum::finish))
}

fn compute_partial_min_max(
    rows: impl Iterator<Item = ScannedRow>,
    column: Option<usize>,
    function: PartialAggregateFunction,
) -> Result<crabka_pgtypes::Datum, ExecError> {
    let Some(column) = column else {
        return Err(ExecError::Unsupported(
            "MIN/MAX(*) is not a supported aggregate".into(),
        ));
    };
    let mut best: Option<crabka_pgtypes::Datum> = None;
    for row in rows {
        let Some(value) = row.row.get(column) else {
            return Err(ExecError::Unsupported(format!(
                "partial MIN/MAX column {column} is outside the scanned row"
            )));
        };
        if value.is_null() {
            continue;
        }
        ensure_partial_min_max_value_is_supported(value)?;
        let should_take = match best.as_ref() {
            None => true,
            Some(current) => {
                let order = crabka_pgtypes::ops::compare(value, current)?;
                matches!(
                    (function, order),
                    (PartialAggregateFunction::Min, Some(Ordering::Less))
                        | (PartialAggregateFunction::Max, Some(Ordering::Greater))
                )
            }
        };
        if should_take {
            best = Some(value.clone());
        }
    }
    Ok(best.unwrap_or(crabka_pgtypes::Datum::Null))
}

enum PartialSum {
    Int(i64),
    Float(f64),
    Numeric(crabka_pgtypes::Datum),
}

impl PartialSum {
    fn try_from_first(value: &crabka_pgtypes::Datum) -> Result<Self, ExecError> {
        match value {
            crabka_pgtypes::Datum::Int4(value) => Ok(Self::Int(i64::from(*value))),
            crabka_pgtypes::Datum::Int8(value) => Ok(Self::Int(*value)),
            crabka_pgtypes::Datum::Float8(value) => Ok(Self::Float(*value)),
            crabka_pgtypes::Datum::Numeric(_) => Ok(Self::Numeric(value.clone())),
            _ => Err(ExecError::Unsupported(
                "partial SUM pushdown supports only int4/int8/float8/numeric inputs".into(),
            )),
        }
    }

    fn add(&mut self, value: &crabka_pgtypes::Datum) -> Result<(), ExecError> {
        match (self, value) {
            (Self::Int(acc), crabka_pgtypes::Datum::Int4(value)) => {
                *acc = acc
                    .checked_add(i64::from(*value))
                    .ok_or(ExecError::Type(crabka_pgtypes::TypeError::Overflow))?;
            }
            (Self::Int(acc), crabka_pgtypes::Datum::Int8(value)) => {
                *acc = acc
                    .checked_add(*value)
                    .ok_or(ExecError::Type(crabka_pgtypes::TypeError::Overflow))?;
            }
            (Self::Float(acc), crabka_pgtypes::Datum::Float8(value)) => *acc += *value,
            (Self::Numeric(acc), crabka_pgtypes::Datum::Numeric(_)) => {
                *acc = crabka_pgtypes::ops::add(acc, value)?;
            }
            _ => {
                return Err(ExecError::Unsupported(
                    "partial SUM pushdown requires one homogeneous supported input type".into(),
                ));
            }
        }
        Ok(())
    }

    fn finish(self) -> crabka_pgtypes::Datum {
        match self {
            Self::Int(value) => crabka_pgtypes::Datum::Int8(value),
            Self::Float(value) => crabka_pgtypes::Datum::Float8(value),
            Self::Numeric(value) => value,
        }
    }
}

fn ensure_partial_min_max_value_is_supported(
    value: &crabka_pgtypes::Datum,
) -> Result<(), ExecError> {
    if matches!(
        value,
        crabka_pgtypes::Datum::Int4(_)
            | crabka_pgtypes::Datum::Int8(_)
            | crabka_pgtypes::Datum::Float8(_)
            | crabka_pgtypes::Datum::Numeric(_)
            | crabka_pgtypes::Datum::Text(_)
            | crabka_pgtypes::Datum::Bool(_)
            | crabka_pgtypes::Datum::Date(_)
            | crabka_pgtypes::Datum::Time(_)
            | crabka_pgtypes::Datum::Timestamp(_)
            | crabka_pgtypes::Datum::Timestamptz(_)
            | crabka_pgtypes::Datum::Interval(_)
    ) {
        return Ok(());
    }
    Err(ExecError::Unsupported(
        "partial MIN/MAX pushdown does not support this input type".into(),
    ))
}

/// Apply a supported per-range top-K request in place.
pub fn apply_top_k_pushdown(rows: &mut Vec<ScannedRow>, spec: &TopKSpec) -> Result<(), ExecError> {
    if spec.limit == 0 {
        rows.clear();
        return Ok(());
    }
    if spec.order_by.is_empty() {
        return Err(ExecError::Unsupported(
            "top-k pushdown requires at least one ORDER BY column".into(),
        ));
    }
    for order_by in &spec.order_by {
        ensure_top_k_column_is_supported(rows, order_by.column)?;
    }
    rows.sort_by(|left, right| compare_top_k_rows(left, right, &spec.order_by));
    let limit = usize::try_from(spec.limit).unwrap_or(usize::MAX);
    rows.truncate(limit);
    Ok(())
}

fn ensure_top_k_column_is_supported(rows: &[ScannedRow], column: usize) -> Result<(), ExecError> {
    let mut expected_type = None;
    for row in rows {
        let Some(value) = row.row.get(column) else {
            return Err(ExecError::Unsupported(format!(
                "top-k pushdown column {column} is outside the scanned row"
            )));
        };
        let value_type = top_k_value_type(value)?;
        if expected_type.is_none() {
            expected_type = Some(value_type);
            continue;
        }
        if expected_type == Some(value_type) {
            continue;
        }
        return Err(ExecError::Unsupported(
            "top-k pushdown order keys must have one homogeneous type".into(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TopKValueType {
    Int4,
    Int8,
    Text,
}

fn top_k_value_type(value: &crabka_pgtypes::Datum) -> Result<TopKValueType, ExecError> {
    match value {
        crabka_pgtypes::Datum::Int4(_) => Ok(TopKValueType::Int4),
        crabka_pgtypes::Datum::Int8(_) => Ok(TopKValueType::Int8),
        crabka_pgtypes::Datum::Text(_) => Ok(TopKValueType::Text),
        _ => Err(ExecError::Unsupported(
            "top-k pushdown supports only non-null int4/int8/text order keys".into(),
        )),
    }
}

fn compare_top_k_rows(left: &ScannedRow, right: &ScannedRow, order_by: &[TopKColumn]) -> Ordering {
    for key in order_by {
        let key_order = compare_top_k_values(&left.row[key.column], &right.row[key.column]);
        let key_order = if key.asc {
            key_order
        } else {
            key_order.reverse()
        };
        if !key_order.is_eq() {
            return key_order;
        }
    }
    left.rowid
        .cmp(&right.rowid)
        .then_with(|| left.xmin.cmp(&right.xmin))
}

fn compare_top_k_values(left: &crabka_pgtypes::Datum, right: &crabka_pgtypes::Datum) -> Ordering {
    use crabka_pgtypes::Datum;
    match (left, right) {
        (Datum::Int4(left), Datum::Int4(right)) => left.cmp(right),
        (Datum::Int8(left), Datum::Int8(right)) => left.cmp(right),
        (Datum::Text(left), Datum::Text(right)) => left.cmp(right),
        _ => Ordering::Equal,
    }
}

fn row_satisfies_predicate(row: &[crabka_pgtypes::Datum], predicate: &PredicatePushdown) -> bool {
    match predicate {
        PredicatePushdown::FullScan => true,
        PredicatePushdown::Conjunctive(predicates) => predicates.iter().all(|predicate| {
            row.get(predicate.column)
                .is_some_and(|datum| compare_datums(datum, predicate.op, &predicate.value))
        }),
    }
}

fn compare_datums(
    left: &crabka_pgtypes::Datum,
    op: PredicateOp,
    right: &crabka_pgtypes::Datum,
) -> bool {
    use crabka_pgtypes::Datum;
    match (left, right) {
        (Datum::Int4(left), Datum::Int4(right)) => compare_order(left.cmp(right), op),
        (Datum::Int8(left), Datum::Int8(right)) => compare_order(left.cmp(right), op),
        (Datum::Text(left), Datum::Text(right)) => compare_order(left.cmp(right), op),
        (Datum::Bool(left), Datum::Bool(right)) => compare_order(left.cmp(right), op),
        _ => false,
    }
}

fn compare_order(ordering: std::cmp::Ordering, op: PredicateOp) -> bool {
    match op {
        PredicateOp::Eq => ordering.is_eq(),
        PredicateOp::Lt => ordering.is_lt(),
        PredicateOp::Le => ordering.is_le(),
        PredicateOp::Gt => ordering.is_gt(),
        PredicateOp::Ge => ordering.is_ge(),
    }
}

fn project_scanned_row(
    row: ScannedRow,
    projection: &ProjectionPushdown,
) -> Result<ScannedRow, ExecError> {
    let ProjectionPushdown::Columns(columns) = projection else {
        return Ok(row);
    };
    let projected = columns
        .iter()
        .map(|column| {
            row.row.get(*column).cloned().ok_or_else(|| {
                ExecError::Unsupported(format!(
                    "projection pushdown column {column} is outside the scanned row"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ScannedRow {
        row: projected,
        ..row
    })
}

#[cfg(test)]
mod cursor_contract_tests {
    use super::{MaterializedRangeCursor, RangeCursor};
    use crabka_pgtypes::Datum;

    fn row(rowid: u64) -> super::ScannedRow {
        super::ScannedRow {
            rowid,
            xmin: 1,
            row: vec![Datum::Int8(i64::try_from(rowid).expect("test rowid fits"))],
        }
    }

    #[tokio::test]
    async fn materialized_adapter_returns_only_the_requested_page() {
        let mut cursor = MaterializedRangeCursor::new(vec![row(1), row(2), row(3)]);

        let first = cursor.next_page(2).await.expect("first page succeeds");
        assert_eq!(first.rows.len(), 2);
        assert!(!first.is_last);
        let second = cursor.next_page(2).await.expect("second page succeeds");
        assert_eq!(second.rows.len(), 1);
        assert!(second.is_last);
    }

    #[tokio::test]
    async fn zero_page_size_is_rejected_without_consuming_rows() {
        let mut cursor = MaterializedRangeCursor::new(vec![row(1)]);

        assert!(cursor.next_page(0).await.is_err());
        let page = cursor.next_page(1).await.expect("valid page succeeds");
        assert_eq!(page.rows[0].rowid, 1);
    }
}
