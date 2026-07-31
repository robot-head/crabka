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
    /// Zero-based source columns forming the group key, in SQL key order.
    pub group_by: Vec<usize>,
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
        Some(Self {
            function,
            column,
            group_by: Vec::new(),
        })
    }

    #[must_use]
    pub fn grouped_by(mut self, columns: Vec<usize>) -> Self {
        self.group_by = columns;
        self
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
    /// Optional timestamp transaction whose pending intents belong to this reader.
    pub own_start_ts: Option<crate::timestamp_txn::TimestampTransactionId>,
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
        let rows = self
            .rows
            .drain(..take)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(ScanPage {
            rows,
            is_last: self.rows.is_empty(),
        })
    }
}

pub const MAX_JOIN_KEY_COLUMNS: usize = 16;
pub const MAX_JOIN_PROJECTION_COLUMNS: usize = 256;
pub const MAX_JOIN_PREDICATES: usize = 256;
pub const MAX_JOIN_SNAPSHOT_XIDS: usize = 65_536;
pub const MAX_JOIN_BROADCAST_ROWS: usize = 8_192;
pub const MAX_JOIN_ROW_BYTES: usize = 256 * 1024;
pub const MAX_JOIN_RESULT_ROWS: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinExecutionStrategy {
    BroadcastLeft,
    BroadcastRight,
    CoPartitioned,
    Gather,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinSnapshot {
    pub xmin: u64,
    pub xmax: u64,
    pub xip: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinTableInterval {
    pub table_id: u64,
    pub interval: RowInterval,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct JoinRow {
    /// Deterministic tuple encoding; values are ordered by the request projection.
    pub tuple: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinRangeRequest {
    pub local_snapshot: JoinSnapshot,
    pub global_snapshot: JoinSnapshot,
    pub read_ts: u64,
    pub own_xid: Option<u64>,
    pub own_start_ts: Option<u64>,
    pub kind: JoinKind,
    pub left_keys: Vec<usize>,
    pub right_keys: Vec<usize>,
    pub strategy: JoinExecutionStrategy,
    pub left: JoinTableInterval,
    pub right: JoinTableInterval,
    pub broadcast_rows: Option<Vec<JoinRow>>,
    pub left_filter: PredicatePushdown,
    pub right_filter: PredicatePushdown,
    pub projection: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinRangeResult {
    pub rows: Vec<JoinRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JoinValidationError {
    #[error("join read timestamp must be nonzero")]
    MissingReadTimestamp,
    #[error("join key lists must be nonempty and have equal length")]
    InvalidJoinKeys,
    #[error("join key count {actual} exceeds limit {limit}")]
    TooManyJoinKeys { actual: usize, limit: usize },
    #[error("join projection count {actual} exceeds limit {limit}")]
    TooManyProjectionColumns { actual: usize, limit: usize },
    #[error("join predicate count {actual} exceeds limit {limit}")]
    TooManyPredicates { actual: usize, limit: usize },
    #[error("join snapshot xid count {actual} exceeds limit {limit}")]
    TooManySnapshotXids { actual: usize, limit: usize },
    #[error("broadcast row count {actual} exceeds limit {limit}")]
    TooManyBroadcastRows { actual: usize, limit: usize },
    #[error("join row byte count {actual} exceeds limit {limit}")]
    JoinRowTooLarge { actual: usize, limit: usize },
    #[error("join result row count {actual} exceeds limit {limit}")]
    TooManyResultRows { actual: usize, limit: usize },
    #[error("broadcast rows do not match the selected strategy")]
    InvalidBroadcastRows,
    #[error("join table identity is invalid")]
    InvalidTableIdentity,
    #[error("join interval is invalid")]
    InvalidInterval,
}

impl JoinRangeRequest {
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn validate(&self) -> Result<(), JoinValidationError> {
        if self.read_ts == 0 {
            return Err(JoinValidationError::MissingReadTimestamp);
        }
        if self.left_keys.is_empty() || self.left_keys.len() != self.right_keys.len() {
            return Err(JoinValidationError::InvalidJoinKeys);
        }
        bound(
            self.left_keys.len(),
            MAX_JOIN_KEY_COLUMNS,
            |actual, limit| JoinValidationError::TooManyJoinKeys { actual, limit },
        )?;
        bound(
            self.projection.len(),
            MAX_JOIN_PROJECTION_COLUMNS,
            |actual, limit| JoinValidationError::TooManyProjectionColumns { actual, limit },
        )?;
        for snapshot in [&self.local_snapshot, &self.global_snapshot] {
            bound(
                snapshot.xip.len(),
                MAX_JOIN_SNAPSHOT_XIDS,
                |actual, limit| JoinValidationError::TooManySnapshotXids { actual, limit },
            )?;
            if snapshot.xmin > snapshot.xmax
                || !snapshot.xip.windows(2).all(|pair| pair[0] < pair[1])
            {
                return Err(JoinValidationError::InvalidTableIdentity);
            }
        }
        for predicate in [&self.left_filter, &self.right_filter] {
            let count = match predicate {
                PredicatePushdown::FullScan => 0,
                PredicatePushdown::Conjunctive(items) => items.len(),
            };
            bound(count, MAX_JOIN_PREDICATES, |actual, limit| {
                JoinValidationError::TooManyPredicates { actual, limit }
            })?;
        }
        for table in [&self.left, &self.right] {
            if table.table_id == 0 {
                return Err(JoinValidationError::InvalidTableIdentity);
            }
            if matches!((table.interval.start, table.interval.end), (Some(start), Some(end)) if start >= end)
            {
                return Err(JoinValidationError::InvalidInterval);
            }
        }
        let expects_broadcast = matches!(
            self.strategy,
            JoinExecutionStrategy::BroadcastLeft | JoinExecutionStrategy::BroadcastRight
        );
        if expects_broadcast != self.broadcast_rows.is_some() {
            return Err(JoinValidationError::InvalidBroadcastRows);
        }
        if let Some(rows) = &self.broadcast_rows {
            bound(rows.len(), MAX_JOIN_BROADCAST_ROWS, |actual, limit| {
                JoinValidationError::TooManyBroadcastRows { actual, limit }
            })?;
            validate_join_rows(rows)?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn test_fixture() -> Self {
        let snapshot = JoinSnapshot {
            xmin: 1,
            xmax: 3,
            xip: vec![2],
        };
        Self {
            local_snapshot: snapshot.clone(),
            global_snapshot: snapshot,
            read_ts: 7,
            own_xid: None,
            own_start_ts: None,
            kind: JoinKind::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            strategy: JoinExecutionStrategy::BroadcastRight,
            left: JoinTableInterval {
                table_id: 1,
                interval: RowInterval::ALL,
            },
            right: JoinTableInterval {
                table_id: 2,
                interval: RowInterval::ALL,
            },
            broadcast_rows: Some(vec![]),
            left_filter: PredicatePushdown::FullScan,
            right_filter: PredicatePushdown::FullScan,
            projection: vec![0],
        }
    }
}

impl JoinRangeResult {
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn validate(&self) -> Result<(), JoinValidationError> {
        bound(self.rows.len(), MAX_JOIN_RESULT_ROWS, |actual, limit| {
            JoinValidationError::TooManyResultRows { actual, limit }
        })?;
        validate_join_rows(&self.rows)
    }
}

/// Execute an equi-join over bounded, encoded visible rows.
///
/// Both inputs contain complete table tuples. Side predicates are evaluated
/// before joining, projection indexes address the concatenated `[left, right]`
/// row, and SQL NULL keys never compare equal.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn execute_materialized_join(
    request: &JoinRangeRequest,
    left: &[JoinRow],
    right: &[JoinRow],
) -> Result<JoinRangeResult, ExecError> {
    request
        .validate()
        .map_err(|error| ExecError::Unsupported(error.to_string()))?;
    if request.kind != JoinKind::Inner {
        return Err(ExecError::Unsupported(
            "distributed owner execution currently supports inner joins only".into(),
        ));
    }
    let left = decode_join_input(left, &request.left_filter)?;
    let right = decode_join_input(right, &request.right_filter)?;
    let mut rows = Vec::new();
    for left_row in &left {
        for right_row in &right {
            if !join_keys_equal(left_row, right_row, &request.left_keys, &request.right_keys)? {
                continue;
            }
            if rows.len() == MAX_JOIN_RESULT_ROWS {
                return Err(ExecError::Unsupported(
                    JoinValidationError::TooManyResultRows {
                        actual: MAX_JOIN_RESULT_ROWS + 1,
                        limit: MAX_JOIN_RESULT_ROWS,
                    }
                    .to_string(),
                ));
            }
            let joined = left_row
                .iter()
                .chain(right_row)
                .cloned()
                .collect::<Vec<_>>();
            let projected = if request.projection.is_empty() {
                joined
            } else {
                request
                    .projection
                    .iter()
                    .map(|&column| {
                        joined.get(column).cloned().ok_or_else(|| {
                            ExecError::Unsupported(format!(
                                "join projection column {column} is outside the joined row"
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?
            };
            rows.push(JoinRow {
                tuple: crabka_pgmvcc::version::encode_tuple(0, 0, &projected),
            });
        }
    }
    rows.sort_by(|left, right| left.tuple.cmp(&right.tuple));
    let result = JoinRangeResult { rows };
    result
        .validate()
        .map_err(|error| ExecError::Unsupported(error.to_string()))?;
    Ok(result)
}

fn decode_join_input(
    rows: &[JoinRow],
    predicate: &PredicatePushdown,
) -> Result<Vec<Vec<crabka_pgtypes::Datum>>, ExecError> {
    rows.iter()
        .map(|row| crabka_pgmvcc::version::decode_tuple(&row.tuple).map(|(_, _, row)| row))
        .filter_map(|row| match row {
            Ok(row) if row_satisfies_predicate(&row, predicate) => Some(Ok(row)),
            Ok(_) => None,
            Err(error) => Some(Err(error.into())),
        })
        .collect()
}

fn join_keys_equal(
    left: &[crabka_pgtypes::Datum],
    right: &[crabka_pgtypes::Datum],
    left_keys: &[usize],
    right_keys: &[usize],
) -> Result<bool, ExecError> {
    for (&left_key, &right_key) in left_keys.iter().zip(right_keys) {
        let left = left.get(left_key).ok_or_else(|| {
            ExecError::Unsupported(format!("left join key {left_key} is outside the row"))
        })?;
        let right = right.get(right_key).ok_or_else(|| {
            ExecError::Unsupported(format!("right join key {right_key} is outside the row"))
        })?;
        if matches!(left, crabka_pgtypes::Datum::Null)
            || matches!(right, crabka_pgtypes::Datum::Null)
            || left != right
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn validate_join_rows(rows: &[JoinRow]) -> Result<(), JoinValidationError> {
    for row in rows {
        bound(row.tuple.len(), MAX_JOIN_ROW_BYTES, |actual, limit| {
            JoinValidationError::JoinRowTooLarge { actual, limit }
        })?;
    }
    Ok(())
}

fn bound<E>(actual: usize, limit: usize, error: impl FnOnce(usize, usize) -> E) -> Result<(), E> {
    if actual > limit {
        Err(error(actual, limit))
    } else {
        Ok(())
    }
}

/// Seam for local or scatter-gather table scans.
pub trait RangeScanner: Send + Sync + 'static {
    /// Return visible rows in deterministic `(rowid, xmin)` table order.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn scan(&self, request: ScanRequest<'_>) -> Result<Vec<ScannedRow>, ExecError>;

    /// Execute a validated distributed join fragment. Implementations which do
    /// not support owner-side joins fail explicitly and never synthesize rows.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn join(&self, request: JoinRangeRequest) -> Result<JoinRangeResult, ExecError> {
        request
            .validate()
            .map_err(|error| ExecError::Unsupported(error.to_string()))?;
        Err(ExecError::Unsupported(
            "distributed join execution is not implemented by this range scanner".into(),
        ))
    }

    fn join_strategy(
        &self,
        _left: &crabka_pgcatalog::Table,
        _right: &crabka_pgcatalog::Table,
    ) -> crate::plan_dist::JoinStrategy {
        crate::plan_dist::JoinStrategy::Gather
    }

    fn join_strategy_for_keys(
        &self,
        left: &crabka_pgcatalog::Table,
        right: &crabka_pgcatalog::Table,
        _left_keys: &[usize],
        _right_keys: &[usize],
    ) -> crate::plan_dist::JoinStrategy {
        self.join_strategy(left, right)
    }

    /// Open a pull-based cursor. The default is a compatibility adapter which
    /// materializes through [`RangeScanner::scan`].
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn scan_cursor<'a>(
        &'a self,
        request: ScanRequest<'a>,
    ) -> Result<Box<dyn RangeCursor + 'a>, ExecError> {
        Ok(Box::new(MaterializedRangeCursor::new(self.scan(request)?)))
    }
}

#[cfg(test)]
mod join_protocol_tests {
    use super::*;

    fn encoded(row: &[crabka_pgtypes::Datum]) -> JoinRow {
        JoinRow {
            tuple: crabka_pgmvcc::version::encode_tuple(1, 0, row),
        }
    }

    fn decoded(rows: JoinRangeResult) -> Vec<Vec<crabka_pgtypes::Datum>> {
        rows.rows
            .into_iter()
            .map(|row| crabka_pgmvcc::version::decode_tuple(&row.tuple).unwrap().2)
            .collect()
    }

    #[test]
    fn materialized_inner_join_has_sql_null_filter_projection_and_order_semantics() {
        use crabka_pgtypes::Datum::{Int4, Null};
        let mut request = JoinRangeRequest::test_fixture();
        request.strategy = JoinExecutionStrategy::Gather;
        request.broadcast_rows = None;
        request.left_filter = PredicatePushdown::Conjunctive(vec![ColumnPredicate {
            column: 1,
            op: PredicateOp::Gt,
            value: Int4(10),
        }]);
        request.projection = vec![3, 1, 0];
        let left = vec![
            encoded(&[Int4(2), Int4(30)]),
            encoded(&[Null, Int4(99)]),
            encoded(&[Int4(1), Int4(5)]),
            encoded(&[Int4(1), Int4(20)]),
        ];
        let right = vec![
            encoded(&[Int4(1), Int4(101)]),
            encoded(&[Null, Int4(999)]),
            encoded(&[Int4(2), Int4(202)]),
            encoded(&[Int4(1), Int4(100)]),
        ];

        let actual = execute_materialized_join(&request, &left, &right).unwrap();

        assert_eq!(
            decoded(actual),
            vec![
                vec![Int4(100), Int4(20), Int4(1)],
                vec![Int4(101), Int4(20), Int4(1)],
                vec![Int4(202), Int4(30), Int4(2)],
            ]
        );
    }

    #[test]
    fn materialized_join_rejects_result_over_bound() {
        let mut request = JoinRangeRequest::test_fixture();
        request.strategy = JoinExecutionStrategy::Gather;
        request.broadcast_rows = None;
        let left = vec![encoded(&[crabka_pgtypes::Datum::Int4(1)]); 257];
        let right = vec![encoded(&[crabka_pgtypes::Datum::Int4(1)]); 256];

        let error = execute_materialized_join(&request, &left, &right).unwrap_err();

        assert!(
            matches!(error, ExecError::Unsupported(message) if message.contains("join result row count"))
        );
    }

    #[test]
    fn randomized_materialized_strategies_equal_gathered_reference() {
        use crabka_pgtypes::Datum::{Int4, Null};
        let mut state = 0x9e37_79b9_u64;
        for _seed in 0..64 {
            let mut next = || {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state
            };
            let left = (0..24)
                .map(|_| {
                    let value = next();
                    encoded(&[
                        if value % 7 == 0 {
                            Null
                        } else {
                            Int4((value % 9) as i32)
                        },
                        Int4((value >> 8) as i32),
                    ])
                })
                .collect::<Vec<_>>();
            let right = (0..19)
                .map(|_| {
                    let value = next();
                    encoded(&[
                        if value % 5 == 0 {
                            Null
                        } else {
                            Int4((value % 9) as i32)
                        },
                        Int4((value >> 8) as i32),
                    ])
                })
                .collect::<Vec<_>>();
            let mut request = JoinRangeRequest::test_fixture();
            request.projection = vec![0, 1, 3];
            request.strategy = JoinExecutionStrategy::Gather;
            request.broadcast_rows = None;
            let expected = execute_materialized_join(&request, &left, &right).unwrap();
            for strategy in [
                JoinExecutionStrategy::BroadcastLeft,
                JoinExecutionStrategy::BroadcastRight,
                JoinExecutionStrategy::CoPartitioned,
            ] {
                request.strategy = strategy;
                request.broadcast_rows = matches!(
                    strategy,
                    JoinExecutionStrategy::BroadcastLeft | JoinExecutionStrategy::BroadcastRight
                )
                .then(Vec::new);
                assert_eq!(
                    execute_materialized_join(&request, &left, &right).unwrap(),
                    expected
                );
            }
        }
    }

    #[test]
    fn join_request_rejects_unbounded_broadcast_rows() {
        let mut request = JoinRangeRequest::test_fixture();
        request.broadcast_rows = Some(vec![JoinRow::default(); MAX_JOIN_BROADCAST_ROWS + 1]);

        assert_eq!(
            request.validate(),
            Err(JoinValidationError::TooManyBroadcastRows {
                actual: MAX_JOIN_BROADCAST_ROWS + 1,
                limit: MAX_JOIN_BROADCAST_ROWS,
            })
        );
    }

    #[test]
    fn scanner_fake_receives_the_whole_join_request() {
        #[derive(Default)]
        struct Fake(std::sync::Mutex<Option<JoinRangeRequest>>);
        impl RangeScanner for Fake {
            fn scan(&self, _request: ScanRequest<'_>) -> Result<Vec<ScannedRow>, ExecError> {
                unreachable!()
            }
            fn join(&self, request: JoinRangeRequest) -> Result<JoinRangeResult, ExecError> {
                request
                    .validate()
                    .map_err(|error| ExecError::Unsupported(error.to_string()))?;
                *self.0.lock().expect("fake mutex") = Some(request);
                Ok(JoinRangeResult { rows: vec![] })
            }
        }
        let fake = Fake::default();
        let request = JoinRangeRequest::test_fixture();

        fake.join(request.clone()).expect("fake join");

        assert_eq!(*fake.0.lock().expect("fake mutex"), Some(request));
    }
}

/// Default cap for rows retained by a blocking executor fallback.
pub const BLOCKING_QUERY_MEMORY_BYTES: usize = 16 * 1024 * 1024;

/// Collect a cursor for a blocking operator while charging one central byte budget.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn collect_cursor_bounded(
    scanner: &dyn RangeScanner,
    request: ScanRequest<'_>,
    max_bytes: usize,
) -> Result<Vec<ScannedRow>, ExecError> {
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                let mut cursor = scanner.scan_cursor(request)?;
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| ExecError::Unsupported(error.to_string()))?;
                runtime.block_on(async move {
                    let mut rows = Vec::new();
                    let mut used = 0usize;
                    loop {
                        let page = cursor.next_page(1024).await?;
                        for row in page.rows {
                            let bytes = scanned_row_bytes(&row);
                            if used.saturating_add(bytes) > max_bytes {
                                return Err(memory_budget_exceeded());
                            }
                            used += bytes;
                            rows.push(row);
                        }
                        if page.is_last {
                            return Ok(rows);
                        }
                    }
                })
            })
            .join()
            .map_err(|_| ExecError::Unsupported("blocking cursor worker panicked".into()))?
    })
}

/// Stream a scan into per-spec partial aggregate states, one cursor page at a
/// time, so a supported aggregate never materializes the whole table.
///
/// The cursor applies the request predicate before pages arrive, so every page
/// row already satisfies the WHERE fragment. Peak retained memory is one page
/// plus the accumulated aggregate states; `max_bytes` bounds each of those
/// independently instead of the whole scanned result, which is what lets a
/// scalar aggregate run over tables far larger than the blocking-query budget
/// while a grouped aggregate with too many distinct keys still fails closed.
///
/// Returns one pre-finalize partial row set per spec, in `specs` order; callers
/// finish with [`finalize_partial_aggregate_rows`].
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub(crate) fn collect_partial_aggregates_bounded(
    scanner: &dyn RangeScanner,
    request: ScanRequest<'_>,
    specs: &[PartialAggregateSpec],
    max_bytes: usize,
) -> Result<Vec<Vec<ScannedRow>>, ExecError> {
    if specs.is_empty() {
        return Err(ExecError::Unsupported(
            "partial aggregate streaming requires at least one aggregate".into(),
        ));
    }
    if request.partial_aggregate.is_some()
        || request.top_k.is_some()
        || request.projection != ProjectionPushdown::All
    {
        return Err(ExecError::Unsupported(
            "partial aggregate streaming owns the scan pushdown contract".into(),
        ));
    }
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                let mut cursor = scanner.scan_cursor(request)?;
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| ExecError::Unsupported(error.to_string()))?;
                runtime.block_on(async move {
                    let mut states = vec![Vec::new(); specs.len()];
                    loop {
                        let page = cursor.next_page(1024).await?;
                        let is_last = page.is_last;
                        let rows = page.rows.into_vec();
                        if scanned_rows_bytes(rows.iter()) > max_bytes {
                            return Err(memory_budget_exceeded());
                        }
                        fold_page_into_states(rows, specs, &mut states)?;
                        if scanned_rows_bytes(states.iter().flatten()) > max_bytes {
                            return Err(memory_budget_exceeded());
                        }
                        if is_last {
                            return Ok(states);
                        }
                    }
                })
            })
            .join()
            .map_err(|_| ExecError::Unsupported("blocking cursor worker panicked".into()))?
    })
}

/// Fold one predicate-filtered page into every spec's running partial state.
fn fold_page_into_states(
    rows: Vec<ScannedRow>,
    specs: &[PartialAggregateSpec],
    states: &mut [Vec<ScannedRow>],
) -> Result<(), ExecError> {
    let mut rows = Some(rows);
    for (index, (state, spec)) in states.iter_mut().zip(specs).enumerate() {
        let page_rows = if index + 1 == specs.len() {
            rows.take()
                .expect("page rows are consumed only by the last spec")
        } else {
            rows.as_ref()
                .expect("page rows are consumed only by the last spec")
                .clone()
        };
        let mut merged = std::mem::take(state);
        merged.extend(apply_executable_scan_pushdown(
            page_rows,
            &PredicatePushdown::FullScan,
            &ProjectionPushdown::All,
            Some(spec),
            None,
        )?);
        *state = merge_partial_aggregate_rows(merged, spec)?;
    }
    Ok(())
}

fn scanned_rows_bytes<'a>(rows: impl Iterator<Item = &'a ScannedRow>) -> usize {
    rows.fold(0usize, |bytes, row| {
        bytes.saturating_add(scanned_row_bytes(row))
    })
}

pub(crate) fn datum_row_bytes(row: &[crabka_pgtypes::Datum]) -> usize {
    row.iter().fold(0usize, |bytes, datum| {
        let variable = match datum {
            crabka_pgtypes::Datum::Text(value) => value.len(),
            crabka_pgtypes::Datum::Bytea(value) => value.len(),
            crabka_pgtypes::Datum::Numeric(value) => value.to_string().len(),
            _ => 0,
        };
        bytes
            .saturating_add(std::mem::size_of::<crabka_pgtypes::Datum>())
            .saturating_add(variable)
    })
}

fn scanned_row_bytes(row: &ScannedRow) -> usize {
    std::mem::size_of::<ScannedRow>().saturating_add(datum_row_bytes(&row.row))
}

pub(crate) fn memory_budget_exceeded() -> ExecError {
    ExecError::Remote(crabka_pgwire::error::PgError::error(
        "53200",
        "blocking query exceeded the memory budget",
    ))
}

/// Decorates a scanner with the read point allocated once for a SQL statement.
/// Retries and every participating range receive the same typed timestamp.
#[derive(Clone)]
pub struct TimestampedRangeScanner {
    inner: std::sync::Arc<dyn RangeScanner>,
    read_ts: crate::timestamp_txn::ReadTimestamp,
    own_start_ts: Option<crate::timestamp_txn::TimestampTransactionId>,
    join_planner: Option<(
        std::sync::Arc<dyn crate::plan_dist::Stats>,
        crate::plan_dist::PlannerConfig,
    )>,
}

impl TimestampedRangeScanner {
    /// Build a statement-scoped scanner.
    #[must_use]
    pub fn new(
        inner: std::sync::Arc<dyn RangeScanner>,
        read_ts: crate::timestamp_txn::ReadTimestamp,
    ) -> Self {
        Self {
            inner,
            read_ts,
            own_start_ts: None,
            join_planner: None,
        }
    }

    /// Build a statement scanner that also exposes this transaction's intents.
    #[must_use]
    pub fn with_own_transaction(
        inner: std::sync::Arc<dyn RangeScanner>,
        read_ts: crate::timestamp_txn::ReadTimestamp,
        own_start_ts: crate::timestamp_txn::TimestampTransactionId,
    ) -> Self {
        Self {
            inner,
            read_ts,
            own_start_ts: Some(own_start_ts),
            join_planner: None,
        }
    }

    #[must_use]
    pub fn with_join_planner(
        mut self,
        stats: std::sync::Arc<dyn crate::plan_dist::Stats>,
        config: crate::plan_dist::PlannerConfig,
    ) -> Self {
        self.join_planner = Some((stats, config));
        self
    }
}

impl RangeScanner for TimestampedRangeScanner {
    fn scan(&self, mut request: ScanRequest<'_>) -> Result<Vec<ScannedRow>, ExecError> {
        if request.table.sharded {
            request.read_ts = Some(self.read_ts);
            request.own_start_ts = self.own_start_ts;
        }
        self.inner.scan(request)
    }

    fn scan_cursor<'a>(
        &'a self,
        mut request: ScanRequest<'a>,
    ) -> Result<Box<dyn RangeCursor + 'a>, ExecError> {
        if request.table.sharded {
            request.read_ts = Some(self.read_ts);
            request.own_start_ts = self.own_start_ts;
        }
        self.inner.scan_cursor(request)
    }

    fn join(&self, mut request: JoinRangeRequest) -> Result<JoinRangeResult, ExecError> {
        request.read_ts = self.read_ts.get();
        request.own_start_ts = self.own_start_ts.map(|timestamp| timestamp.get());
        self.inner.join(request)
    }

    fn join_strategy(
        &self,
        left: &crabka_pgcatalog::Table,
        right: &crabka_pgcatalog::Table,
    ) -> crate::plan_dist::JoinStrategy {
        let Some((stats, config)) = &self.join_planner else {
            return crate::plan_dist::JoinStrategy::Gather;
        };
        crate::plan_dist::plan_join_for_tables(stats.as_ref(), *config, left, right, &[], &[])
    }

    fn join_strategy_for_keys(
        &self,
        left: &crabka_pgcatalog::Table,
        right: &crabka_pgcatalog::Table,
        left_keys: &[usize],
        right_keys: &[usize],
    ) -> crate::plan_dist::JoinStrategy {
        let Some((stats, config)) = &self.join_planner else {
            return crate::plan_dist::JoinStrategy::Gather;
        };
        crate::plan_dist::plan_join_for_tables(
            stats.as_ref(),
            *config,
            left,
            right,
            left_keys,
            right_keys,
        )
    }
}

/// Scanner used by default: reads only the local MVCC store.
#[derive(Debug, Default)]
pub struct LocalRangeScanner;

struct LocalRangeCursor<'a> {
    request: ScanRequest<'a>,
    next_rowid: u64,
    end_rowid: u64,
    done: bool,
}

#[async_trait::async_trait]
impl RangeCursor for LocalRangeCursor<'_> {
    async fn next_page(&mut self, max_rows: usize) -> Result<ScanPage, ExecError> {
        if max_rows == 0 {
            return Err(ExecError::Unsupported(
                "range cursor page size must be greater than zero".into(),
            ));
        }
        if self.done {
            return Ok(ScanPage {
                rows: Box::new([]),
                is_last: true,
            });
        }
        let requested_end = self.end_rowid;
        let width = u64::try_from(max_rows).unwrap_or(u64::MAX);
        let page_end = self.next_rowid.saturating_add(width).min(requested_end);
        let interval = RowInterval {
            start: Some(self.next_rowid),
            end: Some(page_end),
        };
        let rows = if self.request.table.sharded {
            let read_ts = self.request.read_ts.ok_or_else(|| {
                ExecError::Unsupported(
                    "sharded scans require a finite statement read timestamp".into(),
                )
            })?;
            crate::exec::scan_ts_live_interval(
                self.request.local,
                self.request.global,
                self.request.table,
                read_ts,
                self.request.own_start_ts,
                interval,
            )?
        } else {
            crate::exec::scan_live_interval(
                self.request.local,
                self.request.global,
                self.request.global_snapshot,
                self.request.snapshot,
                self.request.own_xid,
                self.request.table,
                interval,
            )?
        };
        let rows = apply_executable_scan_pushdown(
            rows,
            &self.request.predicate,
            &self.request.projection,
            None,
            None,
        )?;
        self.next_rowid = page_end;
        self.done = page_end >= requested_end || page_end == u64::MAX;
        Ok(ScanPage {
            rows: rows.into_boxed_slice(),
            is_last: self.done,
        })
    }
}

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
                request.own_start_ts,
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

    fn scan_cursor<'a>(
        &'a self,
        request: ScanRequest<'a>,
    ) -> Result<Box<dyn RangeCursor + 'a>, ExecError> {
        if request.partial_aggregate.is_some() || request.top_k.is_some() {
            return Ok(Box::new(MaterializedRangeCursor::new(self.scan(request)?)));
        }
        let next_rowid = request.interval.start.unwrap_or(0);
        let end_rowid = request
            .interval
            .end
            .unwrap_or(crate::exec::read_seq_kv(request.local, request.table.id)?);
        Ok(Box::new(LocalRangeCursor {
            request,
            next_rowid,
            end_rowid,
            done: next_rowid >= end_rowid,
        }))
    }
}

/// Apply row-level pushdowns to visible rows returned by a backing scanner.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
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
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
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
        .filter(|row| row_satisfies_predicate(&row.row, predicate))
        .collect::<Vec<_>>();
    if !spec.group_by.is_empty() {
        let mut groups: Vec<(Vec<crabka_pgtypes::Datum>, Vec<ScannedRow>)> = Vec::new();
        for row in rows {
            let key = spec
                .group_by
                .iter()
                .map(|&column| {
                    row.row.get(column).cloned().ok_or_else(|| {
                        ExecError::Unsupported(format!(
                            "partial aggregate group column {column} is outside the scanned row"
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            if let Some((_, members)) = groups
                .iter_mut()
                .find(|(candidate, _)| group_keys_equal(candidate, &key))
            {
                members.push(row);
            } else {
                groups.push((key, vec![row]));
            }
        }
        let mut output = Vec::with_capacity(groups.len());
        for (mut key, members) in groups {
            let state = match spec.function {
                PartialAggregateFunction::AvgParts => {
                    compute_partial_avg_parts(members.into_iter(), spec.column)?
                }
                _ => vec![compute_partial_aggregate_value(members.into_iter(), spec)?],
            };
            key.extend(state);
            output.push(ScannedRow {
                rowid: 0,
                xmin: 0,
                row: key,
            });
        }
        output
            .sort_by(|left, right| compare_group_keys(&left.row, &right.row, spec.group_by.len()));
        return Ok(output);
    }
    let rows = rows.into_iter();
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
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn merge_partial_aggregate_rows(
    rows: Vec<ScannedRow>,
    spec: &PartialAggregateSpec,
) -> Result<Vec<ScannedRow>, ExecError> {
    if !spec.group_by.is_empty() {
        let key_len = spec.group_by.len();
        let state_len = if spec.function == PartialAggregateFunction::AvgParts {
            2
        } else {
            1
        };
        let mut groups: Vec<(Vec<crabka_pgtypes::Datum>, Vec<ScannedRow>)> = Vec::new();
        for row in rows {
            if row.row.len() != key_len + state_len {
                return Err(ExecError::Unsupported(
                    "remote grouped partial aggregate returned an invalid row shape".into(),
                ));
            }
            let key = row.row[..key_len].to_vec();
            let state = ScannedRow {
                rowid: row.rowid,
                xmin: row.xmin,
                row: row.row[key_len..].to_vec(),
            };
            if let Some((_, partials)) = groups
                .iter_mut()
                .find(|(candidate, _)| group_keys_equal(candidate, &key))
            {
                partials.push(state);
            } else {
                groups.push((key, vec![state]));
            }
        }
        let scalar_spec = PartialAggregateSpec {
            function: spec.function,
            column: Some(0),
            group_by: Vec::new(),
        };
        let mut output = Vec::with_capacity(groups.len());
        for (mut key, partials) in groups {
            let merged_rows = merge_partial_aggregate_rows(partials, &scalar_spec)?;
            let [merged] = merged_rows.as_slice() else {
                unreachable!()
            };
            key.extend(merged.row.clone());
            output.push(ScannedRow {
                rowid: 0,
                xmin: 0,
                row: key,
            });
        }
        output.sort_by(|left, right| compare_group_keys(&left.row, &right.row, key_len));
        return Ok(output);
    }
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
            group_by: Vec::new(),
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
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn finalize_partial_aggregate_rows(
    rows: Vec<ScannedRow>,
    spec: &PartialAggregateSpec,
) -> Result<Vec<ScannedRow>, ExecError> {
    let merged = merge_partial_aggregate_rows(rows, spec)?;
    if spec.function != PartialAggregateFunction::AvgParts {
        return Ok(merged);
    }
    if !spec.group_by.is_empty() {
        let key_len = spec.group_by.len();
        return merged
            .into_iter()
            .map(|row| {
                let mut output = row.row[..key_len].to_vec();
                output.push(finalize_avg_parts(&row.row[key_len..])?);
                Ok(ScannedRow {
                    rowid: 0,
                    xmin: 0,
                    row: output,
                })
            })
            .collect();
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

fn group_keys_equal(left: &[crabka_pgtypes::Datum], right: &[crabka_pgtypes::Datum]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            (left.is_null() && right.is_null())
                || crabka_pgtypes::ops::compare(left, right)
                    .is_ok_and(|ordering| ordering == Some(Ordering::Equal))
        })
}

fn compare_group_keys(
    left: &[crabka_pgtypes::Datum],
    right: &[crabka_pgtypes::Datum],
    len: usize,
) -> Ordering {
    left.iter()
        .take(len)
        .zip(right.iter().take(len))
        .map(|(left, right)| match (left.is_null(), right.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => crabka_pgtypes::ops::compare(left, right)
                .ok()
                .flatten()
                .unwrap_or(Ordering::Equal),
        })
        .find(|ordering| *ordering != Ordering::Equal)
        .unwrap_or(Ordering::Equal)
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
    use crabka_pgtypes::numeric::NumericValue;

    match value {
        crabka_pgtypes::Datum::Int4(value) => {
            Ok(crabka_pgtypes::Datum::Numeric(NumericValue::from(*value)))
        }
        crabka_pgtypes::Datum::Int8(value) => {
            Ok(crabka_pgtypes::Datum::Numeric(NumericValue::from(*value)))
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
    Float4(f32),
    Numeric(crabka_pgtypes::Datum),
}

impl PartialSum {
    fn try_from_first(value: &crabka_pgtypes::Datum) -> Result<Self, ExecError> {
        match value {
            crabka_pgtypes::Datum::Int2(value) => Ok(Self::Int(i64::from(*value))),
            crabka_pgtypes::Datum::Int4(value) => Ok(Self::Int(i64::from(*value))),
            crabka_pgtypes::Datum::Int8(value) => Ok(Self::Int(*value)),
            // `sum(real)` is `real`, so its partials must stay single-precision
            // all the way to the coordinator rather than widening to float8.
            crabka_pgtypes::Datum::Float4(value) => Ok(Self::Float4(*value)),
            crabka_pgtypes::Datum::Float8(value) => Ok(Self::Float(*value)),
            crabka_pgtypes::Datum::Numeric(_) => Ok(Self::Numeric(value.clone())),
            _ => Err(ExecError::Unsupported(
                "partial SUM pushdown supports only int2/int4/int8/float4/float8/numeric inputs"
                    .into(),
            )),
        }
    }

    fn add(&mut self, value: &crabka_pgtypes::Datum) -> Result<(), ExecError> {
        match (self, value) {
            (Self::Int(acc), crabka_pgtypes::Datum::Int2(value)) => {
                *acc = acc
                    .checked_add(i64::from(*value))
                    .ok_or(ExecError::Type(crabka_pgtypes::TypeError::Overflow))?;
            }
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
            (Self::Float4(acc), crabka_pgtypes::Datum::Float4(value)) => *acc += *value,
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
            Self::Float4(value) => crabka_pgtypes::Datum::Float4(value),
            Self::Numeric(value) => value,
        }
    }
}

fn ensure_partial_min_max_value_is_supported(
    value: &crabka_pgtypes::Datum,
) -> Result<(), ExecError> {
    if matches!(
        value,
        crabka_pgtypes::Datum::Int2(_)
            | crabka_pgtypes::Datum::Int4(_)
            | crabka_pgtypes::Datum::Int8(_)
            | crabka_pgtypes::Datum::Float4(_)
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
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
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

/// Merge already ordered range-local top-K streams without materializing or
/// globally sorting their union. Only `limit` output rows are retained.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn merge_top_k_streams(
    streams: Vec<Vec<ScannedRow>>,
    spec: &TopKSpec,
) -> Result<Vec<ScannedRow>, ExecError> {
    if spec.order_by.is_empty() {
        return Err(ExecError::Unsupported(
            "top-k pushdown requires at least one ORDER BY column".into(),
        ));
    }
    for stream in &streams {
        for key in &spec.order_by {
            ensure_top_k_column_is_supported(stream, key.column)?;
        }
        if !stream
            .windows(2)
            .all(|rows| compare_top_k_rows(&rows[0], &rows[1], &spec.order_by).is_le())
        {
            return Err(ExecError::Unsupported(
                "range-local top-k stream is not ordered".into(),
            ));
        }
    }
    let limit = usize::try_from(spec.limit).unwrap_or(usize::MAX);
    let mut positions = vec![0_usize; streams.len()];
    let mut output = Vec::with_capacity(limit.min(streams.iter().map(Vec::len).sum()));
    while output.len() < limit {
        let next = streams
            .iter()
            .enumerate()
            .filter_map(|(stream, rows)| rows.get(positions[stream]).map(|row| (stream, row)))
            .min_by(|(_, left), (_, right)| compare_top_k_rows(left, right, &spec.order_by));
        let Some((stream, _)) = next else { break };
        output.push(streams[stream][positions[stream]].clone());
        positions[stream] += 1;
    }
    Ok(output)
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
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use crabka_pgcatalog::{Column, RelationName, Table};
    use crabka_pgkv::MemKv;
    use crabka_pgmvcc::Snapshot;
    use crabka_pgtypes::{ColumnType, Datum};

    use super::{
        MaterializedRangeCursor, PredicatePushdown, ProjectionPushdown, RangeCursor, RangeScanner,
        RowInterval, ScanRequest, TimestampedRangeScanner,
    };

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

    #[derive(Default)]
    struct CursorSpy {
        scan_calls: AtomicUsize,
        cursor_calls: AtomicUsize,
    }

    impl RangeScanner for CursorSpy {
        fn scan(
            &self,
            _request: ScanRequest<'_>,
        ) -> Result<Vec<super::ScannedRow>, super::ExecError> {
            self.scan_calls.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }

        fn scan_cursor<'a>(
            &'a self,
            request: ScanRequest<'a>,
        ) -> Result<Box<dyn RangeCursor + 'a>, super::ExecError> {
            self.cursor_calls.fetch_add(1, Ordering::Relaxed);
            assert_eq!(request.read_ts.map(|timestamp| timestamp.get()), Some(42));
            Ok(Box::new(MaterializedRangeCursor::new(Vec::new())))
        }
    }

    #[tokio::test]
    async fn timestamped_scanner_delegates_to_native_cursor_without_materializing() {
        let inner = Arc::new(CursorSpy::default());
        let scanner = TimestampedRangeScanner::new(
            inner.clone(),
            crate::timestamp_txn::ReadTimestamp::new(42).expect("valid read timestamp"),
        );
        let local = MemKv::new();
        let global = MemKv::new();
        let snapshot = Snapshot {
            xmin: 1,
            xmax: 2,
            xip: Vec::new(),
        };
        let table = Table {
            id: 42,
            name: RelationName::public("items"),
            columns: vec![Column::new("id", ColumnType::Int8)],
            sharded: true,
            sharding: None,
            foreign: None,
            checks: Vec::new(),
        };

        let mut cursor = scanner
            .scan_cursor(ScanRequest {
                local: &local,
                global: &global,
                global_snapshot: &snapshot,
                snapshot: &snapshot,
                own_xid: None,
                read_ts: None,
                own_start_ts: None,
                table: &table,
                interval: RowInterval::default(),
                predicate: PredicatePushdown::FullScan,
                projection: ProjectionPushdown::All,
                partial_aggregate: None,
                top_k: None,
            })
            .expect("native cursor opens");
        let page = cursor.next_page(1).await.expect("cursor page succeeds");

        assert!(page.is_last);
        assert_eq!(inner.cursor_calls.load(Ordering::Relaxed), 1);
        assert_eq!(inner.scan_calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn blocking_cursor_collection_returns_out_of_memory_before_crossing_budget() {
        let scanner = CursorSpy::default();
        let local = MemKv::new();
        let snapshot = Snapshot {
            xmin: 1,
            xmax: 2,
            xip: Vec::new(),
        };
        let table = Table {
            id: 42,
            name: RelationName::public("items"),
            columns: vec![Column::new("id", ColumnType::Int8)],
            sharded: false,
            sharding: None,
            foreign: None,
            checks: Vec::new(),
        };
        let error = super::collect_cursor_bounded(
            &MaterializedOnlyScanner(vec![super::ScannedRow {
                rowid: 1,
                xmin: 1,
                row: vec![Datum::Text("x".repeat(128))],
            }]),
            ScanRequest {
                local: &local,
                global: &local,
                global_snapshot: &snapshot,
                snapshot: &snapshot,
                own_xid: None,
                read_ts: None,
                own_start_ts: None,
                table: &table,
                interval: RowInterval::ALL,
                predicate: PredicatePushdown::FullScan,
                projection: ProjectionPushdown::All,
                partial_aggregate: None,
                top_k: None,
            },
            64,
        )
        .expect_err("row must be rejected before exceeding budget");

        assert_eq!(error.into_pg().code, "53200");
        assert_eq!(scanner.scan_calls.load(Ordering::Relaxed), 0);
    }

    struct MaterializedOnlyScanner(Vec<super::ScannedRow>);

    impl RangeScanner for MaterializedOnlyScanner {
        fn scan(
            &self,
            _request: ScanRequest<'_>,
        ) -> Result<Vec<super::ScannedRow>, super::ExecError> {
            Ok(self.0.clone())
        }
    }
}

#[cfg(test)]
mod streaming_aggregate_tests {
    use assert2::assert;
    use crabka_pgcatalog::{Column, RelationName, Table};
    use crabka_pgkv::MemKv;
    use crabka_pgmvcc::Snapshot;
    use crabka_pgtypes::{ColumnType, Datum};

    use super::{
        PartialAggregateSpec, PredicatePushdown, ProjectionPushdown, RangeScanner, RowInterval,
        ScanRequest, ScannedRow,
    };

    struct FixedRowsScanner(Vec<ScannedRow>);

    impl RangeScanner for FixedRowsScanner {
        fn scan(&self, _request: ScanRequest<'_>) -> Result<Vec<ScannedRow>, super::ExecError> {
            Ok(self.0.clone())
        }
    }

    fn table() -> Table {
        Table {
            id: 42,
            name: RelationName::public("items"),
            columns: vec![Column::new("v", ColumnType::Int8)],
            sharded: false,
            sharding: None,
            foreign: None,
            checks: Vec::new(),
        }
    }

    fn request<'a>(local: &'a MemKv, snapshot: &'a Snapshot, table: &'a Table) -> ScanRequest<'a> {
        ScanRequest {
            local,
            global: local,
            global_snapshot: snapshot,
            snapshot,
            own_xid: None,
            read_ts: None,
            own_start_ts: None,
            table,
            interval: RowInterval::ALL,
            predicate: PredicatePushdown::FullScan,
            projection: ProjectionPushdown::All,
            partial_aggregate: None,
            top_k: None,
        }
    }

    fn snapshot() -> Snapshot {
        Snapshot {
            xmin: 1,
            xmax: 2,
            xip: Vec::new(),
        }
    }

    fn int_rows(count: u64) -> Vec<ScannedRow> {
        (1..=count)
            .map(|rowid| ScannedRow {
                rowid,
                xmin: 1,
                row: vec![Datum::Int8(i64::try_from(rowid).expect("test rowid fits"))],
            })
            .collect()
    }

    fn spec(function: &str) -> PartialAggregateSpec {
        PartialAggregateSpec::from_function(function, Some(0)).expect("supported aggregate")
    }

    #[test]
    fn streaming_fold_computes_every_aggregate_under_a_per_page_budget() {
        let rows = int_rows(2500);
        let whole_table_bytes = rows
            .iter()
            .map(|row| std::mem::size_of::<ScannedRow>() + super::datum_row_bytes(&row.row))
            .sum::<usize>();
        // A budget the whole table exceeds but a single 1024-row page does not:
        // the streaming fold must succeed exactly where whole-table collection fails.
        let budget = whole_table_bytes - 1;
        let scanner = FixedRowsScanner(rows);
        let (local, snapshot, table) = (MemKv::new(), snapshot(), table());
        let specs = vec![
            PartialAggregateSpec::from_function("count", None).expect("count(*)"),
            spec("sum"),
            spec("min"),
            spec("max"),
            spec("avg"),
        ];

        let collected =
            super::collect_cursor_bounded(&scanner, request(&local, &snapshot, &table), budget);
        let states = super::collect_partial_aggregates_bounded(
            &scanner,
            request(&local, &snapshot, &table),
            &specs,
            budget,
        )
        .expect("streaming fold stays under the per-page budget");

        assert!(
            collected
                .expect_err("whole-table collection busts the budget")
                .into_pg()
                .code
                == "53200"
        );
        let finalized = states
            .into_iter()
            .zip(&specs)
            .map(|(state, spec)| {
                let rows = super::finalize_partial_aggregate_rows(state, spec)
                    .expect("finalize partial state");
                let [row] = rows.as_slice() else {
                    panic!("scalar aggregate must finalize to one row");
                };
                let [value] = row.row.as_slice() else {
                    panic!("scalar aggregate must finalize to one value");
                };
                value.clone()
            })
            .collect::<Vec<_>>();
        assert!(
            finalized
                == vec![
                    Datum::Int8(2500),
                    Datum::Int8(3_126_250),
                    Datum::Int8(1),
                    Datum::Int8(2500),
                    Datum::Numeric(crabka_pgtypes::numeric::parse("1250.5").expect("test literal")),
                ]
        );
    }

    #[test]
    fn streaming_fold_rejects_a_single_page_over_the_budget() {
        let scanner = FixedRowsScanner(vec![ScannedRow {
            rowid: 1,
            xmin: 1,
            row: vec![Datum::Text("x".repeat(4096))],
        }]);
        let (local, snapshot, table) = (MemKv::new(), snapshot(), table());
        let specs = vec![PartialAggregateSpec::from_function("count", None).expect("count(*)")];

        let error = super::collect_partial_aggregates_bounded(
            &scanner,
            request(&local, &snapshot, &table),
            &specs,
            64,
        )
        .expect_err("one oversized page must fail closed");

        assert!(error.into_pg().code == "53200");
    }

    #[test]
    fn streaming_fold_rejects_grouped_state_growing_past_the_budget() {
        // Every row is its own group, so the merged state grows with the scan
        // even though each page fits: the accumulated-state check must fire.
        let rows = (1..=2500_u64)
            .map(|rowid| ScannedRow {
                rowid,
                xmin: 1,
                row: vec![Datum::Text(format!("{rowid:08}{}", "k".repeat(504)))],
            })
            .collect::<Vec<_>>();
        let page_bytes = rows
            .iter()
            .take(1024)
            .map(|row| std::mem::size_of::<ScannedRow>() + super::datum_row_bytes(&row.row))
            .sum::<usize>();
        let scanner = FixedRowsScanner(rows);
        let (local, snapshot, table) = (MemKv::new(), snapshot(), table());
        let specs = vec![
            PartialAggregateSpec::from_function("count", None)
                .expect("count(*)")
                .grouped_by(vec![0]),
        ];

        let error = super::collect_partial_aggregates_bounded(
            &scanner,
            request(&local, &snapshot, &table),
            &specs,
            // Above one page (and its per-page state), below two pages of groups.
            page_bytes + page_bytes / 2,
        )
        .expect_err("unbounded distinct group keys must fail closed");

        assert!(error.into_pg().code == "53200");
    }
}
