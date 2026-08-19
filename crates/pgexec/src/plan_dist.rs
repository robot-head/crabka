//! Deterministic distributed-planning pre-pass for range scans.

use std::{collections::BTreeMap, sync::Arc};

use crabka_pgcatalog::Table;
use crabka_pgparser::ast::{BinaryOp, Expr, FuncArgs, FuncCall, SelectItem};
use crabka_pgtypes::{ColumnType, Datum};

use crate::{
    ExecError,
    scanner::{
        ColumnPredicate, PartialAggregateSpec, PredicateOp, PredicatePushdown, ProjectionPushdown,
        TopKSpec,
    },
};

/// Statistics seam consumed by distributed join planning.
pub trait Stats: Send + Sync + 'static {
    fn estimated_bytes(&self, table_id: u64) -> Option<u64>;

    fn are_co_partitioned(&self, _left_table_id: u64, _right_table_id: u64) -> bool {
        false
    }
}

/// Compose live sources conservatively. A table estimate is the maximum value
/// currently published by either authoritative source.
pub struct CombinedStats {
    first: Arc<dyn Stats>,
    second: Arc<dyn Stats>,
}

impl CombinedStats {
    #[must_use]
    pub fn new(first: Arc<dyn Stats>, second: Arc<dyn Stats>) -> Self {
        Self { first, second }
    }
}

impl Stats for CombinedStats {
    fn estimated_bytes(&self, table_id: u64) -> Option<u64> {
        match (
            self.first.estimated_bytes(table_id),
            self.second.estimated_bytes(table_id),
        ) {
            (Some(first), Some(second)) => Some(first.max(second)),
            (first, second) => first.or(second),
        }
    }

    fn are_co_partitioned(&self, left_table_id: u64, right_table_id: u64) -> bool {
        self.first.are_co_partitioned(left_table_id, right_table_id)
            || self
                .second
                .are_co_partitioned(left_table_id, right_table_id)
    }
}

/// Encoded-row allowance applied to a row count to reach a byte estimate. The
/// gateway still checks the exact materialized wire request before
/// broadcasting, so this only has to be the right order of magnitude.
const ASSUMED_ROW_BYTES: u64 = 256;

/// Upper limit on the keys one estimate reads, however large the configured
/// broadcast threshold is.
///
/// It is the gateway's own cap on a broadcast side
/// ([`crate::scanner::MAX_JOIN_BROADCAST_ROWS`]): a table holding more rows than
/// that has its broadcast rejected at execution whatever the planner decided, so
/// reading past it cannot change an outcome. Reaching it makes the measurement
/// abstain, which costs a broadcast opportunity and never a wrong one — and
/// bounds one estimate at a few thousand keys rather than the quarter of a
/// million the default threshold would otherwise allow, which measured two
/// orders of magnitude slower.
const MAX_MEASURED_KEYS: usize = crate::scanner::MAX_JOIN_BROADCAST_ROWS;

/// Fixed per-table byte estimates supplied by the caller.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SequenceCounters(BTreeMap<u64, u64>);

impl SequenceCounters {
    pub fn new(values: impl IntoIterator<Item = (u64, u64)>) -> Self {
        Self(values.into_iter().collect())
    }
}

impl Stats for SequenceCounters {
    fn estimated_bytes(&self, table_id: u64) -> Option<u64> {
        self.0.get(&table_id).copied()
    }
}

/// Read-only statistics adapter over the engine's own applied row storage.
///
/// Two independent upper bounds on a table's live rows are cheap to obtain, and
/// this reports the smaller of them:
///
/// * **Rowids allocated** — the durable next-rowid counter, minus one. Every
///   live row consumed a rowid, so this bounds them, but it only ever counts
///   upwards: it measures every row the table has *ever* held. A delete never
///   lowers it. `CLUSTER` raises it by the whole live row count on each run,
///   because it rewrites every row at a fresh rowid and permanently vacates the
///   block it came from — so a table that never grew doubles its estimate per
///   `CLUSTER`. On a sharded table the hidden rowid is a packed clock reading
///   rather than a position in a counter, and this bound saturates for a table
///   of any size at all.
/// * **Rows stored** — the number of distinct row prefixes in the table's
///   primary index, counted straight out of the store. Every live row holds at
///   least one version key, so this bounds them too, and unlike the counter it
///   is a measurement: it drops when deleted rows are vacuumed away, it does
///   not move when `CLUSTER` renumbers rows in place, and it does not care what
///   a rowid means. It costs one bounded keys-only scan and no value reads.
///
/// Counting is the expensive half, so it is bounded: it reads at most the rows
/// the broadcast threshold could hold, and never more than the gateway would
/// accept in a broadcast, past which it abstains and the counter's answer
/// stands. A table that far over the threshold is not a broadcast candidate
/// under any number. One estimate therefore costs a bounded scan whatever the
/// table's real size — measured at 1.2ms against the durable store and 65us in
/// memory, against about 1us for the counter alone.
///
/// Both bounds read the applied KV on every estimate, so sessions observe
/// commits and replicated apply without refresh.
#[derive(Clone)]
pub struct StoredRowStats {
    kv: Arc<dyn crabka_pgkv::Kv>,
    key_budget: usize,
}

impl StoredRowStats {
    /// Count only what `broadcast_threshold_bytes` makes worth counting.
    #[must_use]
    pub fn new(kv: Arc<dyn crabka_pgkv::Kv>, broadcast_threshold_bytes: u64) -> Self {
        let budget = (broadcast_threshold_bytes / ASSUMED_ROW_BYTES).saturating_add(1);
        Self {
            kv,
            key_budget: usize::try_from(budget)
                .unwrap_or(MAX_MEASURED_KEYS)
                .min(MAX_MEASURED_KEYS),
        }
    }

    /// Count the distinct row prefixes stored for `table_id`, or abstain when
    /// there are more of them than the budget permits reading.
    ///
    /// A row's version keys are adjacent, so distinctness is a comparison
    /// against the previous key rather than a set, and the whole count needs one
    /// key buffer rather than one per key.
    fn stored_rows(&self, table_id: u32) -> Option<u64> {
        let mut rows = 0_u64;
        let mut previous: Vec<u8> = Vec::new();
        let seen = self
            .kv
            .for_each_key(
                &crabka_pgkv::key::table_prefix(table_id),
                &crabka_pgkv::key::table_prefix_end(table_id),
                self.key_budget,
                &mut |key| {
                    // A key too short to carry a version suffix belongs to no
                    // row this can name, so it counts as one of its own: the
                    // bound has to stay above the truth, never under it.
                    let prefix = crabka_pgmvcc::version::row_prefix_of(key).unwrap_or(key);
                    if previous != prefix {
                        rows = rows.saturating_add(1);
                        previous.clear();
                        previous.extend_from_slice(prefix);
                    }
                },
            )
            .ok()?;
        (seen < self.key_budget).then_some(rows)
    }
}

impl Stats for StoredRowStats {
    fn estimated_bytes(&self, table_id: u64) -> Option<u64> {
        let table_id = u32::try_from(table_id).ok()?;
        let allocated = crate::exec::read_seq_kv(self.kv.as_ref(), table_id)
            .ok()?
            .saturating_sub(1);
        let rows = self
            .stored_rows(table_id)
            .map_or(allocated, |stored| stored.min(allocated));
        Some(rows.saturating_mul(ASSUMED_ROW_BYTES))
    }
}

/// Durable per-table byte estimates read from checkpoint metadata.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CheckpointMetadata(BTreeMap<u64, u64>);

impl CheckpointMetadata {
    pub fn new(values: impl IntoIterator<Item = (u64, u64)>) -> Self {
        Self(values.into_iter().collect())
    }
}

impl Stats for CheckpointMetadata {
    fn estimated_bytes(&self, table_id: u64) -> Option<u64> {
        self.0.get(&table_id).copied()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannerConfig {
    pub broadcast_threshold_bytes: u64,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            broadcast_threshold_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinInputs {
    pub left_table_id: u64,
    pub right_table_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinStrategy {
    Broadcast { small_table_id: u64 },
    CoPartitioned,
    Gather,
}

/// Return whether catalog metadata proves that two tables use the same physical
/// hash partitioning. A missing co-location group is deliberately not proof.
#[must_use]
pub fn tables_are_co_partitioned(left: &Table, right: &Table) -> bool {
    use crabka_pgcatalog::ShardingStrategy;

    let (Some(ShardingStrategy::Hash(left_hash)), Some(ShardingStrategy::Hash(right_hash))) =
        (&left.sharding, &right.sharding)
    else {
        return false;
    };
    left_hash.buckets == right_hash.buckets
        && left_hash.columns == right_hash.columns
        && left_hash.co_location_group.is_some()
        && left_hash.co_location_group == right_hash.co_location_group
}

/// Prove that a co-partitioned join uses each table's complete hash key, in
/// catalog order. Identical layouts alone are not enough for range-local
/// equality joins on some other column.
#[must_use]
pub fn co_partitioned_join_keys_match(
    left: &Table,
    right: &Table,
    left_keys: &[usize],
    right_keys: &[usize],
) -> bool {
    use crabka_pgcatalog::ShardingStrategy;

    if !tables_are_co_partitioned(left, right) {
        return false;
    }
    let (Some(ShardingStrategy::Hash(left_hash)), Some(ShardingStrategy::Hash(right_hash))) =
        (&left.sharding, &right.sharding)
    else {
        return false;
    };
    let indexes = |table: &Table, columns: &[String]| {
        columns
            .iter()
            .map(|name| table.columns.iter().position(|column| &column.name == name))
            .collect::<Option<Vec<_>>>()
    };
    indexes(left, &left_hash.columns).as_deref() == Some(left_keys)
        && indexes(right, &right_hash.columns).as_deref() == Some(right_keys)
}

#[must_use]
pub fn plan_join(stats: &dyn Stats, config: PlannerConfig, inputs: JoinInputs) -> JoinStrategy {
    let estimates = [
        (
            inputs.left_table_id,
            stats.estimated_bytes(inputs.left_table_id),
        ),
        (
            inputs.right_table_id,
            stats.estimated_bytes(inputs.right_table_id),
        ),
    ];
    if let Some((small_table_id, _)) = estimates
        .into_iter()
        .filter_map(|(table_id, bytes)| bytes.map(|bytes| (table_id, bytes)))
        .filter(|(_, bytes)| *bytes <= config.broadcast_threshold_bytes)
        .min_by_key(|&(table_id, bytes)| (bytes, table_id))
    {
        return JoinStrategy::Broadcast { small_table_id };
    }
    if stats.are_co_partitioned(inputs.left_table_id, inputs.right_table_id) {
        JoinStrategy::CoPartitioned
    } else {
        JoinStrategy::Gather
    }
}

/// Select a join strategy and validate any co-partitioned answer against the
/// physical catalog metadata execution uses. Statistics may suggest
/// co-partitioning, but they cannot replace an identical hash layout.
#[must_use]
pub fn plan_join_for_tables(
    stats: &dyn Stats,
    config: PlannerConfig,
    left: &Table,
    right: &Table,
    left_keys: &[usize],
    right_keys: &[usize],
) -> JoinStrategy {
    let selected = plan_join(
        stats,
        config,
        JoinInputs {
            left_table_id: u64::from(left.id),
            right_table_id: u64::from(right.id),
        },
    );
    if let JoinStrategy::Broadcast { .. } = selected {
        return selected;
    }
    if co_partitioned_join_keys_match(left, right, left_keys, right_keys) {
        JoinStrategy::CoPartitioned
    } else {
        JoinStrategy::Gather
    }
}

/// Pushdown plan for one base-table scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedScanPlan {
    /// Predicate fragments the range owner can evaluate before returning rows.
    pub predicate: PredicatePushdown,
    /// Columns the range owner may materialize instead of the full tuple.
    pub projection: ProjectionPushdown,
    /// Partial aggregate request. `None` keeps ordinary row scanning.
    pub partial_aggregate: Option<PartialAggregateSpec>,
    /// Per-range top-K request. `None` leaves ordering to the gateway.
    pub top_k: Option<TopKSpec>,
    /// Constant `tsvector @@ tsquery` predicate eligible for a local GIN probe.
    pub text_search: Option<TextSearchPredicate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextSearchPredicate {
    pub column: usize,
    pub query: crabka_pgtypes::TsQuery,
}

impl Default for DistributedScanPlan {
    fn default() -> Self {
        Self {
            predicate: PredicatePushdown::FullScan,
            projection: ProjectionPushdown::All,
            partial_aggregate: None,
            top_k: None,
            text_search: None,
        }
    }
}

/// Build the currently-safe scan pre-pass. This function omits unsupported
/// fragments, so the caller can keep the original local filter and projection as
/// the source of truth.
#[must_use]
pub fn plan_scan(
    table: &Table,
    filter: Option<&Expr>,
    projection: &[SelectItem],
) -> DistributedScanPlan {
    DistributedScanPlan {
        predicate: predicate_for_filter(table, filter),
        projection: projection_for_select_items(table, projection),
        partial_aggregate: partial_aggregate_for_select_items(table, projection),
        top_k: None,
        text_search: text_search_for_filter(table, filter),
    }
}

fn text_search_for_filter(table: &Table, filter: Option<&Expr>) -> Option<TextSearchPredicate> {
    let filter = filter?;
    if let Expr::Binary {
        op: BinaryOp::And,
        left,
        right,
    } = filter
    {
        return text_search_for_filter(table, Some(left))
            .or_else(|| text_search_for_filter(table, Some(right)));
    }
    let Expr::Binary {
        op: BinaryOp::JsonPathMatch,
        left,
        right,
    } = filter
    else {
        return None;
    };
    text_search_pair(table, left, right).or_else(|| text_search_pair(table, right, left))
}

fn text_search_pair(table: &Table, vector: &Expr, query: &Expr) -> Option<TextSearchPredicate> {
    let Expr::Column { name, .. } = vector else {
        return None;
    };
    let column = table
        .columns
        .iter()
        .position(|candidate| candidate.name == *name && candidate.ty == ColumnType::TsVector)?;
    let query = crate::text_search_fn::constant_query(query)
        .ok()
        .flatten()?;
    Some(TextSearchPredicate { column, query })
}

/// Parse a filter into a pushdown predicate. This function fails when any
/// conjunct is not in the supported equality and range subset.
///
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn strict_predicate_for_filter(
    table: &Table,
    filter: Option<&Expr>,
) -> Result<PredicatePushdown, ExecError> {
    let Some(filter) = filter else {
        return Ok(PredicatePushdown::FullScan);
    };
    let mut predicates = Vec::new();
    collect_supported_conjuncts(table, filter, &mut predicates)?;
    if predicates.is_empty() {
        return Ok(PredicatePushdown::FullScan);
    }
    Ok(PredicatePushdown::Conjunctive(predicates))
}

/// Best-effort variant of [`strict_predicate_for_filter`]. A filter with any
/// unsupported conjunct degrades to [`PredicatePushdown::FullScan`] instead of
/// reporting an error. The SELECT scan planner and the UPDATE/DELETE index-probe
/// chooser share it, so both extract equality conjuncts identically.
pub(crate) fn predicate_for_filter(table: &Table, filter: Option<&Expr>) -> PredicatePushdown {
    strict_predicate_for_filter(table, filter).unwrap_or(PredicatePushdown::FullScan)
}

fn collect_supported_conjuncts(
    table: &Table,
    expr: &Expr,
    predicates: &mut Vec<ColumnPredicate>,
) -> Result<(), ExecError> {
    if let Expr::Binary {
        op: BinaryOp::And,
        left,
        right,
    } = expr
    {
        collect_supported_conjuncts(table, left, predicates)?;
        collect_supported_conjuncts(table, right, predicates)?;
        return Ok(());
    }
    if let Some(predicate) = parse_comparison(table, expr)? {
        predicates.push(predicate);
        return Ok(());
    }
    Err(ExecError::Unsupported(
        "distributed predicate pushdown supports only column literal equality/range conjuncts"
            .into(),
    ))
}

fn parse_comparison(table: &Table, expr: &Expr) -> Result<Option<ColumnPredicate>, ExecError> {
    let Expr::Binary { op, left, right } = expr else {
        return Ok(None);
    };
    if let Some((column, value)) = parse_column_literal(table, left, right)? {
        return Ok(PredicateOp::from_binary(*op).map(|op| ColumnPredicate { column, op, value }));
    }
    if let Some((column, value)) = parse_column_literal(table, right, left)? {
        return Ok(
            PredicateOp::from_reversed_binary(*op).map(|op| ColumnPredicate { column, op, value }),
        );
    }
    Ok(None)
}

fn parse_column_literal(
    table: &Table,
    column_expr: &Expr,
    value_expr: &Expr,
) -> Result<Option<(usize, Datum)>, ExecError> {
    let Expr::Column { table: None, name } = column_expr else {
        return Ok(None);
    };
    let Some((index, column)) = table
        .columns
        .iter()
        .enumerate()
        .find(|(_, column)| column.name == *name)
    else {
        return Ok(None);
    };
    literal_for_type(value_expr, column.ty).map(|value| value.map(|value| (index, value)))
}

fn literal_for_type(expr: &Expr, ty: ColumnType) -> Result<Option<Datum>, ExecError> {
    if !scanner_predicate_type_is_supported(ty) {
        return Ok(None);
    }

    match (expr, ty) {
        (Expr::IntLiteral(raw), ColumnType::Int4) => raw
            .parse::<i32>()
            .map(|value| Some(Datum::Int4(value)))
            .map_err(|_| ExecError::TypeMismatch("int4 predicate literal is out of range".into())),
        (Expr::IntLiteral(raw), ColumnType::Int8) => raw
            .parse::<i64>()
            .map(|value| Some(Datum::Int8(value)))
            .map_err(|_| ExecError::TypeMismatch("int8 predicate literal is out of range".into())),
        (Expr::StringLiteral(value), ColumnType::Text) => Ok(Some(Datum::Text(value.clone()))),
        (Expr::BoolLiteral(value), ColumnType::Bool) => Ok(Some(Datum::Bool(*value))),
        (
            Expr::Const {
                value,
                ty: const_ty,
            },
            _,
        ) if *const_ty == ty => Ok(Some(value.clone())),
        _ => Ok(None),
    }
}

fn scanner_predicate_type_is_supported(ty: ColumnType) -> bool {
    matches!(
        ty,
        ColumnType::Bool | ColumnType::Int4 | ColumnType::Int8 | ColumnType::Text
    )
}

fn projection_for_select_items(table: &Table, projection: &[SelectItem]) -> ProjectionPushdown {
    let mut columns = Vec::new();
    for item in projection {
        let SelectItem::Expr {
            expr: Expr::Column { table: None, name },
            ..
        } = item
        else {
            return ProjectionPushdown::All;
        };
        let Some(index) = table.columns.iter().position(|column| column.name == *name) else {
            return ProjectionPushdown::All;
        };
        if !columns.contains(&index) {
            columns.push(index);
        }
    }
    if columns.is_empty() {
        return ProjectionPushdown::All;
    }
    ProjectionPushdown::Columns(columns)
}

pub(crate) fn partial_aggregate_for_select_items(
    table: &Table,
    projection: &[SelectItem],
) -> Option<PartialAggregateSpec> {
    if projection.len() != 1 {
        return None;
    }
    let SelectItem::Expr {
        expr: Expr::Func(call),
        ..
    } = &projection[0]
    else {
        return None;
    };
    partial_aggregate_for_call(table, call)
}

/// The pushdown spec for one aggregate call. The call must be a plain
/// `count(*)`, `count(col)`, `sum(col)`, `avg(col)`, `min(col)` or `max(col)`,
/// without `DISTINCT`, whose column type the partial-aggregate model supports.
/// Every other call is `None`. That includes an argument column that does not
/// exist in `table`, which must fall back so name resolution reports 42703,
/// instead of the spec silently degrading to a whole-table `count(*)`.
pub(crate) fn partial_aggregate_for_call(
    table: &Table,
    call: &FuncCall,
) -> Option<PartialAggregateSpec> {
    // A FILTER predicate is evaluated per row against the full scope, which the
    // scan-level partial aggregate cannot do — pushing the call down without it
    // would aggregate every row and silently ignore the filter. A sort is the
    // same kind of loss: the partial states merge in range order, so a call that
    // asks for a fold order the pushdown cannot promise stays on the general
    // path instead.
    if call.distinct || call.filter.is_some() || !call.order_by.is_empty() {
        return None;
    }
    let column = match &call.args {
        FuncArgs::Star => None,
        FuncArgs::Exprs(args) => {
            let [Expr::Column { table: None, name }] = args.as_slice() else {
                return None;
            };
            Some(
                table
                    .columns
                    .iter()
                    .position(|column| column.name == *name)?,
            )
        }
        FuncArgs::Named { .. } => return None,
    };
    let spec = PartialAggregateSpec::from_function(&call.name, column)?;
    if partial_aggregate_is_safe(table, &spec) {
        Some(spec)
    } else {
        None
    }
}

/// Recognize the narrow grouped-partial shape: group columns, then one
/// aggregate.
pub fn grouped_partial_aggregate_for_select(
    table: &Table,
    projection: &[SelectItem],
    group_by: &[Expr],
) -> Option<PartialAggregateSpec> {
    if group_by.is_empty() || projection.len() != group_by.len() + 1 {
        return None;
    }
    let mut group_columns = Vec::with_capacity(group_by.len());
    for (item, group) in projection.iter().zip(group_by) {
        let Expr::Column { table: None, name } = group else {
            return None;
        };
        let SelectItem::Expr { expr, .. } = item else {
            return None;
        };
        if expr != group {
            return None;
        }
        group_columns.push(
            table
                .columns
                .iter()
                .position(|column| column.name == *name)?,
        );
    }
    let aggregate = partial_aggregate_for_select_items(table, &projection[group_by.len()..])?;
    Some(aggregate.grouped_by(group_columns))
}

fn partial_aggregate_is_safe(table: &Table, spec: &PartialAggregateSpec) -> bool {
    match spec.function {
        crate::PartialAggregateFunction::Count => true,
        crate::PartialAggregateFunction::Sum => spec
            .column
            .and_then(|column| table.columns.get(column))
            .is_some_and(|column| {
                matches!(
                    column.ty,
                    ColumnType::Int4 | ColumnType::Int8 | ColumnType::Numeric(_)
                )
            }),
        crate::PartialAggregateFunction::Min | crate::PartialAggregateFunction::Max => spec
            .column
            .and_then(|column| table.columns.get(column))
            .is_some_and(|column| {
                matches!(
                    column.ty,
                    ColumnType::Bool
                        | ColumnType::Int4
                        | ColumnType::Int8
                        | ColumnType::Text
                        | ColumnType::Float8
                        | ColumnType::Numeric(_)
                        | ColumnType::Date
                        | ColumnType::Time
                        | ColumnType::Timestamp
                        | ColumnType::Timestamptz
                        | ColumnType::Interval
                )
            }),
        crate::PartialAggregateFunction::AvgParts => spec
            .column
            .and_then(|column| table.columns.get(column))
            .is_some_and(|column| {
                matches!(
                    column.ty,
                    ColumnType::Int4 | ColumnType::Int8 | ColumnType::Numeric(_)
                )
            }),
    }
}
