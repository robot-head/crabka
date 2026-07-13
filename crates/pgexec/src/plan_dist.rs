//! Deterministic distributed-planning pre-pass for range scans.

use crabka_pgcatalog::Table;
use crabka_pgparser::ast::{BinaryOp, Expr, FuncArgs, SelectItem};
use crabka_pgtypes::{ColumnType, Datum};
use std::collections::BTreeMap;
use std::sync::Arc;

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

/// Live per-table byte estimates derived from monotonic sequence counters.
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

/// Read-only statistics adapter over the engine's authoritative durable
/// per-table next-rowid keys. It deliberately reads the applied KV on every
/// estimate so sessions observe commits and replicated apply without refresh.
#[derive(Clone)]
pub struct DurableSequenceStats {
    kv: Arc<dyn crabka_pgkv::Kv>,
}

impl DurableSequenceStats {
    #[must_use]
    pub fn new(kv: Arc<dyn crabka_pgkv::Kv>) -> Self {
        Self { kv }
    }
}

impl Stats for DurableSequenceStats {
    fn estimated_bytes(&self, table_id: u64) -> Option<u64> {
        let table_id = u32::try_from(table_id).ok()?;
        let next = crate::exec::read_seq_kv(self.kv.as_ref(), table_id).ok()?;
        // The counter is a row cardinality source, not a byte counter. Use a
        // conservative encoded-row allowance; the gateway still checks the
        // exact materialized wire request before broadcasting.
        Some(next.saturating_sub(1).saturating_mul(256))
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
/// catalog order. Identical layouts alone are insufficient for range-local
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
/// physical catalog metadata used by execution. Statistics may suggest
/// co-partitioning, but they cannot substitute for an identical hash layout.
#[must_use]
pub fn plan_join_for_tables(
    stats: &dyn Stats,
    config: PlannerConfig,
    left: &Table,
    right: &Table,
) -> JoinStrategy {
    let selected = plan_join(
        stats,
        config,
        JoinInputs {
            left_table_id: u64::from(left.id),
            right_table_id: u64::from(right.id),
        },
    );
    if selected == JoinStrategy::CoPartitioned && !tables_are_co_partitioned(left, right) {
        JoinStrategy::Gather
    } else {
        selected
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
}

impl Default for DistributedScanPlan {
    fn default() -> Self {
        Self {
            predicate: PredicatePushdown::FullScan,
            projection: ProjectionPushdown::All,
            partial_aggregate: None,
            top_k: None,
        }
    }
}

/// Build the currently-safe scan pre-pass. Unsupported fragments are omitted so
/// the caller can keep the original local filter/projection as the source of truth.
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
    }
}

/// Parse a filter into a pushdown predicate, failing when any conjunct is not in
/// the supported equality/range subset.
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

fn predicate_for_filter(table: &Table, filter: Option<&Expr>) -> PredicatePushdown {
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

fn partial_aggregate_for_select_items(
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
    if call.distinct {
        return None;
    }
    let column = match &call.args {
        FuncArgs::Star => None,
        FuncArgs::Exprs(args) if args.len() == 1 => match &args[0] {
            Expr::Column { table: None, name } => {
                table.columns.iter().position(|column| column.name == *name)
            }
            _ => return None,
        },
        _ => return None,
    };
    let spec = PartialAggregateSpec::from_function(&call.name, column)?;
    if partial_aggregate_is_safe(table, &spec) {
        Some(spec)
    } else {
        None
    }
}

/// Recognize the narrow grouped-partial shape: group columns followed by one aggregate.
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
