//! Deterministic distributed-planning pre-pass for range scans.

use crabka_pgcatalog::Table;
use crabka_pgparser::ast::{BinaryOp, Expr, FuncArgs, SelectItem};
use crabka_pgtypes::{ColumnType, Datum};

use crate::{
    ExecError,
    scanner::{
        ColumnPredicate, PartialAggregateSpec, PredicateOp, PredicatePushdown, ProjectionPushdown,
        TopKSpec,
    },
};

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
