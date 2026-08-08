//! `LogicalPlan` lowering for the simple `PromQL` aggregations.
//!
//! The aggregations are `sum | avg | min | max | count | group` with
//! `by(...)`/`without(...)`.
//!
//! The recursive instant planner [`crate::engine`] hands this module an inner
//! [`LogicalPlan`] whose output carries one row for each input series. That row
//! holds a set of `Utf8` label columns, a `Float64` `value` column, and, for the
//! instant-selector shape, `timestamp`/`sample_timestamp` index columns. This
//! module wraps that input in a `DataFusion`
//! [`Aggregate`](LogicalPlan::Aggregate) that collapses the rows into per-group
//! results. The aggregate maps Prometheus grouping semantics onto GROUP BY:
//!
//! - `by (l...)` groups by exactly the listed label columns that are present in
//!   the input. A `by` label absent from every input series does not appear.
//!   Prometheus does the same and drops empty grouping labels.
//! - `without (l...)` groups by every input label column except the listed
//!   ones and except `__name__`. Prometheus `without` always drops the metric
//!   name.
//! - `by ()` collapses all series into a single group.
//!
//! Most per-op value aggregates use `DataFusion`'s built-in aggregate
//! expressions, which match Prometheus float semantics exactly. This includes
//! NaN propagation for `sum`/`avg`. Those aggregates are `sum`, `avg`, `count`
//! cast to `Float64`, and `group` as the constant `1.0`. `min`/`max` are the
//! exception: Arrow's built-in `min`/`max` order floats with `total_cmp` and so
//! propagate NaN, but Prometheus and the tree-walking interpreter ignore NaN.
//!
//! A group's extremum is over its non-NaN samples, and the result is NaN only
//! when every sample is NaN. So `min`/`max` lower onto the NaN-ignoring
//! [`prom_min_udaf`]/[`prom_max_udaf`] UDAFs instead. The result columns are the
//! grouping label columns plus the aggregated `value` column. The caller
//! reattaches the eval timestamp during result assembly.

use std::collections::BTreeSet;

use datafusion::{
    functions_aggregate::expr_fn::{avg, count, max, sum},
    logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, cast, col, lit},
};

use crate::{
    PromqlError,
    error::Result,
    functions::{prom_max_udaf, prom_min_udaf},
    planner::leaf::{SAMPLE_TIME_COLUMN, TIME_COLUMN, VALUE_COLUMN},
};

/// Result-value column that the aggregation projection emits.
///
/// The column reuses the leaf/rate `value` name, so the engine's batch-label
/// reader treats every other `Utf8` column as a grouping label.
pub const AGGREGATE_VALUE_COLUMN: &str = VALUE_COLUMN;

/// Synthetic per-row grouping column for an empty `PromQL` grouping.
///
/// An empty grouping is `by ()` or no modifier. `GROUP BY` over an empty key set
/// is SQL's "single global group", which emits one row even over an empty input.
/// Prometheus `sum by ()` over zero series yields the empty vector instead. A
/// group by a constant-valued real column makes the group key per-row. An empty
/// input then produces zero groups, which is the Prometheus behaviour, and a
/// non-empty input collapses to exactly one group.
///
/// This module drops the column at assembly, so it never appears in the
/// projected output.
const ALL_GROUP_COLUMN: &str = "__crabka_agg_all__";

/// The simple aggregation operators this module lowers.
///
/// The param ops `topk`/`bottomk`/`quantile`/`count_values`/`stddev`/`stdvar`
/// are out of scope and never reach here. The recursive planner returns
/// `Unsupported` for them, so the interpreter owns them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SimpleAggregateOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Group,
}

impl SimpleAggregateOp {
    /// Builds the per-group value aggregate expression over the `value` column.
    ///
    /// The expression is aliased to the output value column. `count` is cast to
    /// `Float64`, because Prometheus reports counts as floats. `group` is the
    /// constant `1.0` for each group.
    fn value_aggregate(self) -> Expr {
        let value = col(VALUE_COLUMN);
        match self {
            Self::Sum => sum(value),
            Self::Avg => avg(value),
            // Arrow's built-in min/max propagate NaN (total_cmp ordering);
            // Prometheus ignores it. Lower onto the NaN-ignoring UDAFs so the
            // operator path matches the interpreter bit-for-bit, including the
            // all-NaN -> NaN case.
            Self::Min => prom_min_udaf().call(vec![value]),
            Self::Max => prom_max_udaf().call(vec![value]),
            // COUNT yields Int64 in DataFusion; Prometheus reports a float.
            Self::Count => cast(count(value), arrow::datatypes::DataType::Float64),
            // `group` ignores the values entirely and emits 1.0 per group.
            // `max(1.0)` over a non-empty group is exactly 1.0 and needs no
            // value column, keeping the aggregate valid even when `value` is NaN.
            Self::Group => max(lit(1.0_f64)),
        }
        .alias(AGGREGATE_VALUE_COLUMN)
    }
}

/// How a `PromQL` aggregation selects its grouping labels.
#[derive(Clone, Debug)]
pub enum Grouping {
    /// `by (labels...)`: group by exactly these label columns.
    By(Vec<String>),
    /// `without (labels...)`: group by all input label columns except these and
    /// except `__name__`, which `without` always drops.
    Without(Vec<String>),
}

/// Wraps `input` in a `DataFusion` aggregate for `op grouping (<input>)`.
///
/// The aggregate implements that `PromQL` aggregation. `input` is the inner
/// planner plan. The `Utf8` columns of its output schema are the candidate
/// grouping labels: every column except the `value`/`timestamp`/
/// `sample_timestamp` index columns. The output of the returned plan is the
/// surviving grouping label columns plus the aggregated `value` column.
///
/// # Errors
///
/// Returns [`PromqlError::Exec`] if this function cannot build the aggregate
/// plan.
pub fn plan_simple_aggregate(
    input: LogicalPlan,
    op: SimpleAggregateOp,
    grouping: &Grouping,
) -> Result<LogicalPlan> {
    let input_labels = input_label_columns(&input);
    let group_labels = resolve_group_labels(&input_labels, grouping);

    // Drop no-value input rows (a NULL `value`, the rate/`*_over_time` UDF's
    // "no value" marker) before grouping. The interpreter omits no-value series
    // from a group entirely, so a group whose members are all no-value forms no
    // result row at all, and `count` counts only value-bearing series. Filtering
    // here reproduces that exactly: such rows never reach the aggregate, so the
    // group either disappears (all no-value) or aggregates over its value-bearing
    // members only. A genuine NaN value is non-null, so it survives the filter and
    // propagates (e.g. `sum` over a group holding a genuine NaN yields NaN). For a
    // selector inner (whose `value` is non-nullable) the filter is a no-op.
    let input = LogicalPlanBuilder::from(input)
        .filter(col(VALUE_COLUMN).is_not_null())
        .map_err(|error| PromqlError::Exec(error.to_string()))?
        .build()
        .map_err(|error| PromqlError::Exec(error.to_string()))?;

    // When there is no grouping label, group by a synthetic constant-valued
    // per-row column so an empty input yields zero groups (matching Prometheus'
    // empty vector) rather than SQL's single global-aggregate row. The column is
    // projected away in the final result. Only the `value` column is needed
    // downstream (the aggregate reads it; `group` ignores it), so the synthetic
    // projection carries just `value` plus the group column.
    let (input, group_exprs): (LogicalPlan, Vec<Expr>) = if group_labels.is_empty() {
        let projected = LogicalPlanBuilder::from(input)
            .project(vec![col(VALUE_COLUMN), lit("").alias(ALL_GROUP_COLUMN)])
            .map_err(|error| PromqlError::Exec(error.to_string()))?
            .build()
            .map_err(|error| PromqlError::Exec(error.to_string()))?;
        (projected, vec![col(ALL_GROUP_COLUMN)])
    } else {
        (input, group_labels.iter().map(col).collect())
    };
    let aggr_exprs = vec![op.value_aggregate()];

    let aggregated = LogicalPlanBuilder::from(input)
        .aggregate(group_exprs, aggr_exprs)
        .map_err(|error| PromqlError::Exec(error.to_string()))?
        .build()
        .map_err(|error| PromqlError::Exec(error.to_string()))?;

    // Project the grouping labels through plus the value column. This pins the
    // column order the engine's batch reader expects, keeps the value column
    // last, and drops the synthetic all-group column when one was injected.
    let mut projections: Vec<Expr> = group_labels.iter().map(col).collect();
    projections.push(col(AGGREGATE_VALUE_COLUMN));
    LogicalPlanBuilder::from(aggregated)
        .project(projections)
        .map_err(|error| PromqlError::Exec(error.to_string()))?
        .build()
        .map_err(|error| PromqlError::Exec(error.to_string()))
}

/// The `Utf8` label columns of an inner plan's output schema, in schema order.
///
/// The `value`/`timestamp`/`sample_timestamp` columns are the index and value
/// columns of the operator chain. They are never labels.
fn input_label_columns(input: &LogicalPlan) -> Vec<String> {
    input
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .filter(|name| name != VALUE_COLUMN && name != TIME_COLUMN && name != SAMPLE_TIME_COLUMN)
        .collect()
}

/// Maps a `PromQL` [`Grouping`] onto the concrete grouping label columns.
///
/// The result is intersected with the labels present in the input. `by` keeps
/// the listed labels in their given order and drops each label that is not
/// present. `without` keeps every present label except the listed ones and
/// `__name__`.
fn resolve_group_labels(input_labels: &[String], grouping: &Grouping) -> Vec<String> {
    match grouping {
        Grouping::By(labels) => {
            let present: BTreeSet<&String> = input_labels.iter().collect();
            // Preserve the user's `by` order; drop labels absent from the input.
            let mut seen = BTreeSet::new();
            labels
                .iter()
                .filter(|name| present.contains(name) && seen.insert((*name).clone()))
                .cloned()
                .collect()
        }
        Grouping::Without(labels) => {
            let excluded: BTreeSet<&String> = labels.iter().collect();
            input_labels
                .iter()
                .filter(|name| name.as_str() != "__name__" && !excluded.contains(name))
                .cloned()
                .collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Float64Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::{catalog::MemTable, prelude::SessionContext};

    use super::*;

    /// Builds a leaf plan over an in-memory table like an instant-selector output.
    ///
    /// The table has the `job` and `group` labels plus
    /// `timestamp`/`value`/`sample_timestamp`.
    async fn selector_like_leaf(ctx: &SessionContext, rows: &[(&str, &str, f64)]) -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            Field::new("group", DataType::Utf8, false),
            Field::new("job", DataType::Utf8, false),
            Field::new(TIME_COLUMN, DataType::Int64, false),
            Field::new(VALUE_COLUMN, DataType::Float64, false),
            Field::new(SAMPLE_TIME_COLUMN, DataType::Int64, false),
        ]));
        let groups = StringArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>());
        let jobs = StringArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>());
        let ts = Int64Array::from(rows.iter().map(|_| 0_i64).collect::<Vec<_>>());
        let value = Float64Array::from(rows.iter().map(|r| r.2).collect::<Vec<_>>());
        let sample_ts = Int64Array::from(rows.iter().map(|_| 0_i64).collect::<Vec<_>>());
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(groups),
                Arc::new(jobs),
                Arc::new(ts),
                Arc::new(value),
                Arc::new(sample_ts),
            ],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("agg_leaf", Arc::new(table)).unwrap();
        ctx.table("agg_leaf")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap()
    }

    /// Like [`selector_like_leaf`], but with a nullable `value` column.
    ///
    /// A `None` row models the "no value" NULL cell of a rate or `*_over_time`
    /// UDF. This drives the pre-aggregate NULL filter: the planner must drop
    /// such rows before grouping, exactly as the interpreter omits no-value
    /// series.
    async fn nullable_leaf(
        ctx: &SessionContext,
        rows: &[(&str, &str, Option<f64>)],
    ) -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            Field::new("group", DataType::Utf8, false),
            Field::new("job", DataType::Utf8, false),
            Field::new(TIME_COLUMN, DataType::Int64, false),
            Field::new(VALUE_COLUMN, DataType::Float64, true),
            Field::new(SAMPLE_TIME_COLUMN, DataType::Int64, false),
        ]));
        let groups = StringArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>());
        let jobs = StringArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>());
        let ts = Int64Array::from(rows.iter().map(|_| 0_i64).collect::<Vec<_>>());
        let value = Float64Array::from(rows.iter().map(|r| r.2).collect::<Vec<_>>());
        let sample_ts = Int64Array::from(rows.iter().map(|_| 0_i64).collect::<Vec<_>>());
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(groups),
                Arc::new(jobs),
                Arc::new(ts),
                Arc::new(value),
                Arc::new(sample_ts),
            ],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("agg_leaf", Arc::new(table)).unwrap();
        ctx.table("agg_leaf")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap()
    }

    async fn run(plan: LogicalPlan, ctx: &SessionContext) -> Vec<(Vec<(String, String)>, f64)> {
        let batches = ctx
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut out = Vec::new();
        for batch in &batches {
            let value = batch
                .column_by_name(AGGREGATE_VALUE_COLUMN)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let mut labels = Vec::new();
                for (index, field) in batch.schema().fields().iter().enumerate() {
                    if field.name() == AGGREGATE_VALUE_COLUMN {
                        continue;
                    }
                    let column = batch
                        .column(index)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    labels.push((field.name().clone(), column.value(row).to_string()));
                }
                labels.sort();
                out.push((labels, value.value(row)));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    #[tokio::test]
    async fn sum_by_collapses_to_group_labels() {
        let ctx = SessionContext::new();
        let leaf = selector_like_leaf(
            &ctx,
            &[
                ("prod", "api", 1.0),
                ("prod", "db", 2.0),
                ("canary", "api", 4.0),
            ],
        )
        .await;
        let plan = plan_simple_aggregate(
            leaf,
            SimpleAggregateOp::Sum,
            &Grouping::By(vec!["group".into()]),
        )
        .unwrap();
        let got = run(plan, &ctx).await;
        assert2::assert!(
            got == vec![
                (vec![("group".to_string(), "canary".to_string())], 4.0),
                (vec![("group".to_string(), "prod".to_string())], 3.0),
            ]
        );
    }

    #[tokio::test]
    async fn sum_without_drops_listed_and_name() {
        let ctx = SessionContext::new();
        let leaf = selector_like_leaf(
            &ctx,
            &[
                ("prod", "api", 1.0),
                ("prod", "db", 2.0),
                ("canary", "api", 4.0),
            ],
        )
        .await;
        // without (job) -> group by `group`.
        let plan = plan_simple_aggregate(
            leaf,
            SimpleAggregateOp::Sum,
            &Grouping::Without(vec!["job".into()]),
        )
        .unwrap();
        let got = run(plan, &ctx).await;
        assert2::assert!(
            got == vec![
                (vec![("group".to_string(), "canary".to_string())], 4.0),
                (vec![("group".to_string(), "prod".to_string())], 3.0),
            ]
        );
    }

    #[tokio::test]
    async fn sum_by_empty_collapses_all() {
        let ctx = SessionContext::new();
        let leaf = selector_like_leaf(
            &ctx,
            &[
                ("prod", "api", 1.0),
                ("prod", "db", 2.0),
                ("canary", "api", 4.0),
            ],
        )
        .await;
        let plan =
            plan_simple_aggregate(leaf, SimpleAggregateOp::Sum, &Grouping::By(vec![])).unwrap();
        let got = run(plan, &ctx).await;
        assert2::assert!(got == vec![(vec![], 7.0)]);
    }

    #[tokio::test]
    async fn count_and_group_yield_floats() {
        let cases = [
            (
                SimpleAggregateOp::Count,
                [
                    ("prod", "api", 1.0),
                    ("prod", "db", 2.0),
                    ("canary", "api", 4.0),
                ],
                vec![
                    (vec![("group".to_string(), "canary".to_string())], 1.0),
                    (vec![("group".to_string(), "prod".to_string())], 2.0),
                ],
            ),
            (
                SimpleAggregateOp::Group,
                [
                    ("prod", "api", 9.0),
                    ("prod", "db", 2.0),
                    ("canary", "api", 4.0),
                ],
                vec![
                    (vec![("group".to_string(), "canary".to_string())], 1.0),
                    (vec![("group".to_string(), "prod".to_string())], 1.0),
                ],
            ),
        ];
        for (op, rows, want) in cases {
            let ctx = SessionContext::new();
            let leaf = selector_like_leaf(&ctx, &rows).await;
            let plan =
                plan_simple_aggregate(leaf, op, &Grouping::By(vec!["group".into()])).unwrap();
            let got = run(plan, &ctx).await;
            assert2::assert!(got == want);
        }
    }

    #[tokio::test]
    async fn empty_input_by_empty_yields_no_group() {
        // `sum by ()` over zero input rows must yield zero groups (Prometheus
        // empty vector), not SQL's single global-aggregate row.
        let ctx = SessionContext::new();
        let leaf = selector_like_leaf(&ctx, &[]).await;
        let plan =
            plan_simple_aggregate(leaf, SimpleAggregateOp::Sum, &Grouping::By(vec![])).unwrap();
        let got = run(plan, &ctx).await;
        assert2::assert!(got.is_empty());
    }

    #[tokio::test]
    async fn empty_input_by_label_yields_no_group() {
        let ctx = SessionContext::new();
        let leaf = selector_like_leaf(&ctx, &[]).await;
        let plan = plan_simple_aggregate(
            leaf,
            SimpleAggregateOp::Count,
            &Grouping::By(vec!["group".into()]),
        )
        .unwrap();
        let got = run(plan, &ctx).await;
        assert2::assert!(got.is_empty());
    }

    #[tokio::test]
    async fn sum_propagates_nan() {
        let ctx = SessionContext::new();
        let leaf =
            selector_like_leaf(&ctx, &[("prod", "api", 1.0), ("prod", "db", f64::NAN)]).await;
        let plan = plan_simple_aggregate(
            leaf,
            SimpleAggregateOp::Sum,
            &Grouping::By(vec!["group".into()]),
        )
        .unwrap();
        let got = run(plan, &ctx).await;
        assert2::assert!(got.len() == 1);
        assert2::assert!(got[0].1.is_nan());
    }

    #[tokio::test]
    async fn min_max_ignore_nan_over_mixed_group() {
        // A group mixing genuine NaN with finite samples: Prometheus takes the
        // extremum over the non-NaN values (NaN ignored), unlike Arrow's built-in
        // min/max which propagate NaN.
        for (op, want) in [
            (SimpleAggregateOp::Min, 1.0_f64),
            (SimpleAggregateOp::Max, 3.0_f64),
        ] {
            let ctx = SessionContext::new();
            let leaf = selector_like_leaf(
                &ctx,
                &[
                    ("prod", "api", f64::NAN),
                    ("prod", "db", 3.0),
                    ("prod", "x", 1.0),
                    ("prod", "y", f64::NAN),
                ],
            )
            .await;
            let plan =
                plan_simple_aggregate(leaf, op, &Grouping::By(vec!["group".into()])).unwrap();
            let got = run(plan, &ctx).await;
            assert2::assert!(got.len() == 1);
            assert2::assert!(got[0].1.to_bits() == want.to_bits());
        }
    }

    #[tokio::test]
    async fn min_max_over_all_nan_group_yield_nan_and_keep_series() {
        // Every sample in the group is NaN: Prometheus keeps the series with a
        // NaN result (it does not drop the group).
        for op in [SimpleAggregateOp::Min, SimpleAggregateOp::Max] {
            let ctx = SessionContext::new();
            let leaf =
                selector_like_leaf(&ctx, &[("prod", "api", f64::NAN), ("prod", "db", f64::NAN)])
                    .await;
            let plan =
                plan_simple_aggregate(leaf, op, &Grouping::By(vec!["group".into()])).unwrap();
            let got = run(plan, &ctx).await;
            assert2::assert!(got.len() == 1);
            assert2::assert!(got[0].1.is_nan());
        }
    }

    #[tokio::test]
    async fn all_null_group_yields_no_row() {
        // Every member of group g="x" is a NULL (no-value) row; the pre-aggregate
        // filter drops them, so the group forms no result row at all — matching
        // the interpreter, which never forms a group with no value-bearing sample.
        let ctx = SessionContext::new();
        let leaf = nullable_leaf(
            &ctx,
            &[
                ("x", "api", None),
                ("x", "db", None),
                ("y", "api", Some(3.0)),
            ],
        )
        .await;
        let plan = plan_simple_aggregate(
            leaf,
            SimpleAggregateOp::Sum,
            &Grouping::By(vec!["group".into()]),
        )
        .unwrap();
        let got = run(plan, &ctx).await;
        // Only group y survives; the all-NULL group x produces no row.
        assert2::assert!(got == vec![(vec![("group".to_string(), "y".to_string())], 3.0)]);
    }

    #[tokio::test]
    async fn count_skips_null_rows() {
        // A group mixing NULL (no-value) rows with value-bearing rows: `count`
        // counts only the value-bearing series (NULLs dropped pre-aggregate), and
        // a genuine NaN value is non-null so it IS counted.
        let ctx = SessionContext::new();
        let leaf = nullable_leaf(
            &ctx,
            &[
                ("prod", "api", Some(1.0)),
                ("prod", "db", None),
                ("prod", "x", Some(f64::NAN)),
                ("prod", "y", None),
            ],
        )
        .await;
        let plan = plan_simple_aggregate(
            leaf,
            SimpleAggregateOp::Count,
            &Grouping::By(vec!["group".into()]),
        )
        .unwrap();
        let got = run(plan, &ctx).await;
        // 2 value-bearing rows (1.0 and the genuine NaN); the two NULLs are
        // dropped before counting.
        assert2::assert!(got == vec![(vec![("group".to_string(), "prod".to_string())], 2.0)]);
    }

    #[tokio::test]
    async fn sum_drops_null_keeps_genuine_nan() {
        // A NULL (no-value) member is excluded from the sum; a genuine NaN member
        // is kept and propagates, so the group's sum is NaN (not the value of the
        // single finite member, and not absent).
        let ctx = SessionContext::new();
        let leaf = nullable_leaf(
            &ctx,
            &[
                ("prod", "api", Some(2.0)),
                ("prod", "db", None),
                ("prod", "x", Some(f64::NAN)),
            ],
        )
        .await;
        let plan = plan_simple_aggregate(
            leaf,
            SimpleAggregateOp::Sum,
            &Grouping::By(vec!["group".into()]),
        )
        .unwrap();
        let got = run(plan, &ctx).await;
        assert2::assert!(got.len() == 1);
        assert2::assert!(got[0].1.is_nan());
    }
}
