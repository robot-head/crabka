//! Physical planning for the custom `PromQL` logical operators.
//!
//! `DataFusion` does not know how to turn the [`SeriesDivide`],
//! [`SeriesNormalize`], and [`InstantManipulate`] logical nodes into
//! [`ExecutionPlan`]s on its own. This module supplies an [`ExtensionPlanner`]
//! that maps each logical node to its `Exec` counterpart, plus a
//! [`prom_session_context`] helper that builds a [`SessionContext`] wired with
//! that planner so `execute_logical_plan` can run the operator chain.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;

use super::instant_manipulate::{InstantManipulate, InstantManipulateExec};
use super::normalize::{SeriesNormalize, SeriesNormalizeExec};
use super::range_manipulate::{RangeManipulate, RangeManipulateExec};
use super::series_divide::{SeriesDivide, SeriesDivideExec};
use crate::functions::{
    register_aggregate_udafs, register_over_time_udfs, register_rate_udfs,
    register_scalar_math_udfs,
};

/// Maps the custom `PromQL` logical nodes to their physical `Exec` nodes.
#[derive(Debug, Default)]
pub struct PromExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for PromExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
        let any = node.as_any();
        if let Some(divide) = any.downcast_ref::<SeriesDivide>() {
            let input = single_input(physical_inputs)?;
            return Ok(Some(Arc::new(SeriesDivideExec::new(
                divide.tag_columns.clone(),
                input,
            ))));
        }
        if let Some(normalize) = any.downcast_ref::<SeriesNormalize>() {
            let input = single_input(physical_inputs)?;
            return Ok(Some(Arc::new(SeriesNormalizeExec::new(
                normalize.offset_ms,
                normalize.time_index.clone(),
                normalize.need_filter_out_nan,
                input,
            ))));
        }
        if let Some(instant) = any.downcast_ref::<InstantManipulate>() {
            let input = single_input(physical_inputs)?;
            return Ok(Some(Arc::new(InstantManipulateExec::new(
                instant.start_ms,
                instant.end_ms,
                instant.step_ms,
                instant.lookback_delta_ms,
                instant.time_index.clone(),
                instant.field_column.clone(),
                input,
            ))));
        }
        if let Some(range) = any.downcast_ref::<RangeManipulate>() {
            let input = single_input(physical_inputs)?;
            return Ok(Some(Arc::new(RangeManipulateExec::new(
                range.start_ms,
                range.end_ms,
                range.interval_ms,
                range.range_ms,
                range.time_index.clone(),
                range.field_column.clone(),
                input,
            ))));
        }
        Ok(None)
    }
}

fn single_input(physical_inputs: &[Arc<dyn ExecutionPlan>]) -> DfResult<Arc<dyn ExecutionPlan>> {
    match physical_inputs {
        [input] => Ok(Arc::clone(input)),
        _ => Err(datafusion::error::DataFusionError::Plan(
            "PromQL operator node expects exactly one input".to_string(),
        )),
    }
}

/// Query planner that teaches the default physical planner about the custom
/// `PromQL` operator nodes.
#[derive(Debug, Default)]
struct PromQueryPlanner;

#[async_trait]
impl QueryPlanner for PromQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(PromExtensionPlanner)]);
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Build a [`SessionContext`] whose physical planner understands the custom
/// `PromQL` operator nodes ([`SeriesDivide`], [`SeriesNormalize`],
/// [`InstantManipulate`], [`RangeManipulate`]) and whose function registry holds
/// the rate-family, `*_over_time`, and per-row scalar-math `ScalarUDF`s plus the
/// NaN-ignoring `prom_min`/`prom_max` aggregate UDAFs so a range-function,
/// scalar-math, or `min`/`max` aggregation can lower onto them.
#[must_use]
pub fn prom_session_context() -> SessionContext {
    // Pin single-partition execution. The custom PromQL operator chain
    // ([`SeriesNormalize`] / [`SeriesDivide`] / [`InstantManipulate`] /
    // [`RangeManipulate`]) assumes its input arrives as one ordered partition
    // (each series contiguous, sorted by fingerprint then timestamp). With the
    // default `target_partitions` = CPU count, DataFusion's `EnforceDistribution`
    // rule inserts a repartition ahead of the operator chain / aggregate that
    // scatters a series across partitions, silently producing wrong results —
    // reproduced deterministically at `target_partitions` in `2..=6` (e.g.
    // `COUNT(m) BY (job)` collapsing 4 series to 2 on the 2-4 core CI runners,
    // while a high-core dev box at 32 partitions happens to dodge it). Per-query
    // parallelism comes from the query-frontend's shard fan-out, not from
    // DataFusion intra-query partitioning, so pinning one partition costs nothing.
    let config = datafusion::prelude::SessionConfig::new().with_target_partitions(1);
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_query_planner(Arc::new(PromQueryPlanner))
        .build();
    let ctx = SessionContext::new_with_state(state);
    register_rate_udfs(&ctx);
    register_over_time_udfs(&ctx);
    register_scalar_math_udfs(&ctx);
    register_aggregate_udafs(&ctx);
    ctx
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use assert2::assert;
    use datafusion::catalog::MemTable;
    use datafusion::logical_expr::{Extension, LogicalPlan};

    use super::*;
    use crate::extension::instant_manipulate::InstantManipulate;
    use crate::extension::normalize::SeriesNormalize;
    use crate::extension::series_divide::SeriesDivide;

    #[tokio::test]
    async fn execute_logical_plan_runs_divide_normalize_instant_chain() {
        // Two series ("a" and "b") with two samples each, intentionally out of
        // timestamp order so SeriesNormalize must sort them.
        let job = StringArray::from(vec!["a", "a", "b", "b"]);
        let ts = Int64Array::from(vec![60_000_i64, 0, 60_000, 0]);
        let value = Float64Array::from(vec![2.0, 1.0, 20.0, 10.0]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("job", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(job), Arc::new(ts), Arc::new(value)],
        )
        .unwrap();

        let ctx = prom_session_context();
        let table = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
        ctx.register_table("leaf", Arc::new(table)).unwrap();
        let leaf = ctx
            .table("leaf")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();

        let divide = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesDivide {
                tag_columns: vec!["job".to_string()],
                input: leaf,
            }),
        });
        let normalize = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesNormalize {
                offset_ms: 0,
                time_index: "timestamp".to_string(),
                need_filter_out_nan: false,
                input: divide,
            }),
        });
        let instant = LogicalPlan::Extension(Extension {
            node: Arc::new(InstantManipulate {
                start_ms: 120_000,
                end_ms: 120_000,
                step_ms: 300_000,
                lookback_delta_ms: 300_000,
                time_index: "timestamp".to_string(),
                field_column: "value".to_string(),
                input: normalize,
            }),
        });

        let batches = ctx
            .execute_logical_plan(instant)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let mut got = Vec::new();
        for batch in &batches {
            let job = batch
                .column_by_name("job")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let value = batch
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                got.push((job.value(row).to_string(), value.value(row)));
            }
        }
        got.sort_by(|left, right| left.0.cmp(&right.0));

        // At grid step 120_000 within a 300_000 lookback, the latest sample for
        // each series (ts=60_000) is selected.
        assert!(got == vec![("a".to_string(), 2.0), ("b".to_string(), 20.0)]);
    }
}
