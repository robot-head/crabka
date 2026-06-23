//! `InstantManipulate`: step-grid instant-vector lookback selection.

use std::fmt;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, UInt32Array};
use arrow::compute::take;
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

/// Logical node: instant-vector selection over a step grid.
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct InstantManipulate {
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
    pub lookback_delta_ms: i64,
    pub time_index: String,
    pub field_column: String,
    pub input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for InstantManipulate {
    fn name(&self) -> &'static str {
        "InstantManipulate"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PromInstantManipulate: start_ms={}, end_ms={}, step_ms={}, lookback_delta_ms={}",
            self.start_ms, self.end_ms, self.step_ms, self.lookback_delta_ms
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "InstantManipulate expects no expressions and one input".to_string(),
            ));
        }
        Ok(Self {
            start_ms: self.start_ms,
            end_ms: self.end_ms,
            step_ms: self.step_ms,
            lookback_delta_ms: self.lookback_delta_ms,
            time_index: self.time_index.clone(),
            field_column: self.field_column.clone(),
            input: inputs.swap_remove(0),
        })
    }
}

/// Physical node that emits one selected sample per valid grid step.
#[derive(Debug)]
pub struct InstantManipulateExec {
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    lookback_delta_ms: i64,
    time_index: String,
    field_column: String,
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl InstantManipulateExec {
    #[must_use]
    pub fn new(
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
        lookback_delta_ms: i64,
        time_index: String,
        field_column: String,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let properties = Arc::clone(input.properties());
        Self {
            start_ms,
            end_ms,
            step_ms,
            lookback_delta_ms,
            time_index,
            field_column,
            input,
            properties,
        }
    }

    fn manipulate_batch(&self, batch: &RecordBatch) -> DfResult<RecordBatch> {
        if self.step_ms <= 0 {
            return Err(DataFusionError::Execution(format!(
                "step_ms must be positive, got {}",
                self.step_ms
            )));
        }
        let time_column_index = batch
            .schema()
            .index_of(&self.time_index)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let timestamps = batch
            .column(time_column_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "InstantManipulate time column `{}` must be Int64",
                    self.time_index
                ))
            })?;
        let values = batch
            .column_by_name(&self.field_column)
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "InstantManipulate field column `{}` must be Float64",
                    self.field_column
                ))
            })?;

        let mut selected_rows = Vec::new();
        let mut output_timestamps = Vec::new();
        let mut sample_cursor = 0_usize;
        let mut grid_ts = self.start_ms;
        while grid_ts <= self.end_ms {
            while sample_cursor < timestamps.len() && timestamps.value(sample_cursor) <= grid_ts {
                sample_cursor += 1;
            }
            if let Some(row) = sample_cursor.checked_sub(1) {
                let sample_ts = timestamps.value(row);
                // Drop the selected sample only when it is Prometheus' stale-NaN
                // marker (the series has been terminated); a genuine NaN value is
                // kept as a NaN sample, matching `engine::eval_instant_selector`.
                if grid_ts - sample_ts < self.lookback_delta_ms
                    && !super::is_stale_nan(values.value(row))
                {
                    selected_rows.push(
                        u32::try_from(row)
                            .map_err(|error| DataFusionError::Execution(error.to_string()))?,
                    );
                    output_timestamps.push(grid_ts);
                }
            }
            grid_ts = grid_ts
                .checked_add(self.step_ms)
                .ok_or_else(|| DataFusionError::Execution("grid timestamp overflow".to_string()))?;
        }

        let take_indices = UInt32Array::from_iter_values(selected_rows);
        let mut columns = Vec::with_capacity(batch.num_columns());
        for (index, column) in batch.columns().iter().enumerate() {
            if index == time_column_index {
                columns.push(Arc::new(Int64Array::from_iter_values(
                    output_timestamps.iter().copied(),
                )) as ArrayRef);
            } else {
                columns.push(take(column.as_ref(), &take_indices, None)?);
            }
        }

        RecordBatch::try_new(batch.schema(), columns)
            .map_err(|error| DataFusionError::Execution(error.to_string()))
    }
}

impl DisplayAs for InstantManipulateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PromInstantManipulateExec: start_ms={}, end_ms={}, step_ms={}, lookback_delta_ms={}",
            self.start_ms, self.end_ms, self.step_ms, self.lookback_delta_ms
        )
    }
}

impl ExecutionPlan for InstantManipulateExec {
    fn name(&self) -> &'static str {
        "InstantManipulateExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "InstantManipulateExec expects one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            self.start_ms,
            self.end_ms,
            self.step_ms,
            self.lookback_delta_ms,
            self.time_index.clone(),
            self.field_column.clone(),
            children.swap_remove(0),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = self.schema();
        let this = Self {
            start_ms: self.start_ms,
            end_ms: self.end_ms,
            step_ms: self.step_ms,
            lookback_delta_ms: self.lookback_delta_ms,
            time_index: self.time_index.clone(),
            field_column: self.field_column.clone(),
            input: Arc::clone(&self.input),
            properties: Arc::clone(&self.properties),
        };
        let stream = input.map(move |batch| batch.and_then(|batch| this.manipulate_batch(&batch)));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Float64Array, Int64Array};
    use arrow::compute::concat_batches;
    use arrow::datatypes::{DataType, Field, Schema};
    use assert2::assert;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    use super::*;

    #[tokio::test]
    async fn selects_latest_sample_within_lookback_for_each_grid_step() {
        let ts = Int64Array::from(vec![0_i64, 60_000]);
        let val = Float64Array::from(vec![1.0, 2.0]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ts), Arc::new(val)]).unwrap();
        let mem = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();

        let exec = InstantManipulateExec::new(
            0,
            120_000,
            60_000,
            300_000,
            "timestamp".into(),
            "value".into(),
            mem,
        );
        let ctx = SessionContext::new();
        let out = collect(Arc::new(exec), ctx.task_ctx()).await.unwrap();

        let merged = concat_batches(&out[0].schema(), &out).unwrap();
        let ts = merged
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let val = merged
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            (0..ts.len())
                .map(|index| ts.value(index))
                .collect::<Vec<_>>()
                == vec![0, 60_000, 120_000]
        );
        assert!(
            (0..val.len())
                .map(|index| val.value(index))
                .collect::<Vec<_>>()
                == vec![1.0, 2.0, 2.0]
        );
    }

    #[tokio::test]
    async fn keeps_genuine_nan_and_drops_stale_nan_marker() {
        // Two series: one whose latest in-window sample is a genuine NaN, and
        // one whose latest in-window sample is Prometheus' stale-NaN marker.
        // The genuine NaN must survive selection as a NaN value; the stale
        // marker must suppress its grid step entirely.
        let stale = f64::from_bits(super::super::STALE_NAN_BITS);
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        // Two single-row batches so each series is normalized independently.
        let genuine = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![0_i64])),
                Arc::new(Float64Array::from(vec![f64::NAN])),
            ],
        )
        .unwrap();
        let staled = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![0_i64])),
                Arc::new(Float64Array::from(vec![stale])),
            ],
        )
        .unwrap();
        let mem =
            MemorySourceConfig::try_new_exec(&[vec![genuine], vec![staled]], schema, None).unwrap();

        let exec = InstantManipulateExec::new(
            0,
            0,
            60_000,
            300_000,
            "timestamp".into(),
            "value".into(),
            mem,
        );
        let ctx = SessionContext::new();
        let out = collect(Arc::new(exec), ctx.task_ctx()).await.unwrap();

        let merged = concat_batches(&out[0].schema(), &out).unwrap();
        let val = merged
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // Exactly one row survives: the genuine NaN. The stale marker is dropped.
        assert!(val.len() == 1);
        assert!(val.value(0).is_nan());
        assert!(!super::super::is_stale_nan(val.value(0)));
    }
}
