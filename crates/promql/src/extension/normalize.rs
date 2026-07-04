//! `SeriesNormalize`: apply offset, sort by timestamp, and drop stale values.

use std::{fmt, sync::Arc};

use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, UInt32Array},
    compute::take,
    record_batch::RecordBatch,
};
use datafusion::{
    common::{DataFusionError, Result as DfResult},
    execution::TaskContext,
    logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
        stream::RecordBatchStreamAdapter,
    },
};
use futures::StreamExt;

/// Logical node: normalize each single-series batch.
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct SeriesNormalize {
    pub offset_ms: i64,
    pub time_index: String,
    pub need_filter_out_nan: bool,
    pub input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for SeriesNormalize {
    fn name(&self) -> &'static str {
        "SeriesNormalize"
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
            "PromSeriesNormalize: time={}, offset_ms={}, filter_nan={}",
            self.time_index, self.offset_ms, self.need_filter_out_nan
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "SeriesNormalize expects no expressions and one input".to_string(),
            ));
        }
        Ok(Self {
            offset_ms: self.offset_ms,
            time_index: self.time_index.clone(),
            need_filter_out_nan: self.need_filter_out_nan,
            input: inputs.swap_remove(0),
        })
    }
}

/// Physical node that normalizes one-series batches.
#[derive(Debug)]
pub struct SeriesNormalizeExec {
    offset_ms: i64,
    time_index: String,
    need_filter_out_nan: bool,
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl SeriesNormalizeExec {
    #[must_use]
    pub fn new(
        offset_ms: i64,
        time_index: String,
        need_filter_out_nan: bool,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let properties = Arc::clone(input.properties());
        Self {
            offset_ms,
            time_index,
            need_filter_out_nan,
            input,
            properties,
        }
    }

    fn normalize_batch(&self, batch: &RecordBatch) -> DfResult<RecordBatch> {
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
                    "SeriesNormalize time column `{}` must be Int64",
                    self.time_index
                ))
            })?;
        let values = batch
            .column_by_name("value")
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>());

        let mut rows = (0..batch.num_rows())
            .filter(|&row| {
                !self.need_filter_out_nan
                    || values.is_none_or(|value_array| !value_array.value(row).is_nan())
            })
            .map(|row| {
                timestamps
                    .value(row)
                    .checked_add(self.offset_ms)
                    .map(|ts| (row, ts))
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "timestamp offset overflow at row {row}"
                        ))
                    })
            })
            .collect::<DfResult<Vec<_>>>()?;
        rows.sort_by_key(|&(row, ts)| (ts, row));

        let take_indices = UInt32Array::from_iter_values(
            rows.iter()
                .map(|&(row, _)| u32::try_from(row))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| DataFusionError::Execution(error.to_string()))?,
        );
        let mut columns = Vec::with_capacity(batch.num_columns());
        for (index, column) in batch.columns().iter().enumerate() {
            if index == time_column_index {
                columns.push(
                    Arc::new(Int64Array::from_iter_values(rows.iter().map(|&(_, ts)| ts)))
                        as ArrayRef,
                );
            } else {
                columns.push(take(column.as_ref(), &take_indices, None)?);
            }
        }

        RecordBatch::try_new(batch.schema(), columns)
            .map_err(|error| DataFusionError::Execution(error.to_string()))
    }
}

impl DisplayAs for SeriesNormalizeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PromSeriesNormalizeExec: time={}, offset_ms={}, filter_nan={}",
            self.time_index, self.offset_ms, self.need_filter_out_nan
        )
    }
}

impl ExecutionPlan for SeriesNormalizeExec {
    fn name(&self) -> &'static str {
        "SeriesNormalizeExec"
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
                "SeriesNormalizeExec expects one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            self.offset_ms,
            self.time_index.clone(),
            self.need_filter_out_nan,
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
            offset_ms: self.offset_ms,
            time_index: self.time_index.clone(),
            need_filter_out_nan: self.need_filter_out_nan,
            input: Arc::clone(&self.input),
            properties: Arc::clone(&self.properties),
        };
        let stream = input.map(move |batch| batch.and_then(|batch| this.normalize_batch(&batch)));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Array, Float64Array, Int64Array},
        compute::concat_batches,
        datatypes::{DataType, Field, Schema},
    };
    use assert2::{assert, check};
    use datafusion::{
        catalog::MemTable,
        datasource::memory::MemorySourceConfig,
        logical_expr::{Extension, UserDefinedLogicalNodeCore, col},
        physical_plan::{collect, display::DisplayableExecutionPlan},
        prelude::SessionContext,
    };

    use super::*;

    async fn logical_input() -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![100_i64])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("leaf", Arc::new(table)).unwrap();
        ctx.table("leaf")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap()
    }

    fn physical_input() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![100_i64])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    #[tokio::test]
    async fn logical_node_reports_identity_explain_and_rejects_bad_rewrites() {
        let input = logical_input().await;
        let node = SeriesNormalize {
            offset_ms: 123,
            time_index: "timestamp".to_string(),
            need_filter_out_nan: true,
            input: input.clone(),
        };

        check!(UserDefinedLogicalNodeCore::name(&node) == "SeriesNormalize");
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        });
        let explain = format!("{plan}");
        check!(
            explain
                .starts_with("PromSeriesNormalize: time=timestamp, offset_ms=123, filter_nan=true")
        );
        check!(explain.contains("TableScan: leaf projection=[timestamp, value]"));

        let node = SeriesNormalize {
            offset_ms: 123,
            time_index: "timestamp".to_string(),
            need_filter_out_nan: true,
            input: input.clone(),
        };
        check!(
            node.with_exprs_and_inputs(vec![col("timestamp")], vec![input.clone()])
                .is_err()
        );
        check!(node.with_exprs_and_inputs(vec![], vec![]).is_err());
        let rewritten = node
            .with_exprs_and_inputs(vec![], vec![input.clone()])
            .expect("valid rewrite");
        assert!(
            rewritten
                == SeriesNormalize {
                    offset_ms: 123,
                    time_index: "timestamp".to_string(),
                    need_filter_out_nan: true,
                    input,
                }
        );
    }

    #[test]
    fn physical_node_reports_identity_display_ordering_and_rejects_bad_children() {
        let input = physical_input();
        let exec = Arc::new(SeriesNormalizeExec::new(
            123,
            "timestamp".to_string(),
            true,
            Arc::clone(&input),
        ));

        check!(exec.name() == "SeriesNormalizeExec");
        check!(
            format!(
                "{}",
                DisplayableExecutionPlan::new(exec.as_ref()).indent(false)
            ) == "PromSeriesNormalizeExec: time=timestamp, offset_ms=123, filter_nan=true\n  DataSourceExec: partitions=1, partition_sizes=[1]\n"
        );
        check!(exec.maintains_input_order() == vec![false]);
        check!(Arc::clone(&exec).with_new_children(vec![]).is_err());
        check!(
            Arc::clone(&exec)
                .with_new_children(vec![input])
                .expect("valid child rewrite")
                .name()
                == "SeriesNormalizeExec"
        );
    }

    #[tokio::test]
    async fn sorts_by_time_and_drops_nan() {
        let ts = Int64Array::from(vec![300_i64, 100, 200]);
        let val = Float64Array::from(vec![3.0, f64::NAN, 2.0]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ts), Arc::new(val)]).unwrap();
        let mem = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();

        let exec = SeriesNormalizeExec::new(0, "timestamp".into(), true, mem);
        let ctx = SessionContext::new();
        let out = collect(Arc::new(exec), ctx.task_ctx()).await.unwrap();

        let merged = concat_batches(&out[0].schema(), &out).unwrap();
        let ts = merged
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(
            (0..ts.len())
                .map(|index| ts.value(index))
                .collect::<Vec<_>>()
                == vec![200, 300]
        );
    }
}
