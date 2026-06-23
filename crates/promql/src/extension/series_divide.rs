//! `SeriesDivide`: split sorted input into contiguous single-series batches.

use std::fmt;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

/// Logical node: partition the input into per-series batches.
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct SeriesDivide {
    pub tag_columns: Vec<String>,
    pub input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for SeriesDivide {
    fn name(&self) -> &'static str {
        "SeriesDivide"
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
        write!(f, "PromSeriesDivide: tags={:?}", self.tag_columns)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "SeriesDivide expects no expressions and one input".to_string(),
            ));
        }
        Ok(Self {
            tag_columns: self.tag_columns.clone(),
            input: inputs.swap_remove(0),
        })
    }
}

/// Physical node that emits one batch per contiguous series run.
#[derive(Debug)]
pub struct SeriesDivideExec {
    tag_columns: Vec<String>,
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl SeriesDivideExec {
    #[must_use]
    pub fn new(tag_columns: Vec<String>, input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::clone(input.properties());
        Self {
            tag_columns,
            input,
            properties,
        }
    }

    fn split_batch(tag_columns: &[String], batch: RecordBatch) -> DfResult<Vec<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(vec![batch]);
        }
        let mut boundaries = vec![0_usize];
        for row in 1..batch.num_rows() {
            if Self::series_changed(tag_columns, &batch, row - 1, row)? {
                boundaries.push(row);
            }
        }
        boundaries.push(batch.num_rows());

        Ok(boundaries
            .windows(2)
            .filter_map(|window| {
                let start = window[0];
                let len = window[1] - start;
                (len > 0).then(|| batch.slice(start, len))
            })
            .collect())
    }

    fn series_changed(
        tag_columns: &[String],
        batch: &RecordBatch,
        left: usize,
        right: usize,
    ) -> DfResult<bool> {
        for column_name in tag_columns {
            let column = batch.column_by_name(column_name).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "SeriesDivide tag column `{column_name}` not found"
                ))
            })?;
            // A NULL label entry (ABSENT label) must not compare equal to a
            // present-but-empty (`""`) entry: `array_value_to_string` renders
            // both as `""`, so compare nullness first. Two series that differ
            // only in whether a label is absent vs present-empty are distinct.
            let left_null = column.is_null(left);
            let right_null = column.is_null(right);
            if left_null != right_null {
                return Ok(true);
            }
            if left_null {
                // Both NULL on this column: identical here, check the next.
                continue;
            }
            let left_value = array_value_to_string(column.as_ref(), left)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            let right_value = array_value_to_string(column.as_ref(), right)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            if left_value != right_value {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl DisplayAs for SeriesDivideExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PromSeriesDivideExec: tags={:?}", self.tag_columns)
    }
}

impl ExecutionPlan for SeriesDivideExec {
    fn name(&self) -> &'static str {
        "SeriesDivideExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "SeriesDivideExec expects one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            self.tag_columns.clone(),
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
        let tag_columns = self.tag_columns.clone();
        let stream = input
            .map(move |batch| match batch {
                Ok(batch) => match Self::split_batch(&tag_columns, batch) {
                    Ok(batches) => futures::stream::iter(batches.into_iter().map(Ok)).boxed(),
                    Err(error) => futures::stream::iter([Err(error)]).boxed(),
                },
                Err(error) => futures::stream::iter([Err(error)]).boxed(),
            })
            .flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use assert2::assert;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn input_batch() -> RecordBatch {
        let job = StringArray::from(vec!["a", "a", "b"]);
        let ts = Int64Array::from(vec![1_i64, 2, 1]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("job", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
        ]));
        RecordBatch::try_new(schema, vec![Arc::new(job), Arc::new(ts)]).unwrap()
    }

    #[tokio::test]
    async fn divides_into_single_series_batches() {
        let batch = input_batch();
        let schema = batch.schema();
        let mem = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let exec = SeriesDivideExec::new(vec!["job".to_string()], mem);

        let ctx = SessionContext::new();
        let out = collect(Arc::new(exec), ctx.task_ctx()).await.unwrap();

        for batch in &out {
            let job = batch
                .column_by_name("job")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let first = job.value(0);
            assert!((0..job.len()).all(|index| job.value(index) == first));
        }
        let total = out.iter().map(RecordBatch::num_rows).sum::<usize>();
        assert!(total == 3);
    }
}
