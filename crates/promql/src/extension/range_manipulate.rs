//! `RangeManipulate`: materialize range vectors over a step grid.
//!
//! # Output-schema / column contract
//!
//! `RangeManipulate` consumes a single series' time-sorted `(timestamp, value)`
//! batch (as produced downstream of [`SeriesNormalize`]) and folds the samples
//! into per-eval-step windows. The output schema produced by
//! [`build_extended_range_schema`] is, in column order:
//!
//! 1. Every **label column** of the input (any column that is neither the time
//!    index nor the value column) carried through **unchanged** in its original
//!    relative order. For each eval step these columns repeat the series' label
//!    values (one row per eval step).
//! 2. The eval **`timestamp`** column (reuses the input time-index column name):
//!    `Int64`, **scalar** — one value per aligned step on the `[start, end]`
//!    grid with stride `interval`. This is the instant `t` each window closes
//!    on. Downstream rate-family UDFs read this as the evaluation timestamp.
//! 3. A **`<time_index>_range`** column (e.g. `timestamp_range`): a `RangeArray`
//!    encoded as `Dictionary<Int64, List<Int64>>`. Cell `i` holds the *sample
//!    timestamps* whose timestamp falls in the window that closes at eval step
//!    `i`.
//! 4. A **`<value>_range`** column (e.g. `value_range`): a `RangeArray` encoded
//!    as `Dictionary<Int64, List<Float64>>`. Cell `i` holds the *sample values*
//!    aligned 1:1 with the timestamps in `<time_index>_range` cell `i`.
//!
//! The two `RangeArray` columns are always row-aligned with each other and with
//! the eval `timestamp` column. Decode them with
//! [`RangeArray::try_from_dict_array`].
//!
//! # Window semantics
//!
//! For eval timestamp `t` and range duration `range`, a sample at timestamp
//! `ts` is included in the window iff `t - range < ts <= t`. The window is
//! **left-open, right-closed** `(t - range, t]`: a sample exactly on the right
//! boundary (`ts == t`) is **included**; a sample exactly on the left edge
//! (`ts == t - range`) is **excluded**. This matches `PromQL` range-selector
//! semantics. Empty windows produce empty (zero-length) `RangeArray` cells.

use std::{fmt, sync::Arc};

use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, UInt32Array},
    compute::take,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{
    common::{DataFusionError, Result as DfResult},
    execution::TaskContext,
    logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
        stream::RecordBatchStreamAdapter,
    },
};
use futures::StreamExt;

use crate::range_array::RangeArray;

/// Suffix appended to the time-index and value columns to name their windowed
/// [`RangeArray`] counterparts in the extended schema.
pub const RANGE_SUFFIX: &str = "_range";

/// The eval-step grid paired with the `(offset, len)` windows that index the
/// sorted input rows for each step.
type StepWindows = (Vec<i64>, Vec<(u32, u32)>);

/// The Arrow `DataType` of a [`RangeArray`] column whose backing samples have
/// `value_type`.
///
/// A `RangeArray` is serialized as a dictionary of per-cell lists
/// (`Dictionary<Int64, List<value_type>>`), matching
/// [`RangeArray::into_dict_array`].
#[must_use]
fn range_array_type(value_type: DataType, nullable: bool) -> DataType {
    let item = Field::new("item", value_type, nullable);
    DataType::Dictionary(
        Box::new(DataType::Int64),
        Box::new(DataType::List(Arc::new(item))),
    )
}

/// Build the extended range-vector schema described in the module contract.
///
/// `input_schema` is the per-series scalar schema (label columns plus the
/// `time_index` `Int64` column and the `field_column` `Float64` column). The
/// returned schema carries the label columns through, keeps a scalar eval
/// `time_index` column, and appends the `<time_index>_range` and
/// `<field_column>_range` [`RangeArray`] columns.
#[must_use]
pub fn build_extended_range_schema(
    input_schema: &Schema,
    time_index: &str,
    field_column: &str,
) -> SchemaRef {
    let mut fields = Vec::with_capacity(input_schema.fields().len() + 2);

    // 1. Label columns, unchanged and in original order.
    for field in input_schema.fields() {
        let name = field.name();
        if name == time_index || name == field_column {
            continue;
        }
        fields.push(field.clone());
    }

    // 2. Scalar eval-timestamp column (reuses the time-index name).
    fields.push(Arc::new(Field::new(time_index, DataType::Int64, false)));

    // 3. Windowed timestamps RangeArray.
    fields.push(Arc::new(Field::new(
        format!("{time_index}{RANGE_SUFFIX}"),
        range_array_type(DataType::Int64, false),
        false,
    )));

    // 4. Windowed values RangeArray.
    fields.push(Arc::new(Field::new(
        format!("{field_column}{RANGE_SUFFIX}"),
        range_array_type(DataType::Float64, false),
        false,
    )));

    Arc::new(Schema::new(fields))
}

/// Logical node: materialize range vectors over a step grid.
///
/// `output_schema` is fully determined by the other fields, so it is excluded
/// from the manual `PartialEq`/`Eq`/`Hash`/`PartialOrd` impls (which the
/// `UserDefinedLogicalNodeCore` machinery requires) to keep node identity tied
/// to the logical parameters alone.
#[derive(Debug, Clone)]
pub struct RangeManipulate {
    pub start_ms: i64,
    pub end_ms: i64,
    pub interval_ms: i64,
    pub range_ms: i64,
    pub time_index: String,
    pub field_column: String,
    pub input: LogicalPlan,
    output_schema: datafusion::common::DFSchemaRef,
}

impl RangeManipulate {
    /// Construct the logical node and derive its extended output schema.
    pub fn new(
        start_ms: i64,
        end_ms: i64,
        interval_ms: i64,
        range_ms: i64,
        time_index: String,
        field_column: String,
        input: LogicalPlan,
    ) -> DfResult<Self> {
        let extended =
            build_extended_range_schema(input.schema().as_arrow(), &time_index, &field_column);
        let output_schema = Arc::new(datafusion::common::DFSchema::try_from(
            extended.as_ref().clone(),
        )?);
        Ok(Self {
            start_ms,
            end_ms,
            interval_ms,
            range_ms,
            time_index,
            field_column,
            input,
            output_schema,
        })
    }

    /// The logical parameters that define node identity (everything but the
    /// derived `output_schema`).
    fn identity(&self) -> (i64, i64, i64, i64, &str, &str, &LogicalPlan) {
        (
            self.start_ms,
            self.end_ms,
            self.interval_ms,
            self.range_ms,
            &self.time_index,
            &self.field_column,
            &self.input,
        )
    }
}

impl PartialEq for RangeManipulate {
    fn eq(&self, other: &Self) -> bool {
        self.identity() == other.identity()
    }
}

impl Eq for RangeManipulate {}

impl std::hash::Hash for RangeManipulate {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.identity().hash(state);
    }
}

impl PartialOrd for RangeManipulate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // `LogicalPlan` is not `Ord`; order by the scalar parameters only, which
        // is sufficient for the framework's deterministic-ordering needs.
        (
            self.start_ms,
            self.end_ms,
            self.interval_ms,
            self.range_ms,
            &self.time_index,
            &self.field_column,
        )
            .partial_cmp(&(
                other.start_ms,
                other.end_ms,
                other.interval_ms,
                other.range_ms,
                &other.time_index,
                &other.field_column,
            ))
    }
}

impl UserDefinedLogicalNodeCore for RangeManipulate {
    fn name(&self) -> &'static str {
        "RangeManipulate"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PromRangeManipulate: start_ms={}, end_ms={}, interval_ms={}, range_ms={}",
            self.start_ms, self.end_ms, self.interval_ms, self.range_ms
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "RangeManipulate expects no expressions and one input".to_string(),
            ));
        }
        Self::new(
            self.start_ms,
            self.end_ms,
            self.interval_ms,
            self.range_ms,
            self.time_index.clone(),
            self.field_column.clone(),
            inputs.swap_remove(0),
        )
    }
}

/// Physical node that folds samples into per-eval-step range windows.
#[derive(Debug)]
pub struct RangeManipulateExec {
    start_ms: i64,
    end_ms: i64,
    interval_ms: i64,
    range_ms: i64,
    time_index: String,
    field_column: String,
    output_schema: SchemaRef,
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl RangeManipulateExec {
    #[must_use]
    pub fn new(
        start_ms: i64,
        end_ms: i64,
        interval_ms: i64,
        range_ms: i64,
        time_index: String,
        field_column: String,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let output_schema =
            build_extended_range_schema(&input.schema(), &time_index, &field_column);
        // RangeManipulate rewrites the schema (it drops the scalar time/value
        // columns and appends the windowed RangeArray columns), so the
        // input's `PlanProperties` schema is stale. Build fresh properties keyed
        // on the *output* schema while preserving the input's partitioning,
        // emission, and boundedness so the framework's schema check passes.
        let input_properties = input.properties();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            input_properties.output_partitioning().clone(),
            input_properties.emission_type,
            input_properties.boundedness,
        ));
        Self {
            start_ms,
            end_ms,
            interval_ms,
            range_ms,
            time_index,
            field_column,
            output_schema,
            input,
            properties,
        }
    }

    /// Compute, for each eval step `t` on the grid, the half-open backing-array
    /// window `[lo, hi)` of sample rows whose timestamp falls in `(t-range, t]`.
    ///
    /// Returns `(eval_timestamps, ranges)` where `ranges[i] == (offset, len)`
    /// indexes the sorted input rows for eval step `eval_timestamps[i]`.
    fn windows(&self, timestamps: &Int64Array) -> DfResult<StepWindows> {
        if self.interval_ms <= 0 {
            return Err(DataFusionError::Execution(format!(
                "interval_ms must be positive, got {}",
                self.interval_ms
            )));
        }

        let mut eval_timestamps = Vec::new();
        let mut ranges = Vec::new();
        // The samples are time-sorted, so both window edges advance monotonically
        // as the grid steps forward.
        let mut lo = 0_usize;
        let mut hi = 0_usize;
        let mut grid_ts = self.start_ms;
        while grid_ts <= self.end_ms {
            let lower_bound = grid_ts.checked_sub(self.range_ms).ok_or_else(|| {
                DataFusionError::Execution("range lower-bound underflow".to_string())
            })?;
            // Left edge is open: exclude samples with ts <= grid_ts - range.
            while lo < timestamps.len() && timestamps.value(lo) <= lower_bound {
                lo += 1;
            }
            // Right edge is closed: include samples with ts <= grid_ts.
            if hi < lo {
                hi = lo;
            }
            while hi < timestamps.len() && timestamps.value(hi) <= grid_ts {
                hi += 1;
            }

            let offset =
                u32::try_from(lo).map_err(|error| DataFusionError::Execution(error.to_string()))?;
            let len = u32::try_from(hi - lo)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            eval_timestamps.push(grid_ts);
            ranges.push((offset, len));

            grid_ts = grid_ts
                .checked_add(self.interval_ms)
                .ok_or_else(|| DataFusionError::Execution("grid timestamp overflow".to_string()))?;
        }

        Ok((eval_timestamps, ranges))
    }

    fn manipulate_batch(&self, batch: &RecordBatch) -> DfResult<RecordBatch> {
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
                    "RangeManipulate time column `{}` must be Int64",
                    self.time_index
                ))
            })?;
        let value_column_index = batch
            .schema()
            .index_of(&self.field_column)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let values = batch
            .column(value_column_index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "RangeManipulate field column `{}` must be Float64",
                    self.field_column
                ))
            })?;

        // An empty input series has no labels to project and no samples to
        // window, so it contributes no output rows.
        if batch.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
        }

        let (eval_timestamps, ranges) = self.windows(timestamps)?;

        let timestamps_values = Arc::new(timestamps.clone()) as ArrayRef;
        let values_values = Arc::new(values.clone()) as ArrayRef;
        let timestamp_range = RangeArray::from_ranges(timestamps_values, ranges.iter().copied())
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let value_range = RangeArray::from_ranges(values_values, ranges.iter().copied())
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let timestamp_range = timestamp_range
            .into_dict_array()
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let value_range = value_range
            .into_dict_array()
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;

        // Label values repeat per eval step: take row 0 of each label column for
        // every output row (one series per batch, so all rows share labels).
        let take_indices =
            UInt32Array::from_iter_values(std::iter::repeat_n(0_u32, eval_timestamps.len()));

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.output_schema.fields().len());
        for (index, column) in batch.columns().iter().enumerate() {
            if index == time_column_index || index == value_column_index {
                continue;
            }
            columns.push(take(column.as_ref(), &take_indices, None)?);
        }

        columns.push(Arc::new(Int64Array::from_iter_values(
            eval_timestamps.iter().copied(),
        )) as ArrayRef);
        columns.push(Arc::new(timestamp_range) as ArrayRef);
        columns.push(Arc::new(value_range) as ArrayRef);

        RecordBatch::try_new(Arc::clone(&self.output_schema), columns)
            .map_err(|error| DataFusionError::Execution(error.to_string()))
    }
}

impl DisplayAs for RangeManipulateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PromRangeManipulateExec: start_ms={}, end_ms={}, interval_ms={}, range_ms={}",
            self.start_ms, self.end_ms, self.interval_ms, self.range_ms
        )
    }
}

impl ExecutionPlan for RangeManipulateExec {
    fn name(&self) -> &'static str {
        "RangeManipulateExec"
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
                "RangeManipulateExec expects one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            self.start_ms,
            self.end_ms,
            self.interval_ms,
            self.range_ms,
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
        let schema = Arc::clone(&self.output_schema);
        let this = Self {
            start_ms: self.start_ms,
            end_ms: self.end_ms,
            interval_ms: self.interval_ms,
            range_ms: self.range_ms,
            time_index: self.time_index.clone(),
            field_column: self.field_column.clone(),
            output_schema: Arc::clone(&self.output_schema),
            input: Arc::clone(&self.input),
            properties: Arc::clone(&self.properties),
        };
        let stream = input.map(move |batch| batch.and_then(|batch| this.manipulate_batch(&batch)));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Array, DictionaryArray, Float64Array, Int64Array, StringArray},
        compute::concat_batches,
        datatypes::{DataType, Field, Int64Type, Schema},
    };
    use assert2::check;
    use datafusion::{
        datasource::memory::MemorySourceConfig, physical_plan::collect, prelude::SessionContext,
    };

    use super::*;

    fn series_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("job", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]))
    }

    fn series_batch(ts: Vec<i64>, val: Vec<f64>) -> (RecordBatch, Arc<Schema>) {
        let schema = series_schema();
        let job = StringArray::from(vec!["a"; ts.len()]);
        let ts = Int64Array::from(ts);
        let val = Float64Array::from(val);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(job), Arc::new(ts), Arc::new(val)],
        )
        .unwrap();
        (batch, schema)
    }

    /// Decode a `RangeArray` dict column and return each cell as a `Vec` of i64
    /// (timestamps) — the backing values are read generically.
    fn timestamp_cells(batch: &RecordBatch, name: &str) -> Vec<Vec<i64>> {
        let dict = batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<DictionaryArray<Int64Type>>()
            .unwrap();
        let range = RangeArray::try_from_dict_array(dict).unwrap();
        (0..range.len())
            .map(|cell| {
                let arr = range.get(cell).unwrap();
                let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                (0..arr.len()).map(|i| arr.value(i)).collect()
            })
            .collect()
    }

    fn value_cells(batch: &RecordBatch, name: &str) -> Vec<Vec<f64>> {
        let dict = batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<DictionaryArray<Int64Type>>()
            .unwrap();
        let range = RangeArray::try_from_dict_array(dict).unwrap();
        (0..range.len())
            .map(|cell| {
                let arr = range.get(cell).unwrap();
                let arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
                (0..arr.len()).map(|i| arr.value(i)).collect()
            })
            .collect()
    }

    async fn run(
        batch: RecordBatch,
        schema: Arc<Schema>,
        start: i64,
        end: i64,
        interval: i64,
        range: i64,
    ) -> RecordBatch {
        let mem = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let exec = RangeManipulateExec::new(
            start,
            end,
            interval,
            range,
            "timestamp".into(),
            "value".into(),
            mem,
        );
        let out_schema = exec.output_schema.clone();
        let ctx = SessionContext::new();
        let out = collect(Arc::new(exec), ctx.task_ctx()).await.unwrap();
        concat_batches(&out_schema, &out).unwrap()
    }

    #[test]
    fn extended_schema_layout_matches_contract() {
        let schema = build_extended_range_schema(&series_schema(), "timestamp", "value");
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        check!(names == vec!["job", "timestamp", "timestamp_range", "value_range"]);
        check!(schema.field_with_name("timestamp").unwrap().data_type() == &DataType::Int64);
        check!(schema.field_with_name("job").unwrap().data_type() == &DataType::Utf8);
        // The range columns are dictionaries of lists.
        assert2::assert!(matches!(
            schema
                .field_with_name("timestamp_range")
                .unwrap()
                .data_type(),
            DataType::Dictionary(_, _)
        ));
        assert2::assert!(matches!(
            schema.field_with_name("value_range").unwrap().data_type(),
            DataType::Dictionary(_, _)
        ));
    }

    #[tokio::test]
    async fn right_boundary_sample_is_included() {
        // Sample exactly at eval timestamp t must land in the window.
        let (batch, schema) = series_batch(vec![100], vec![1.0]);
        let out = run(batch, schema, 100, 100, 60, 60).await;
        let cells = timestamp_cells(&out, "timestamp_range");
        assert2::assert!(cells == vec![vec![100_i64]]);
    }

    #[tokio::test]
    async fn left_edge_sample_is_excluded() {
        // Sample at t - range must be excluded (left-open). With t=100, range=60
        // the left edge is 40; a sample at 40 is out, a sample at 41 is in.
        let (batch, schema) = series_batch(vec![40, 41], vec![1.0, 2.0]);
        let out = run(batch, schema, 100, 100, 60, 60).await;
        let ts_cells = timestamp_cells(&out, "timestamp_range");
        let val_cells = value_cells(&out, "value_range");
        assert2::assert!(ts_cells == vec![vec![41_i64]]);
        assert2::assert!(val_cells == vec![vec![2.0_f64]]);
    }

    #[tokio::test]
    async fn empty_window_produces_empty_cell() {
        // No samples in (40, 100]; the window must be empty, not absent.
        let (batch, schema) = series_batch(vec![10, 20], vec![1.0, 2.0]);
        let out = run(batch, schema, 100, 100, 60, 60).await;
        let ts_cells = timestamp_cells(&out, "timestamp_range");
        assert2::assert!(ts_cells == vec![Vec::<i64>::new()]);
    }

    #[tokio::test]
    async fn multiple_eval_steps_fold_overlapping_windows() {
        // range=60, interval=30. Samples every 25ms.
        let (batch, schema) = series_batch(vec![0, 25, 50, 75, 100], vec![0.0, 1.0, 2.0, 3.0, 4.0]);
        let out = run(batch, schema, 60, 120, 30, 60).await;

        // Eval steps: 60, 90, 120.
        let eval = out
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        check!((0..eval.len()).map(|i| eval.value(i)).collect::<Vec<_>>() == vec![60, 90, 120]);

        let ts_cells = timestamp_cells(&out, "timestamp_range");
        let val_cells = value_cells(&out, "value_range");
        // (0, 60]  -> 25, 50            (0 excluded: 60-60=0, left-open)
        // (30, 90] -> 50, 75
        // (60, 120]-> 75, 100
        check!(ts_cells == vec![vec![25, 50], vec![50, 75], vec![75, 100]]);
        check!(val_cells == vec![vec![1.0, 2.0], vec![2.0, 3.0], vec![3.0, 4.0]]);

        // Labels carried through, one row per eval step.
        let job = out
            .column_by_name("job")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        check!(job.len() == 3);
        check!((0..job.len()).all(|i| job.value(i) == "a"));
    }

    #[tokio::test]
    async fn windows_share_offsets_across_value_and_timestamp() {
        // The two RangeArray columns must be row-aligned: same cell lengths.
        let (batch, schema) = series_batch(vec![10, 20, 30], vec![1.0, 2.0, 3.0]);
        let out = run(batch, schema, 30, 30, 30, 30).await;
        let ts_cells = timestamp_cells(&out, "timestamp_range");
        let val_cells = value_cells(&out, "value_range");
        // (0, 30] -> 10, 20, 30
        assert2::assert!(ts_cells == vec![vec![10, 20, 30]]);
        assert2::assert!(val_cells == vec![vec![1.0, 2.0, 3.0]]);
    }

    #[tokio::test]
    async fn empty_input_series_yields_no_rows() {
        // A series with no samples projects nothing: no labels to repeat and no
        // windows to emit, so the output batch has the extended schema but zero
        // rows.
        let (batch, schema) = series_batch(vec![], vec![]);
        let out = run(batch, schema, 0, 120, 60, 60).await;
        assert2::assert!(out.num_rows() == 0);
        let out_schema = out.schema();
        let names: Vec<&str> = out_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert2::assert!(names == vec!["job", "timestamp", "timestamp_range", "value_range"]);
    }
}
