//! Leaf source and `LogicalPlan` assembly for the per-row scalar-math operator path.
//!
//! The path covers `abs`, `ceil`, …, the trig and hyperbolic family, `sgn`,
//! `round`, and the `clamp` family.
//!
//! The selector, rate, and `*_over_time` paths are not handed an evaluated input.
//! This module is handed the already-evaluated inner instant vector: one float
//! value per matched series, with genuine NaN preserved. The engine sources those
//! samples from a NaN-preserving bare-selector selection, or it assembles a
//! nested plannable inner expression. This module materializes the samples as a
//! leaf table with one row per series. The table carries the label columns of the
//! series plus a `value` column. The module then projects
//!
//! `Projection(labels-without-__name__..., prom_<fn>([bounds...,] value) AS value)`
//!
//! over that table.
//!
//! The projection drops the metric name `__name__` and the result-metadata labels
//! `__type__` and `__unit__`. Every scalar-math function drops them, as the
//! interpreter function `labels_without_metric_name` does. The module keeps every
//! result row and suppresses no NaN. `f(NaN)` and `sqrt(-1)` render as `NaN`,
//! exactly as the interpreter keeps every float sample.

use std::{collections::BTreeSet, sync::Arc};

use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use crabka_blockstore::Labels;
use datafusion::{
    catalog::MemTable,
    execution::FunctionRegistry,
    logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, col, lit},
    prelude::SessionContext,
};

use crate::{
    PromqlError, error::Result, extension::planner::prom_session_context, functions::ScalarMathOp,
};

/// Leaf-batch and projection column that carries the per-series float value.
pub const VALUE_COLUMN: &str = "value";
/// Leaf-batch and projection column that carries the per-series sample timestamp.
///
/// The scalar-math functions report the timestamp of the inner sample unchanged,
/// because the interpreter function `eval_unary_float_call` keeps `sample.ts_ms`.
/// The projection therefore carries the timestamp with the value.
pub const SAMPLE_TIME_COLUMN: &str = "sample_timestamp";

/// Result-metadata labels that every scalar-math function drops.
///
/// This list mirrors the interpreter function `is_result_metadata_label`. These
/// labels never reach the leaf, so the projection drops them implicitly.
const METADATA_LABELS: [&str; 3] = ["__name__", "__type__", "__unit__"];

fn is_metadata_label(name: &str) -> bool {
    METADATA_LABELS.contains(&name)
}

/// One already-evaluated inner instant-vector sample.
///
/// The sample holds the full label set, the reported timestamp of the inner
/// sample, and the float value that `f` is applied to. Leaf assembly drops the
/// metadata labels, and the projection keeps the timestamp. This type carries no
/// fingerprint, because the code reads the result label set straight from the
/// projected batch.
pub struct LabeledValue {
    pub labels: Labels,
    pub ts_ms: i64,
    pub value: f64,
}

/// The assembled scalar-math operator plan.
///
/// The leaf already drops the metric name, so the code reads the result label set
/// directly from the projected output batch. The plan needs no map from
/// fingerprint to labels.
pub struct ScalarMathPlan {
    /// Session context whose registry holds the scalar-math UDFs. The context
    /// also holds the registered leaf table.
    pub ctx: SessionContext,
    /// The `Projection(labels..., prom_<fn>([bounds...,] value) AS value)` plan.
    pub plan: LogicalPlan,
}

/// Builds the leaf table and projection that evaluates `op([bounds...,] value)`.
///
/// This function evaluates the operator over the already-evaluated inner instant
/// vector `samples`. `bounds` holds the leading scalar arguments in call order:
/// `[]` for the unary functions, `[to_nearest]` for `round`, `[min]` for
/// `clamp_min`, `[max]` for `clamp_max`, and `[min, max]` for `clamp`.
///
/// # Errors
///
/// This function returns an error if it cannot build the Arrow batch, the table,
/// or the projection plan.
pub async fn plan_scalar_math(
    samples: Vec<LabeledValue>,
    op: ScalarMathOp,
    bounds: &[f64],
) -> Result<ScalarMathPlan> {
    // Collect the distinct non-metadata label names; these become the leaf's
    // label columns. The series fingerprint is recomputed over the metadata-free
    // label set so it matches the projected output exactly.
    let mut label_names: BTreeSet<String> = BTreeSet::new();
    let mut rows: Vec<(Labels, i64, f64)> = Vec::with_capacity(samples.len());
    for sample in samples {
        let mut labels = Labels::new();
        for (name, value) in sample.labels.iter() {
            if !is_metadata_label(name) {
                label_names.insert(name.clone());
                labels.insert(name, value);
            }
        }
        rows.push((labels, sample.ts_ms, sample.value));
    }
    let label_names: Vec<String> = label_names.into_iter().collect();

    let schema = leaf_schema(&label_names);
    let batch = build_leaf_batch(Arc::clone(&schema), &label_names, &rows)?;

    let ctx = prom_session_context();
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|error| PromqlError::Exec(error.to_string()))?;
    ctx.register_table("prom_scalar_math_leaf", Arc::new(table))?;
    let leaf = ctx
        .table("prom_scalar_math_leaf")
        .await?
        .into_optimized_plan()?;

    let udf = ctx
        .udf(op.udf_name())
        .map_err(|error| PromqlError::Exec(error.to_string()))?;

    // The UDF call threads the constant scalar bounds ahead of the value column,
    // matching the scalar-math call convention.
    let mut udf_args: Vec<Expr> = bounds.iter().map(|bound| lit(*bound)).collect();
    udf_args.push(col(VALUE_COLUMN));
    let call = udf.call(udf_args).alias(VALUE_COLUMN);

    let mut projections: Vec<Expr> = label_names.iter().map(col).collect();
    projections.push(call);
    // Carry the inner sample timestamp through unchanged so the assembler can
    // report it (the scalar-math functions preserve `sample.ts_ms`).
    projections.push(col(SAMPLE_TIME_COLUMN));

    let plan = LogicalPlanBuilder::from(leaf)
        .project(projections)
        .map_err(|error| PromqlError::Exec(error.to_string()))?
        .build()
        .map_err(|error| PromqlError::Exec(error.to_string()))?;

    Ok(ScalarMathPlan { ctx, plan })
}

fn leaf_schema(label_names: &[String]) -> Arc<Schema> {
    let mut fields = Vec::with_capacity(label_names.len() + 2);
    for name in label_names {
        // Nullable so an ABSENT label (NULL) stays distinct from a PRESENT-but-
        // empty-valued label (`""`); see `super::leaf::leaf_schema`.
        fields.push(Field::new(name, DataType::Utf8, true));
    }
    fields.push(Field::new(VALUE_COLUMN, DataType::Float64, true));
    fields.push(Field::new(SAMPLE_TIME_COLUMN, DataType::Int64, false));
    Arc::new(Schema::new(fields))
}

fn build_leaf_batch(
    schema: Arc<Schema>,
    label_names: &[String],
    rows: &[(Labels, i64, f64)],
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(label_names.len() + 2);
    for name in label_names {
        // `None` (NULL) for an ABSENT label; `Some("")` for a PRESENT-empty one.
        let values = rows
            .iter()
            .map(|(labels, _, _)| labels.get(name).map(str::to_string))
            .collect::<Vec<Option<String>>>();
        columns.push(Arc::new(StringArray::from(values)));
    }
    columns.push(Arc::new(Float64Array::from_iter_values(
        rows.iter().map(|(_, _, value)| *value),
    )));
    columns.push(Arc::new(Int64Array::from_iter_values(
        rows.iter().map(|(_, ts_ms, _)| *ts_ms),
    )));
    RecordBatch::try_new(schema, columns).map_err(|error| PromqlError::Exec(error.to_string()))
}

#[cfg(test)]
mod tests {
    use arrow::array::Float64Array as Float64ArrayT;
    use assert2::check;

    use super::*;

    fn labeled(name: &str, l: &str, value: f64) -> LabeledValue {
        let mut labels = Labels::new();
        labels.insert("__name__", name);
        labels.insert("l", l);
        LabeledValue {
            labels,
            ts_ms: 0,
            value,
        }
    }

    async fn run(
        samples: Vec<LabeledValue>,
        op: ScalarMathOp,
        bounds: &[f64],
    ) -> Vec<(String, f64)> {
        let plan = plan_scalar_math(samples, op, bounds).await.unwrap();
        let batches = plan
            .ctx
            .execute_logical_plan(plan.plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut got = Vec::new();
        for batch in &batches {
            // `__name__` must be gone; the projection carries only `l` + `value`.
            assert2::assert!(batch.column_by_name("__name__").is_none());
            let l = batch
                .column_by_name("l")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let value = batch
                .column_by_name(VALUE_COLUMN)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64ArrayT>()
                .unwrap();
            for row in 0..batch.num_rows() {
                got.push((l.value(row).to_string(), value.value(row)));
            }
        }
        got.sort_by(|a, b| a.0.cmp(&b.0));
        got
    }

    #[tokio::test]
    async fn abs_drops_name_and_keeps_label() {
        let got = run(
            vec![labeled("m", "x", -3.0), labeled("m", "y", 4.0)],
            ScalarMathOp::Abs,
            &[],
        )
        .await;
        assert2::assert!(got == vec![("x".to_string(), 3.0), ("y".to_string(), 4.0)]);
    }

    #[tokio::test]
    async fn sqrt_negative_preserves_nan_row() {
        let got = run(vec![labeled("m", "x", -1.0)], ScalarMathOp::Sqrt, &[]).await;
        check!(got.len() == 1);
        check!(got[0].0 == "x");
        check!(got[0].1.is_nan());
    }

    #[tokio::test]
    async fn genuine_nan_value_survives() {
        let got = run(vec![labeled("m", "x", f64::NAN)], ScalarMathOp::Sin, &[]).await;
        assert2::assert!(got.len() == 1);
        assert2::assert!(got[0].1.is_nan());
    }

    #[tokio::test]
    async fn clamp_min_greater_handled_by_bounds() {
        let got = run(
            vec![labeled("m", "x", 5.0), labeled("m", "y", -5.0)],
            ScalarMathOp::Clamp,
            &[0.0, 3.0],
        )
        .await;
        assert2::assert!(got == vec![("x".to_string(), 3.0), ("y".to_string(), 0.0)]);
    }

    #[tokio::test]
    async fn round_uses_to_nearest_bound() {
        let got = run(vec![labeled("m", "x", 12.0)], ScalarMathOp::Round, &[5.0]).await;
        assert2::assert!(got == vec![("x".to_string(), 10.0)]);
    }
}
