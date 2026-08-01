//! Rate-family `PromQL` functions as `DataFusion` [`ScalarUDF`]s.
//!
//! Each UDF consumes the windowed columns that `RangeManipulate` emits per eval
//! step and produces a `Float64Array` with one value per step. The shared
//! extrapolation/instant math lives in [`super::extrapolate`] (a byte-for-byte
//! port of the tree-walking engine), so these UDFs and the interpreter agree on
//! every number.
//!
//! # Call convention
//!
//! Every rate-family UDF is called with **four** positional arguments, in order:
//!
//! 1. `eval_timestamp` (`Int64`): the eval instant `t` each window closes on —
//!    `RangeManipulate`'s scalar `timestamp` column. This is `range_end_ms`.
//! 2. `timestamp_range` (`Dictionary<Int64, List<Int64>>`): the windowed sample
//!    timestamps — `RangeManipulate`'s `<time>_range` column.
//! 3. `value_range` (`Dictionary<Int64, List<Float64>>`): the windowed sample
//!    values — `RangeManipulate`'s `<value>_range` column, 1:1 with (2).
//! 4. `range_ms` (`Int64`, scalar): the range-selector width in milliseconds.
//!    `range_start_ms = eval_timestamp - range_ms`.
//!
//! All five functions take the same arity even though `irate`/`idelta` ignore
//! `range_ms` (they only use the last two samples) — a uniform shape keeps the
//! planner lowering trivial. Cells that Prometheus has no value for (fewer than
//! two samples, zero-width interval) render as **NULL** (not a NaN sentinel), so
//! the assembler drops the series and downstream aggregates skip it — matching
//! the interpreter, which omits no-value series before aggregating. A
//! genuinely-computed value, even a legitimately-NaN one, stays a non-null float
//! and propagates.

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, DictionaryArray, Float64Builder, Int64Array},
    datatypes::{DataType, Int64Type},
};
use crabka_units::prelude::*;
use datafusion::{
    common::{DataFusionError, Result as DfResult},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::SessionContext,
};

use super::extrapolate::{InstantKind, RangeKind, extrapolated_rate, instant_delta};
use crate::range_array::RangeArray;

/// Which rate-family function a [`RateUdf`] evaluates.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum RateFamily {
    /// Windowed, reset-corrected, per-second rate.
    Rate,
    /// Windowed, reset-corrected total increase.
    Increase,
    /// Windowed gauge delta (first..last, no reset correction).
    Delta,
    /// Instant per-second rate from the last two samples.
    Irate,
    /// Instant gauge delta from the last two samples.
    Idelta,
}

impl RateFamily {
    fn udf_name(self) -> &'static str {
        match self {
            Self::Rate => "prom_rate",
            Self::Increase => "prom_increase",
            Self::Delta => "prom_delta",
            Self::Irate => "prom_irate",
            Self::Idelta => "prom_idelta",
        }
    }

    /// Evaluate one window. `eval_ts` is `range_end_ms`; `range` is the
    /// selector width. Returns `None` where Prometheus yields no value.
    fn eval_window(
        self,
        timestamps: &[i64],
        values: &[f64],
        eval_ts: i64,
        range: Time,
    ) -> Option<f64> {
        let range_ms = range.millis_i64();
        match self {
            Self::Rate => extrapolated_rate(
                timestamps,
                values,
                eval_ts - range_ms,
                eval_ts,
                range,
                RangeKind::Rate,
            ),
            Self::Increase => extrapolated_rate(
                timestamps,
                values,
                eval_ts - range_ms,
                eval_ts,
                range,
                RangeKind::Increase,
            ),
            Self::Delta => extrapolated_rate(
                timestamps,
                values,
                eval_ts - range_ms,
                eval_ts,
                range,
                RangeKind::Delta,
            ),
            Self::Irate => instant_delta(timestamps, values, InstantKind::Irate),
            Self::Idelta => instant_delta(timestamps, values, InstantKind::Idelta),
        }
    }
}

/// A `ScalarUDFImpl` over `RangeManipulate`'s windowed columns. One instance per
/// [`RateFamily`] member; the family discriminates the math.
///
/// `ScalarUDFImpl` requires `Eq`/`Hash` (via `DynEq`/`DynHash`) so the planner
/// can deduplicate and key on UDF identity; both fields derive them cleanly.
#[derive(Debug, PartialEq, Eq, Hash)]
struct RateUdf {
    family: RateFamily,
    signature: Signature,
}

impl RateUdf {
    fn new(family: RateFamily) -> Self {
        Self {
            family,
            // Args mix Int64 scalars and Dictionary range columns, so type
            // coercion is bespoke: accept whatever the planner supplies and
            // validate shapes at invoke time.
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

/// Decode a `Dictionary<Int64, List<_>>` range column into a [`RangeArray`].
fn decode_range_column(array: &ArrayRef, arg: &str, udf: &str) -> DfResult<RangeArray> {
    let dict = array
        .as_any()
        .downcast_ref::<DictionaryArray<Int64Type>>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "{udf}: `{arg}` must be a RangeArray dictionary column, got {:?}",
                array.data_type()
            ))
        })?;
    RangeArray::try_from_dict_array(dict)
        .map_err(|error| DataFusionError::Execution(format!("{udf}: decoding `{arg}`: {error}")))
}

impl ScalarUDFImpl for RateUdf {
    fn name(&self) -> &str {
        self.family.udf_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }

    /// `Signature::user_defined` requires bespoke coercion. The rate UDFs accept
    /// their args verbatim — the `RangeArray` dictionary columns and the Int64
    /// scalar are already the exact types `RangeManipulate` produces, so no
    /// casting is wanted (and casting a `Dictionary<Int64, List<_>>` would be
    /// nonsensical). Validate arity and pass the types through unchanged.
    fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
        if arg_types.len() != 4 {
            return Err(DataFusionError::Plan(format!(
                "{} expects 4 arguments (eval_timestamp, timestamp_range, value_range, range_ms), got {}",
                self.name(),
                arg_types.len()
            )));
        }
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let name = self.name();
        if args.args.len() != 4 {
            return Err(DataFusionError::Execution(format!(
                "{name} expects 4 arguments (eval_timestamp, timestamp_range, value_range, range_ms), got {}",
                args.args.len()
            )));
        }
        let rows = args.number_rows;

        // 1. eval_timestamp column (Int64): range_end_ms per step.
        let eval_ts = args.args[0].clone().into_array(rows)?;
        let eval_ts = eval_ts
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{name}: `eval_timestamp` must be Int64, got {:?}",
                    eval_ts.data_type()
                ))
            })?;

        // 2 & 3. The windowed timestamp and value RangeArrays.
        let timestamp_range = args.args[1].clone().into_array(rows)?;
        let timestamp_range = decode_range_column(&timestamp_range, "timestamp_range", name)?;
        let value_range = args.args[2].clone().into_array(rows)?;
        let value_range = decode_range_column(&value_range, "value_range", name)?;

        // 4. range_ms scalar (the range-selector width).
        let range = Time::from_millis(scalar_i64(&args.args[3], "range_ms", name)?);

        if timestamp_range.len() != rows || value_range.len() != rows || eval_ts.len() != rows {
            return Err(DataFusionError::Execution(format!(
                "{name}: row-count mismatch (eval_ts={}, timestamp_range={}, value_range={}, rows={rows})",
                eval_ts.len(),
                timestamp_range.len(),
                value_range.len()
            )));
        }

        let mut builder = Float64Builder::with_capacity(rows);
        for row in 0..rows {
            let timestamps = timestamp_range.timestamp_slice(row).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{name}: `timestamp_range` cell {row} is not Int64"
                ))
            })?;
            let values = value_range.value_slice(row).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{name}: `value_range` cell {row} is not Float64"
                ))
            })?;
            let eval = eval_ts.value(row);
            match self.family.eval_window(timestamps, values, eval, range) {
                // A genuinely-computed value (including a legitimately-NaN result)
                // is kept as a non-null float so it propagates through downstream
                // aggregates exactly as the interpreter propagates it.
                Some(value) => builder.append_value(value),
                // Prometheus has no value for this window (fewer than two samples,
                // zero-width interval). Emit NULL — not a NaN sentinel — so the
                // assembler drops the series and aggregates skip it, matching the
                // interpreter, which omits no-value series before aggregating.
                None => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// Read a scalar `Int64` argument, accepting a single-row array fallback.
fn scalar_i64(value: &ColumnarValue, arg: &str, udf: &str) -> DfResult<i64> {
    match value {
        ColumnarValue::Scalar(scalar) => match scalar {
            datafusion::common::ScalarValue::Int64(Some(v)) => Ok(*v),
            other => Err(DataFusionError::Execution(format!(
                "{udf}: `{arg}` must be a non-null Int64 scalar, got {other:?}"
            ))),
        },
        ColumnarValue::Array(array) => {
            let ints = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{udf}: `{arg}` must be Int64, got {:?}",
                    array.data_type()
                ))
            })?;
            if ints.is_empty() || ints.is_null(0) {
                return Err(DataFusionError::Execution(format!(
                    "{udf}: `{arg}` must be a non-null Int64"
                )));
            }
            Ok(ints.value(0))
        }
    }
}

/// The `rate` UDF: per-second, counter-reset-corrected, extrapolated rate.
#[must_use]
pub fn rate_udf() -> ScalarUDF {
    ScalarUDF::from(RateUdf::new(RateFamily::Rate))
}

/// The `increase` UDF: counter-reset-corrected, extrapolated total increase.
#[must_use]
pub fn increase_udf() -> ScalarUDF {
    ScalarUDF::from(RateUdf::new(RateFamily::Increase))
}

/// The `delta` UDF: gauge first..last delta with boundary extrapolation.
#[must_use]
pub fn delta_udf() -> ScalarUDF {
    ScalarUDF::from(RateUdf::new(RateFamily::Delta))
}

/// The `irate` UDF: per-second instant rate from the last two samples.
#[must_use]
pub fn irate_udf() -> ScalarUDF {
    ScalarUDF::from(RateUdf::new(RateFamily::Irate))
}

/// The `idelta` UDF: gauge delta of the last two samples.
#[must_use]
pub fn idelta_udf() -> ScalarUDF {
    ScalarUDF::from(RateUdf::new(RateFamily::Idelta))
}

/// Every rate-family UDF, ready to register on a [`SessionContext`].
#[must_use]
pub fn rate_family_udfs() -> Vec<ScalarUDF> {
    vec![
        rate_udf(),
        increase_udf(),
        delta_udf(),
        irate_udf(),
        idelta_udf(),
    ]
}

/// Register every rate-family UDF on `ctx` so a planner can lower onto them.
pub fn register_rate_udfs(ctx: &SessionContext) {
    for udf in rate_family_udfs() {
        ctx.register_udf(udf);
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::Float64Array,
        datatypes::{Field, Schema},
        record_batch::RecordBatch,
    };
    use assert2::check;
    use datafusion::common::ScalarValue;

    use super::*;

    fn approx_eq(left: f64, right: f64) -> bool {
        (left - right).abs() < 1e-9
    }

    fn timestamp_range(windows: &[&[i64]]) -> ArrayRef {
        let mut values = Vec::new();
        let mut ranges = Vec::new();
        let mut offset = 0_u32;
        for window in windows {
            let len = u32::try_from(window.len()).unwrap();
            values.extend_from_slice(window);
            ranges.push((offset, len));
            offset += len;
        }
        let range = RangeArray::from_ranges(Arc::new(Int64Array::from(values)) as ArrayRef, ranges)
            .unwrap();
        Arc::new(range.into_dict_array().unwrap())
    }

    fn value_range(windows: &[&[f64]]) -> ArrayRef {
        let mut values = Vec::new();
        let mut ranges = Vec::new();
        let mut offset = 0_u32;
        for window in windows {
            let len = u32::try_from(window.len()).unwrap();
            values.extend_from_slice(window);
            ranges.push((offset, len));
            offset += len;
        }
        let range =
            RangeArray::from_ranges(Arc::new(Float64Array::from(values)) as ArrayRef, ranges)
                .unwrap();
        Arc::new(range.into_dict_array().unwrap())
    }

    fn invoke_args(
        eval_col: ArrayRef,
        ts_dict: ArrayRef,
        val_dict: ArrayRef,
        range_ms: ColumnarValue,
        rows: usize,
    ) -> ScalarFunctionArgs {
        let return_field = Arc::new(Field::new("out", DataType::Float64, true));
        let arg_fields = vec![
            Arc::new(Field::new(
                "eval_timestamp",
                eval_col.data_type().clone(),
                false,
            )),
            Arc::new(Field::new(
                "timestamp_range",
                ts_dict.data_type().clone(),
                false,
            )),
            Arc::new(Field::new(
                "value_range",
                val_dict.data_type().clone(),
                false,
            )),
            Arc::new(Field::new("range_ms", DataType::Int64, false)),
        ];
        ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(eval_col),
                ColumnarValue::Array(ts_dict),
                ColumnarValue::Array(val_dict),
                range_ms,
            ],
            arg_fields,
            number_rows: rows,
            return_field,
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        }
    }

    /// Build the four invoke args (`eval_ts`, `timestamp_range`, `value_range`,
    /// `range_ms`) for a multi-step window set and run a `RateUdf` directly,
    /// returning each step's value or `None` for a no-value (NULL) cell.
    fn run_udf_nullable(
        udf: &RateUdf,
        steps: &[(i64, &[i64], &[f64])],
        range_ms: i64,
    ) -> Vec<Option<f64>> {
        // Flatten the per-step windows into paired backing arrays + ranges.
        let mut all_ts = Vec::new();
        let mut all_val = Vec::new();
        let mut ranges = Vec::new();
        let mut eval = Vec::new();
        let mut offset = 0_u32;
        for (eval_ts, ts, val) in steps {
            assert2::assert!(ts.len() == val.len());
            let len = u32::try_from(ts.len()).unwrap();
            all_ts.extend_from_slice(ts);
            all_val.extend_from_slice(val);
            ranges.push((offset, len));
            offset += len;
            eval.push(*eval_ts);
        }
        let (value_ra, ts_ra) = RangeArray::from_paired_ranges(
            Float64Array::from(all_val),
            Int64Array::from(all_ts),
            ranges,
        )
        .unwrap();
        let ts_dict: ArrayRef = Arc::new(ts_ra.into_dict_array().unwrap());
        let val_dict: ArrayRef = Arc::new(value_ra.into_dict_array().unwrap());
        let eval_col: ArrayRef = Arc::new(Int64Array::from(eval.clone()));

        let rows = steps.len();
        let args = invoke_args(
            eval_col,
            ts_dict,
            val_dict,
            ColumnarValue::Scalar(ScalarValue::Int64(Some(range_ms))),
            rows,
        );

        let out = udf.invoke_with_args(args).unwrap();
        let array = out.into_array(rows).unwrap();
        let floats = array.as_any().downcast_ref::<Float64Array>().unwrap();
        (0..floats.len())
            .map(|i| {
                if floats.is_null(i) {
                    None
                } else {
                    Some(floats.value(i))
                }
            })
            .collect()
    }

    /// Convenience wrapper asserting every step produced a (non-null) value,
    /// returning the unwrapped floats. Tests that exercise the no-value (NULL)
    /// case call [`run_udf_nullable`] directly.
    fn run_udf(udf: &RateUdf, steps: &[(i64, &[i64], &[f64])], range_ms: i64) -> Vec<f64> {
        run_udf_nullable(udf, steps, range_ms)
            .into_iter()
            .map(|value| value.expect("expected a non-null value cell"))
            .collect()
    }

    #[test]
    fn rate_udf_rejects_each_row_count_mismatch_independently() {
        let udf = RateUdf::new(RateFamily::Rate);
        let eval: ArrayRef = Arc::new(Int64Array::from(vec![60_000_i64]));
        let timestamps = timestamp_range(&[&[0, 60_000]]);
        let values = value_range(&[&[1.0, 2.0]]);
        let range_ms = ColumnarValue::Scalar(ScalarValue::Int64(Some(60_000)));

        for (_case, args) in [
            (
                "timestamp_range has extra rows",
                invoke_args(
                    Arc::clone(&eval),
                    timestamp_range(&[&[0, 60_000], &[0, 60_000]]),
                    Arc::clone(&values),
                    range_ms.clone(),
                    1,
                ),
            ),
            (
                "value_range has extra rows",
                invoke_args(
                    Arc::clone(&eval),
                    Arc::clone(&timestamps),
                    value_range(&[&[1.0, 2.0], &[1.0, 2.0]]),
                    range_ms.clone(),
                    1,
                ),
            ),
            (
                "eval_timestamp has extra rows",
                invoke_args(
                    Arc::new(Int64Array::from(vec![60_000_i64, 120_000])),
                    timestamps,
                    values,
                    range_ms,
                    1,
                ),
            ),
        ] {
            assert2::assert!(udf.invoke_with_args(args).is_err());
        }
    }

    #[test]
    fn rate_udf_rejects_empty_or_null_range_ms_array() {
        let udf = RateUdf::new(RateFamily::Rate);
        let eval: ArrayRef = Arc::new(Int64Array::from(vec![60_000_i64]));
        let timestamps = timestamp_range(&[&[0, 60_000]]);
        let values = value_range(&[&[1.0, 2.0]]);

        assert2::assert!(
            udf.invoke_with_args(invoke_args(
                Arc::clone(&eval),
                Arc::clone(&timestamps),
                Arc::clone(&values),
                ColumnarValue::Array(Arc::new(Int64Array::from(Vec::<i64>::new()))),
                1,
            ))
            .is_err()
        );
        assert2::assert!(
            udf.invoke_with_args(invoke_args(
                eval,
                timestamps,
                values,
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![None::<i64>]))),
                1,
            ))
            .is_err()
        );
    }

    /// `prom_rate` over the engine's counter window reproduces 5/300 — the same
    /// number `engine.rs::instant_rate_extrapolates_counter_window` asserts.
    #[test]
    fn rate_udf_matches_engine_counter_window() {
        let udf = RateUdf::new(RateFamily::Rate);
        // Single eval step at t=300s, window (0, 300s] holds 0..240s.
        let out = run_udf(
            &udf,
            &[(
                300_000,
                &[0, 60_000, 120_000, 180_000, 240_000],
                &[0.0, 1.0, 2.0, 3.0, 4.0],
            )],
            300_000,
        );
        assert2::assert!(out.len() == 1);
        assert2::assert!(approx_eq(out[0], 5.0 / 300.0));
    }

    /// `prom_rate` across multiple eval steps yields a per-step rate vector,
    /// matching `engine.rs::range_rate_uses_each_step_as_window_end` (4/300,
    /// 5/300 for the two steps at t=240s and t=300s).
    #[test]
    fn rate_udf_produces_per_step_vector() {
        let udf = RateUdf::new(RateFamily::Rate);
        let out = run_udf(
            &udf,
            &[
                // t=240s, window (-60s, 240s] -> 0..240s.
                (
                    240_000,
                    &[0, 60_000, 120_000, 180_000, 240_000],
                    &[0.0, 1.0, 2.0, 3.0, 4.0],
                ),
                // t=300s, window (0, 300s] -> 60..300s.
                (
                    300_000,
                    &[60_000, 120_000, 180_000, 240_000, 300_000],
                    &[1.0, 2.0, 3.0, 4.0, 5.0],
                ),
            ],
            300_000,
        );
        assert2::assert!(out.len() == 2);
        for (step, want) in [(0_usize, 4.0 / 300.0), (1, 5.0 / 300.0)] {
            assert2::assert!(approx_eq(out[step], want));
        }
    }

    /// `prom_increase` reproduces the engine's reset correction: 1,2,1 -> 2.0.
    #[test]
    fn increase_udf_corrects_counter_reset() {
        let udf = RateUdf::new(RateFamily::Increase);
        let out = run_udf(
            &udf,
            &[(120_000, &[0, 60_000, 120_000], &[1.0, 2.0, 1.0])],
            120_000,
        );
        assert2::assert!(approx_eq(out[0], 2.0));
    }

    /// `prom_delta` is gauge mode: 4,3 -> -2.0 (matches the engine).
    #[test]
    fn delta_udf_is_gauge_delta() {
        let udf = RateUdf::new(RateFamily::Delta);
        let out = run_udf(&udf, &[(60_000, &[30_000, 60_000], &[4.0, 3.0])], 60_000);
        assert2::assert!(approx_eq(out[0], -2.0));
    }

    /// `prom_irate` reproduces 2/30 from the last two samples (engine number).
    #[test]
    fn irate_udf_uses_last_two_samples() {
        let udf = RateUdf::new(RateFamily::Irate);
        let out = run_udf(
            &udf,
            &[(90_000, &[0, 60_000, 90_000], &[0.0, 1.0, 3.0])],
            120_000,
        );
        assert2::assert!(approx_eq(out[0], 2.0 / 30.0));
    }

    /// `prom_idelta` reproduces 2.0 from the last two samples (engine number).
    #[test]
    fn idelta_udf_uses_last_two_samples() {
        let udf = RateUdf::new(RateFamily::Idelta);
        let out = run_udf(
            &udf,
            &[(90_000, &[0, 60_000, 90_000], &[0.0, 1.0, 3.0])],
            120_000,
        );
        assert2::assert!(approx_eq(out[0], 2.0));
    }

    /// A window with fewer than two samples renders as NULL (no Prometheus
    /// value), not a NaN sentinel — so the assembler drops the series and
    /// aggregates skip it.
    #[test]
    fn under_two_samples_yields_null() {
        let udf = RateUdf::new(RateFamily::Rate);
        let out = run_udf_nullable(&udf, &[(60_000, &[60_000], &[1.0])], 60_000);
        assert2::assert!(out[0].is_none());
    }

    /// A genuinely-computed value is kept as a non-null float even when it is
    /// itself NaN (e.g. a delta over a window containing a NaN sample): the cell
    /// is non-null, so downstream aggregates propagate it rather than skip it.
    #[test]
    fn genuine_nan_value_is_kept_non_null() {
        let udf = RateUdf::new(RateFamily::Delta);
        // Two in-window samples (NaN, 1.0): the gauge delta is computed (not a
        // no-value case), and the arithmetic yields NaN. It must be a non-null
        // NaN cell, not a NULL.
        let out = run_udf_nullable(
            &udf,
            &[(120_000, &[60_000, 120_000], &[f64::NAN, 1.0])],
            120_000,
        );
        assert2::assert!(out[0].is_some());
        assert2::assert!(out[0].unwrap().is_nan());
    }

    /// The UDF installs onto a `SessionContext` under its Prometheus-prefixed
    /// names, so a planner can resolve them.
    #[test]
    fn register_installs_named_udfs() {
        use datafusion::execution::FunctionRegistry;

        let ctx = SessionContext::new();
        register_rate_udfs(&ctx);
        for name in [
            "prom_rate",
            "prom_increase",
            "prom_delta",
            "prom_irate",
            "prom_idelta",
        ] {
            assert2::assert!(ctx.udf(name).is_ok());
        }
    }

    /// End-to-end: register the UDF on a context and invoke it through a SQL
    /// projection over a `RecordBatch` carrying the `RangeManipulate` columns.
    #[tokio::test]
    async fn rate_udf_runs_through_sql_projection() {
        use datafusion::datasource::MemTable;

        // One eval step: t=300s, window holds 0..240s stepping by 1.0.
        let (value_ra, ts_ra) = RangeArray::from_paired_ranges(
            Float64Array::from(vec![0.0, 1.0, 2.0, 3.0, 4.0]),
            Int64Array::from(vec![0_i64, 60_000, 120_000, 180_000, 240_000]),
            [(0_u32, 5_u32)],
        )
        .unwrap();
        let ts_dict: ArrayRef = Arc::new(ts_ra.into_dict_array().unwrap());
        let val_dict: ArrayRef = Arc::new(value_ra.into_dict_array().unwrap());
        let eval_col: ArrayRef = Arc::new(Int64Array::from(vec![300_000_i64]));

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("timestamp_range", ts_dict.data_type().clone(), false),
            Field::new("value_range", val_dict.data_type().clone(), false),
        ]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![eval_col, ts_dict, val_dict]).unwrap();

        let ctx = SessionContext::new();
        register_rate_udfs(&ctx);
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("series", Arc::new(table)).unwrap();

        let df = ctx
            .sql(
                "SELECT prom_rate(timestamp, timestamp_range, value_range, CAST(300000 AS BIGINT)) AS r FROM series",
            )
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        let column = results[0]
            .column_by_name("r")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert2::assert!(column.len() == 1);
        assert2::assert!(approx_eq(column.value(0), 5.0 / 300.0));
    }

    /// Confirm the helper round-trips a `DictionaryArray` back into a `RangeArray`.
    #[test]
    fn decode_range_column_round_trips() {
        let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef;
        let range = RangeArray::from_ranges(values, [(0_u32, 2_u32), (2, 1)]).unwrap();
        let dict: ArrayRef = Arc::new(range.into_dict_array().unwrap());
        let back = decode_range_column(&dict, "value_range", "prom_rate").unwrap();
        check!(back.len() == 2);
        check!(back.value_slice(0).unwrap() == [1.0, 2.0]);

        // A non-dictionary column is rejected.
        let plain: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        check!(decode_range_column(&plain, "value_range", "prom_rate").is_err());
    }
}
