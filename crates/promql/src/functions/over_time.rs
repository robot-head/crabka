//! `*_over_time` `PromQL` functions as `DataFusion` [`ScalarUDF`]s.
//!
//! Each UDF consumes the windowed columns that `RangeManipulate` emits per eval
//! step and produces a `Float64Array` with one value per step. The per-window
//! reductions are a byte-for-byte port of the tree-walking engine's
//! `over_time_sample_from_series` (float path) and `quantile_value`, so these
//! UDFs and the interpreter agree on every number.
//!
//! # Call convention
//!
//! Every non-quantile `*_over_time` UDF is called with **three** positional
//! arguments, in order:
//!
//! 1. `eval_timestamp` (`Int64`): the eval instant `t` each window closes on —
//!    `RangeManipulate`'s scalar `timestamp` column.
//! 2. `timestamp_range` (`Dictionary<Int64, List<Int64>>`): the windowed sample
//!    timestamps — `RangeManipulate`'s `<time>_range` column.
//! 3. `value_range` (`Dictionary<Int64, List<Float64>>`): the windowed sample
//!    values — `RangeManipulate`'s `<value>_range` column, 1:1 with (2).
//!
//! `quantile_over_time` takes a fourth leading argument, the quantile `phi`
//! (`Float64` scalar), threaded ahead of the three windowed columns:
//! `prom_quantile_over_time(phi, eval_timestamp, timestamp_range, value_range)`.

//!
//! The `eval_timestamp`/`timestamp_range` columns are accepted uniformly even
//! though only `last_over_time` consults timestamps (to pick the latest sample);
//! a uniform shape keeps the planner lowering trivial. Empty windows render as
//! **NULL** (not a NaN sentinel; Prometheus emits no sample there), which the
//! assembler drops and downstream aggregates skip — matching the interpreter,
//! which omits no-value series before aggregating. A genuinely-computed
//! reduction, even a legitimately-NaN one, stays a non-null float and propagates.

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, DictionaryArray, Float64Array, Float64Builder, Int64Array},
    datatypes::{DataType, Int64Type},
};
use datafusion::{
    common::{DataFusionError, Result as DfResult, ScalarValue},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::SessionContext,
};
use num_traits::ToPrimitive;

use crate::range_array::RangeArray;

/// Which `*_over_time` function an [`OverTimeUdf`] evaluates. Only the
/// non-experimental, float-typed members the operator path supports appear here;
/// `mad_over_time`, `first_over_time`, and the `ts_of_*_over_time` family stay on
/// the interpreter.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum OverTimeFamily {
    /// Sum of the window's sample values.
    Sum,
    /// Arithmetic mean of the window's sample values.
    Avg,
    /// Count of samples in the window.
    Count,
    /// Smallest sample value, NaN-ignoring (Prometheus folds NaN out, matching
    /// the engine and the `prom_min` aggregate).
    Min,
    /// Largest sample value, NaN-ignoring (Prometheus folds NaN out, matching
    /// the engine and the `prom_max` aggregate).
    Max,
    /// Population standard deviation of the window's sample values.
    Stddev,
    /// Population variance of the window's sample values.
    Stdvar,
    /// Value of the latest (max-timestamp) sample in the window.
    Last,
    /// `1.0` if the window holds any sample.
    Present,
    /// `phi`-quantile of the window's sample values (linear interpolation),
    /// matching the engine's `quantile_value`.
    Quantile,
}

impl OverTimeFamily {
    /// The registered UDF name this family projects.
    #[must_use]
    pub fn udf_name(self) -> &'static str {
        match self {
            Self::Sum => "prom_sum_over_time",
            Self::Avg => "prom_avg_over_time",
            Self::Count => "prom_count_over_time",
            Self::Min => "prom_min_over_time",
            Self::Max => "prom_max_over_time",
            Self::Stddev => "prom_stddev_over_time",
            Self::Stdvar => "prom_stdvar_over_time",
            Self::Last => "prom_last_over_time",
            Self::Present => "prom_present_over_time",
            Self::Quantile => "prom_quantile_over_time",
        }
    }

    /// Whether this family threads a leading `phi` (quantile) scalar argument.
    fn takes_quantile_param(self) -> bool {
        matches!(self, Self::Quantile)
    }

    /// Evaluate one window's reduction. `timestamps` and `values` are paired 1:1
    /// in sample order; `phi` is the quantile for [`OverTimeFamily::Quantile`]
    /// and ignored otherwise. Returns `None` where Prometheus yields no value
    /// (an empty window).
    fn eval_window(self, timestamps: &[i64], values: &[f64], phi: f64) -> Option<f64> {
        if values.is_empty() {
            return None;
        }
        let value = match self {
            Self::Sum => values.iter().sum(),
            Self::Avg => over_time_mean(values),
            Self::Count => values.iter().map(|_| 1.0).sum(),
            Self::Min => fold_extremum(values, Extremum::Min),
            Self::Max => fold_extremum(values, Extremum::Max),
            Self::Stddev => over_time_variance(values).sqrt(),
            Self::Stdvar => over_time_variance(values),
            Self::Last => last_value_by_timestamp(timestamps, values)?,
            Self::Present => 1.0,
            Self::Quantile => quantile_value(phi, values)?,
        };
        Some(value)
    }
}

/// Which extremum [`fold_extremum`] tracks.
#[derive(Clone, Copy)]
enum Extremum {
    Min,
    Max,
}

impl Extremum {
    /// Whether the running value `running` should be replaced by `candidate`
    /// under Prometheus' NaN-ignoring float ordering — the exact rule the
    /// `prom_min`/`prom_max` aggregate UDAF's `Extremum::should_replace` and the
    /// engine's `AggregateState::push_float` use. A NaN running value is always
    /// replaced; a NaN candidate (with a non-NaN running value) never is, since
    /// `NaN > _` / `NaN < _` are both false.
    fn should_replace(self, running: f64, candidate: f64) -> bool {
        if running.is_nan() {
            return true;
        }
        match self {
            Self::Min => running > candidate,
            Self::Max => running < candidate,
        }
    }
}

/// NaN-ignoring `min`/`max` fold over a non-empty window: seed with the first
/// sample (NaN included), then replace the running value under
/// [`Extremum::should_replace`]. The result is NaN only when *every* sample is
/// NaN — matching Prometheus, the engine's `over_time_sample_from_series`, and
/// the `prom_min`/`prom_max` aggregate UDAF.
fn fold_extremum(values: &[f64], extremum: Extremum) -> f64 {
    let mut running = values[0];
    for &candidate in &values[1..] {
        if extremum.should_replace(running, candidate) {
            running = candidate;
        }
    }
    running
}

/// Population variance of `values` via Welford's online algorithm with
/// Kahan-compensated accumulation, a port of the engine's `over_time_variance`
/// (which now matches Prometheus' `stdvar_over_time`/`stddev_over_time`). The
/// naive `E[x^2] - E[x]^2` form suffers catastrophic cancellation for
/// large-magnitude close-valued windows (a negative variance whose `sqrt` is
/// NaN); Welford stays stable.
fn over_time_variance(values: &[f64]) -> f64 {
    let mut count = 0.0_f64;
    let (mut mean, mut mean_comp) = (0.0_f64, 0.0_f64);
    let (mut aux, mut aux_comp) = (0.0_f64, 0.0_f64);
    for value in values {
        count += 1.0;
        let delta = value - (mean + mean_comp);
        let (new_mean, new_mean_comp) = kahan_sum_inc(delta / count, mean, mean_comp);
        mean = new_mean;
        mean_comp = new_mean_comp;
        let (new_aux, new_aux_comp) =
            kahan_sum_inc(delta * (value - (mean + mean_comp)), aux, aux_comp);
        aux = new_aux;
        aux_comp = new_aux_comp;
    }
    (aux + aux_comp) / count
}

/// Arithmetic mean of a non-empty `values` window via Prometheus' incremental
/// Kahan-compensated mean (`avg_over_time`), a port of the engine's
/// `over_time_mean`. The naive `sum / count` overflows to ±Inf for
/// very-large-magnitude windows; the incremental form stays finite and
/// preserves the same-sign-infinity handling once it does saturate.
fn over_time_mean(values: &[f64]) -> f64 {
    let mut count = 0.0_f64;
    let (mut mean, mut comp) = (0.0_f64, 0.0_f64);
    for &value in values {
        count += 1.0;
        if keep_infinite_mean(mean, value) {
            continue;
        }
        let (new_mean, new_comp) = kahan_sum_inc(value / count - mean / count, mean, comp);
        mean = new_mean;
        comp = new_comp;
    }
    mean + comp
}

fn keep_infinite_mean(mean: f64, value: f64) -> bool {
    mean.is_infinite()
        && ((value.is_infinite() && value.is_sign_positive() == mean.is_sign_positive())
            || (!value.is_infinite() && !value.is_nan()))
}

/// One Kahan-compensated incremental sum step, a port of Prometheus' `kahanSumInc`
/// (`promql/engine.go`). Returns the updated `(sum, comp)` after adding
/// `increment`, so the mean/variance folds agree bit-for-bit with the engine.
fn kahan_sum_inc(increment: f64, sum: f64, comp: f64) -> (f64, f64) {
    let new_sum = sum + increment;
    let new_comp = if sum.abs() >= increment.abs() {
        comp + ((sum - new_sum) + increment)
    } else {
        comp + ((increment - new_sum) + sum)
    };
    (new_sum, new_comp)
}

/// Value of the sample with the greatest timestamp, ties broken by the later
/// element (mirroring `max_by_key(timestamp)` over a time-sorted window). Returns
/// `None` only for an empty window.
fn last_value_by_timestamp(timestamps: &[i64], values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    // The window is time-sorted ascending (SeriesNormalize), so the engine's
    // `max_by_key(timestamp)` is the last element. Fall back to a scan if the
    // timestamps and values disagree in length (defensive; should not happen).
    if timestamps.len() == values.len() {
        let mut best_idx = 0;
        for (idx, ts) in timestamps.iter().enumerate() {
            // `max_by_key` keeps the *last* maximum on ties.
            if *ts >= timestamps[best_idx] {
                best_idx = idx;
            }
        }
        return Some(values[best_idx]);
    }
    values.last().copied()
}

/// `phi`-quantile of `values` with linear interpolation between ranks. A direct
/// port of the engine's `quantile_value` (operating on a local sorted copy so
/// the UDF can take `&[f64]`). Returns `None` for an empty slice.
fn quantile_value(phi: f64, values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    // Prometheus does NOT error on an out-of-range/NaN phi: NaN -> NaN, phi < 0
    // -> -Inf, phi > 1 -> +Inf (the engine raises an `InvalidQuantileWarning`
    // alongside). Mirror the engine's `quantile_value` leading guards so the UDF
    // and interpreter agree.
    if phi.is_nan() {
        return Some(f64::NAN);
    }
    if phi < 0.0 {
        return Some(f64::NEG_INFINITY);
    }
    if phi > 1.0 {
        return Some(f64::INFINITY);
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    if sorted.len() == 1 {
        return Some(sorted[0]);
    }
    let rank = phi * (sorted.len() - 1).to_f64()?;
    let lower = rank.floor().to_usize()?;
    let upper = rank.ceil().to_usize()?;
    if lower == upper {
        return Some(sorted[lower]);
    }
    let weight = rank - lower.to_f64()?;
    Some(sorted[lower] * (1.0 - weight) + sorted[upper] * weight)
}

/// A `ScalarUDFImpl` over `RangeManipulate`'s windowed columns. One instance per
/// [`OverTimeFamily`] member; the family discriminates the reduction.
#[derive(Debug, PartialEq, Eq, Hash)]
struct OverTimeUdf {
    family: OverTimeFamily,
    signature: Signature,
}

impl OverTimeUdf {
    fn new(family: OverTimeFamily) -> Self {
        Self {
            family,
            // Args mix scalars and Dictionary range columns, so coercion is
            // bespoke: accept whatever the planner supplies, validate at invoke.
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    /// Expected positional-argument count for this family.
    fn arity(&self) -> usize {
        if self.family.takes_quantile_param() {
            4
        } else {
            3
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

impl ScalarUDFImpl for OverTimeUdf {
    fn name(&self) -> &str {
        self.family.udf_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }

    /// `Signature::user_defined` requires bespoke coercion. These UDFs accept
    /// their args verbatim — the `RangeArray` dictionary columns and the
    /// scalars are already the exact types the planner supplies. Validate arity
    /// and pass the types through unchanged.
    fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
        if arg_types.len() != self.arity() {
            return Err(DataFusionError::Plan(format!(
                "{} expects {} arguments, got {}",
                self.name(),
                self.arity(),
                arg_types.len()
            )));
        }
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let name = self.name();
        if args.args.len() != self.arity() {
            return Err(DataFusionError::Execution(format!(
                "{name} expects {} arguments, got {}",
                self.arity(),
                args.args.len()
            )));
        }
        let rows = args.number_rows;

        // For the quantile family the leading argument is the `phi` scalar; the
        // three windowed columns follow. For every other family the windowed
        // columns start at index 0.
        let (phi, base) = if self.family.takes_quantile_param() {
            (scalar_f64(&args.args[0], "phi", name)?, 1)
        } else {
            (f64::NAN, 0)
        };

        // eval_timestamp column (Int64): range_end_ms per step.
        let eval_ts = args.args[base].clone().into_array(rows)?;
        let eval_ts = eval_ts
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{name}: `eval_timestamp` must be Int64, got {:?}",
                    eval_ts.data_type()
                ))
            })?;

        // The windowed timestamp and value RangeArrays.
        let timestamp_range = args.args[base + 1].clone().into_array(rows)?;
        let timestamp_range = decode_range_column(&timestamp_range, "timestamp_range", name)?;
        let value_range = args.args[base + 2].clone().into_array(rows)?;
        let value_range = decode_range_column(&value_range, "value_range", name)?;

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
            match self.family.eval_window(timestamps, values, phi) {
                // A genuinely-computed reduction (including a legitimately-NaN
                // result, e.g. a quantile over a NaN sample) is kept as a non-null
                // float so it propagates through downstream aggregates exactly as
                // the interpreter propagates it.
                Some(value) => builder.append_value(value),
                // Empty window: Prometheus emits no sample. Emit NULL — not a NaN
                // sentinel — so the assembler drops the series and aggregates skip
                // it, matching the interpreter, which omits no-value series before
                // aggregating.
                None => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// Read a scalar `Float64` argument, accepting a single-row array fallback.
fn scalar_f64(value: &ColumnarValue, arg: &str, udf: &str) -> DfResult<f64> {
    match value {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Float64(Some(v)) => Ok(*v),
            other => Err(DataFusionError::Execution(format!(
                "{udf}: `{arg}` must be a non-null Float64 scalar, got {other:?}"
            ))),
        },
        ColumnarValue::Array(array) => {
            let floats = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "{udf}: `{arg}` must be Float64, got {:?}",
                        array.data_type()
                    ))
                })?;
            if floats.is_empty() || floats.is_null(0) {
                return Err(DataFusionError::Execution(format!(
                    "{udf}: `{arg}` must be a non-null Float64"
                )));
            }
            Ok(floats.value(0))
        }
    }
}

/// The `over_time` UDF for `family`.
#[must_use]
pub fn over_time_udf(family: OverTimeFamily) -> ScalarUDF {
    ScalarUDF::from(OverTimeUdf::new(family))
}

/// Every non-experimental `*_over_time` UDF, ready to register on a
/// [`SessionContext`].
#[must_use]
pub fn over_time_family_udfs() -> Vec<ScalarUDF> {
    [
        OverTimeFamily::Sum,
        OverTimeFamily::Avg,
        OverTimeFamily::Count,
        OverTimeFamily::Min,
        OverTimeFamily::Max,
        OverTimeFamily::Stddev,
        OverTimeFamily::Stdvar,
        OverTimeFamily::Last,
        OverTimeFamily::Present,
        OverTimeFamily::Quantile,
    ]
    .into_iter()
    .map(over_time_udf)
    .collect()
}

/// Register every `*_over_time` UDF on `ctx` so a planner can lower onto them.
pub fn register_over_time_udfs(ctx: &SessionContext) {
    for udf in over_time_family_udfs() {
        ctx.register_udf(udf);
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Field;
    use assert2::check;

    use super::*;

    fn approx_eq(left: f64, right: f64) -> bool {
        (left - right).abs() < 1e-9
    }

    /// Build the invoke args for a multi-step window set and run an
    /// `OverTimeUdf` directly, returning each step's value or `None` for a
    /// no-value (NULL) cell. `phi` is threaded only for the quantile family.
    fn run_udf_nullable(
        family: OverTimeFamily,
        steps: &[(i64, &[i64], &[f64])],
        phi: f64,
    ) -> Vec<Option<f64>> {
        let udf = OverTimeUdf::new(family);
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
        let return_field = Arc::new(Field::new("out", DataType::Float64, true));
        let mut arg_fields = Vec::new();
        let mut call_args = Vec::new();
        if family.takes_quantile_param() {
            arg_fields.push(Arc::new(Field::new("phi", DataType::Float64, false)));
            call_args.push(ColumnarValue::Scalar(ScalarValue::Float64(Some(phi))));
        }
        arg_fields.push(Arc::new(Field::new(
            "eval_timestamp",
            DataType::Int64,
            false,
        )));
        arg_fields.push(Arc::new(Field::new(
            "timestamp_range",
            ts_dict.data_type().clone(),
            false,
        )));
        arg_fields.push(Arc::new(Field::new(
            "value_range",
            val_dict.data_type().clone(),
            false,
        )));
        call_args.push(ColumnarValue::Array(eval_col));
        call_args.push(ColumnarValue::Array(ts_dict));
        call_args.push(ColumnarValue::Array(val_dict));

        let args = ScalarFunctionArgs {
            args: call_args,
            arg_fields,
            number_rows: rows,
            return_field,
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
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
    fn run_udf(family: OverTimeFamily, steps: &[(i64, &[i64], &[f64])], phi: f64) -> Vec<f64> {
        run_udf_nullable(family, steps, phi)
            .into_iter()
            .map(|value| value.expect("expected a non-null value cell"))
            .collect()
    }

    /// One window with 3,5 reproduces the engine's basic reductions
    /// (`instant_basic_over_time_functions_reduce_range_samples`).
    #[test]
    fn basic_reductions_match_engine() {
        let window: &[(i64, &[i64], &[f64])] = &[(120_000, &[60_000, 120_000], &[3.0, 5.0])];
        for (family, want) in [
            (OverTimeFamily::Sum, 8.0),
            (OverTimeFamily::Avg, 4.0),
            (OverTimeFamily::Count, 2.0),
            (OverTimeFamily::Min, 3.0),
            (OverTimeFamily::Max, 5.0),
            (OverTimeFamily::Last, 5.0),
            (OverTimeFamily::Present, 1.0),
        ] {
            let got = run_udf(family, window, 0.0)[0];
            assert2::assert!(approx_eq(got, want));
        }
    }

    /// Population stddev/stdvar over 2,4,4,4,5,5,7,9 reproduce the engine's
    /// `instant_statistical_over_time_functions_reduce_range_samples` numbers
    /// (stdvar == 4, stddev == 2), and the median quantile (0.5) == 4.5.
    #[test]
    fn statistical_reductions_match_engine() {
        let vals: &[f64] = &[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let ts: Vec<i64> = (0..i64::try_from(vals.len()).unwrap())
            .map(|i| (i + 1) * 60_000)
            .collect();
        let window: &[(i64, &[i64], &[f64])] = &[(480_000, &ts, vals)];
        for (family, phi, want) in [
            (OverTimeFamily::Stdvar, 0.0, 4.0),
            (OverTimeFamily::Stddev, 0.0, 2.0),
            (OverTimeFamily::Quantile, 0.5, 4.5),
        ] {
            let got = run_udf(family, window, phi)[0];
            assert2::assert!(approx_eq(got, want));
        }
    }

    #[test]
    fn extremum_ties_preserve_first_signed_zero() {
        let min = fold_extremum(&[0.0, -0.0], Extremum::Min);
        assert2::assert!(min.to_bits() == 0.0_f64.to_bits());

        let max = fold_extremum(&[-0.0, 0.0], Extremum::Max);
        assert2::assert!(max.to_bits() == (-0.0_f64).to_bits());
    }

    #[test]
    fn variance_uses_compensated_welford_terms() {
        let small = over_time_variance(&[1.0, 1e-16, 1e-16, 1e-16]);
        assert2::assert!(small.to_bits() == 0x3fc7_ffff_ffff_fffe);

        let large = over_time_variance(&[1e-16, 1e16, 1e16, 5.0, 1e8, -1e8]);
        assert2::assert!(large.to_bits() == 0x4671_87bd_f63d_b730);
    }

    #[test]
    fn mean_uses_compensated_updates() {
        let mean = over_time_mean(&[1e16, 1e-16, 1e-16, -1e16]);
        assert2::assert!(mean.to_bits() == 0.25_f64.to_bits());
    }

    #[test]
    fn infinite_mean_guard_matches_prometheus_cases() {
        for (mean, value, want) in [
            (f64::INFINITY, f64::INFINITY, true),
            (f64::INFINITY, 1.0, true),
            (f64::NEG_INFINITY, f64::NEG_INFINITY, true),
            (f64::NEG_INFINITY, -1.0, true),
            (f64::INFINITY, f64::NEG_INFINITY, false),
            (f64::NEG_INFINITY, f64::INFINITY, false),
            (f64::INFINITY, f64::NAN, false),
            (1.0, 1.0, false),
        ] {
            assert2::assert!(keep_infinite_mean(mean, value) == want);
        }
    }

    #[test]
    fn kahan_sum_inc_recovers_lost_low_bits() {
        // Both operand orders (|sum| >= |increment| and the swapped branch)
        // recover the low bits into the compensation term.
        for (increment, initial_sum) in [(1e-16, 1.0), (1.0, 1e-16)] {
            let (sum, comp) = kahan_sum_inc(increment, initial_sum, 0.0);
            assert2::assert!(sum.to_bits() == 1.0_f64.to_bits());
            assert2::assert!(comp.to_bits() == 1e-16_f64.to_bits());
        }
    }

    #[test]
    fn quantile_boundaries_match_prometheus() {
        let values = [3.0, 1.0, 2.0];
        for (phi, want) in [
            (-0.1, f64::NEG_INFINITY),
            (0.0, 1.0),
            (1.0, 3.0),
            (1.1, f64::INFINITY),
        ] {
            let got = quantile_value(phi, &values).unwrap();
            // Out-of-range phi yields an exact signed infinity; in-range keeps
            // the epsilon comparison.
            let matches = if want.is_infinite() {
                got.to_bits() == want.to_bits()
            } else {
                approx_eq(got, want)
            };
            assert2::assert!(matches);
        }
    }

    fn dict_i64(values: Vec<i64>, ranges: impl IntoIterator<Item = (u32, u32)>) -> ArrayRef {
        Arc::new(
            RangeArray::from_ranges(Arc::new(Int64Array::from(values)) as ArrayRef, ranges)
                .unwrap()
                .into_dict_array()
                .unwrap(),
        )
    }

    fn dict_f64(values: Vec<f64>, ranges: impl IntoIterator<Item = (u32, u32)>) -> ArrayRef {
        Arc::new(
            RangeArray::from_ranges(Arc::new(Float64Array::from(values)) as ArrayRef, ranges)
                .unwrap()
                .into_dict_array()
                .unwrap(),
        )
    }

    fn invoke_sum_with_columns(
        rows: usize,
        eval_col: ArrayRef,
        ts_dict: ArrayRef,
        val_dict: ArrayRef,
    ) -> DfResult<ColumnarValue> {
        let return_field = Arc::new(Field::new("out", DataType::Float64, true));
        let arg_fields = vec![
            Arc::new(Field::new("eval_timestamp", DataType::Int64, false)),
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
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(eval_col),
                ColumnarValue::Array(ts_dict),
                ColumnarValue::Array(val_dict),
            ],
            arg_fields,
            number_rows: rows,
            return_field,
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
        OverTimeUdf::new(OverTimeFamily::Sum).invoke_with_args(args)
    }

    #[test]
    fn invoke_rejects_each_row_count_mismatch() {
        let err = invoke_sum_with_columns(
            2,
            Arc::new(Int64Array::from(vec![60_000, 120_000])) as ArrayRef,
            dict_i64(vec![60_000], [(0, 1)]),
            dict_f64(vec![1.0, 2.0], [(0, 1), (1, 1)]),
        )
        .unwrap_err()
        .to_string();
        assert2::assert!(err.contains("row-count mismatch"));

        let err = invoke_sum_with_columns(
            2,
            Arc::new(Int64Array::from(vec![60_000])) as ArrayRef,
            dict_i64(vec![60_000, 120_000], [(0, 1), (1, 1)]),
            dict_f64(vec![1.0, 2.0], [(0, 1), (1, 1)]),
        )
        .unwrap_err()
        .to_string();
        assert2::assert!(err.contains("row-count mismatch"));
    }

    #[test]
    fn scalar_f64_rejects_empty_or_null_array_fallback() {
        let empty = ColumnarValue::Array(Arc::new(Float64Array::from(Vec::<f64>::new())));
        assert2::assert!(scalar_f64(&empty, "phi", "prom_quantile_over_time").is_err());

        let null = ColumnarValue::Array(Arc::new(Float64Array::from(vec![None])));
        assert2::assert!(scalar_f64(&null, "phi", "prom_quantile_over_time").is_err());
    }

    /// `last_over_time` returns the latest sample's value even when the window
    /// is presented out of timestamp order.
    #[test]
    fn last_uses_max_timestamp() {
        let window: &[(i64, &[i64], &[f64])] =
            &[(300_000, &[60_000, 300_000, 120_000], &[1.0, 9.0, 2.0])];
        assert2::assert!(approx_eq(
            run_udf(OverTimeFamily::Last, window, 0.0)[0],
            9.0
        ));
    }

    /// An empty window yields NULL (no Prometheus value), not a NaN sentinel —
    /// so the assembler drops the series and aggregates skip it.
    #[test]
    fn empty_window_yields_null() {
        let window: &[(i64, &[i64], &[f64])] = &[(60_000, &[], &[])];
        for (family, phi) in [
            (OverTimeFamily::Sum, 0.0),
            (OverTimeFamily::Count, 0.0),
            (OverTimeFamily::Present, 0.0),
            (OverTimeFamily::Quantile, 0.5),
        ] {
            assert2::assert!(run_udf_nullable(family, window, phi)[0].is_none());
        }
    }

    /// A genuinely-computed reduction is kept as a non-null float even when it is
    /// itself NaN (a window holding a NaN sample): the cell is non-null, so
    /// downstream aggregates propagate it rather than skip it.
    #[test]
    fn genuine_nan_reduction_is_kept_non_null() {
        // sum over [NaN, 1.0] is a genuine NaN value (the window is non-empty, so
        // this is not the no-value case).
        let window: &[(i64, &[i64], &[f64])] = &[(120_000, &[60_000, 120_000], &[f64::NAN, 1.0])];
        let out = run_udf_nullable(OverTimeFamily::Sum, window, 0.0);
        assert2::assert!(out[0].is_some());
        assert2::assert!(out[0].unwrap().is_nan());
    }

    /// H9: `min`/`max_over_time` IGNORE NaN. A NaN sample never displaces a
    /// non-NaN extremum, regardless of position; a window's extremum is over its
    /// non-NaN samples, and is NaN only when *every* sample is NaN.
    #[test]
    fn min_max_over_time_ignore_nan() {
        let cases: &[(&[f64], f64, f64)] = &[
            // {NaN, 1, 2}: min=1, max=2 (the leading NaN is folded out).
            (&[f64::NAN, 1.0, 2.0], 1.0, 2.0),
            // {1, NaN}: a trailing NaN never displaces the running extremum.
            (&[1.0, f64::NAN], 1.0, 1.0),
            // {NaN, NaN}: an all-NaN window stays NaN.
            (&[f64::NAN, f64::NAN], f64::NAN, f64::NAN),
        ];
        for &(vals, want_min, want_max) in cases {
            let ts: Vec<i64> = (1..=i64::try_from(vals.len()).unwrap())
                .map(|i| i * 60_000)
                .collect();
            let window: &[(i64, &[i64], &[f64])] = &[(*ts.last().unwrap(), &ts, vals)];
            for (family, want) in [
                (OverTimeFamily::Min, want_min),
                (OverTimeFamily::Max, want_max),
            ] {
                let got = run_udf(family, window, 0.0)[0];
                let matches = if want.is_nan() {
                    got.is_nan()
                } else {
                    approx_eq(got, want)
                };
                assert2::assert!(matches);
            }
        }
    }

    /// M16: `stdvar`/`stddev_over_time` over a large-offset close-valued window
    /// must not catastrophically cancel into a negative variance (whose `sqrt`
    /// is NaN). Welford yields the small positive population variance/stddev.
    #[test]
    fn over_time_variance_is_stable_for_large_offset_window() {
        let vals: &[f64] = &[1e8, 1e8 + 1.0, 1e8 + 2.0];
        let ts: &[i64] = &[60_000, 120_000, 180_000];
        let window: &[(i64, &[i64], &[f64])] = &[(180_000, ts, vals)];
        // population variance of {0,1,2} == 2/3; stddev == sqrt(2/3). Pinning
        // the exact positive value also rules out the cancellation failure
        // (a negative variance whose sqrt is NaN).
        let stdvar = run_udf(OverTimeFamily::Stdvar, window, 0.0)[0];
        assert2::assert!(approx_eq(stdvar, 2.0 / 3.0));
        let stddev = run_udf(OverTimeFamily::Stddev, window, 0.0)[0];
        assert2::assert!(approx_eq(stddev, (2.0_f64 / 3.0).sqrt()));
    }

    /// M17: `avg_over_time` of very-large-magnitude samples must not overflow the
    /// running sum to +/-Inf; the incremental Kahan mean stays finite.
    #[test]
    fn avg_over_time_does_not_overflow() {
        let vals: &[f64] = &[f64::MAX, f64::MAX];
        let window: &[(i64, &[i64], &[f64])] = &[(120_000, &[60_000, 120_000], vals)];
        let avg = run_udf(OverTimeFamily::Avg, window, 0.0)[0];
        // The naive `(MAX + MAX) / 2` overflows to +Inf; the mean of two equal
        // values is the value itself.
        assert2::assert!(avg.is_finite());
        assert2::assert!(approx_eq(avg, f64::MAX));
    }

    /// A multi-step window set yields one reduction per step.
    #[test]
    fn produces_per_step_vector() {
        let out = run_udf(
            OverTimeFamily::Sum,
            &[
                (120_000, &[60_000, 120_000], &[1.0, 2.0]),
                (240_000, &[180_000, 240_000], &[3.0, 4.0]),
            ],
            0.0,
        );
        check!(out.len() == 2);
        check!(approx_eq(out[0], 3.0));
        check!(approx_eq(out[1], 7.0));
    }

    /// The UDFs install onto a `SessionContext` under their Prometheus-prefixed
    /// names, so a planner can resolve them.
    #[test]
    fn register_installs_named_udfs() {
        use datafusion::execution::FunctionRegistry;

        let ctx = SessionContext::new();
        register_over_time_udfs(&ctx);
        for name in [
            "prom_sum_over_time",
            "prom_avg_over_time",
            "prom_count_over_time",
            "prom_min_over_time",
            "prom_max_over_time",
            "prom_stddev_over_time",
            "prom_stdvar_over_time",
            "prom_last_over_time",
            "prom_present_over_time",
            "prom_quantile_over_time",
        ] {
            assert2::assert!(ctx.udf(name).is_ok());
        }
    }

    /// Confirm the helper round-trips a `DictionaryArray` back into a `RangeArray`.
    #[test]
    fn decode_range_column_round_trips() {
        let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef;
        let range = RangeArray::from_ranges(values, [(0_u32, 2_u32), (2, 1)]).unwrap();
        let dict: ArrayRef = Arc::new(range.into_dict_array().unwrap());
        let back = decode_range_column(&dict, "value_range", "prom_sum_over_time").unwrap();
        check!(back.len() == 2);
        check!(back.value_slice(0).unwrap() == [1.0, 2.0]);

        let plain: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        check!(decode_range_column(&plain, "value_range", "prom_sum_over_time").is_err());
    }
}
