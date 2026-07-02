//! Per-row scalar math / trig / clamp / round / sgn `PromQL` functions as
//! `DataFusion` [`ScalarUDF`]s.
//!
//! Each UDF consumes a single `Float64` `value` column (the per-series value of
//! the inner instant vector) and produces a `Float64` result, one value per row.
//! `clamp`/`clamp_min`/`clamp_max` thread their bound(s) and `round` threads its
//! optional `to_nearest` as additional leading `Float64` scalar columns.
//!
//! The math is a byte-for-byte port of the tree-walking interpreter's
//! `UnaryFloatFn::apply`, `clamp_float`, and `round_to_nearest`, so the operator
//! path and the interpreter agree on every number — including the edge values a
//! `DataFusion` built-in math expression might round differently (`ln(0)`,
//! `sqrt(-1)`, `sgn(NaN)`/`sgn(-0.0)`, the `.5` rounding direction). Using a UDF
//! sidesteps having to audit each built-in against Prometheus.
//!
//! # Call convention
//!
//! - Unary families (`abs`, `ceil`, …, `deg`, `rad`): `prom_<fn>(value)`.
//! - `clamp_min`/`clamp_max`: `prom_clamp_min(bound, value)` /
//!   `prom_clamp_max(bound, value)` — the bound leads.
//! - `clamp`: `prom_clamp(min, max, value)` — both bounds lead.
//! - `round`: `prom_round(to_nearest, value)` — `to_nearest` leads.
//!
//! Genuine NaN is preserved (never dropped): `f(NaN)` and `sqrt(-1)` render as
//! `NaN`, matching the interpreter, which keeps every float sample.

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, Float64Array, Float64Builder},
    datatypes::DataType,
};
use datafusion::{
    common::{DataFusionError, Result as DfResult, ScalarValue},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::SessionContext,
};

/// Which per-row scalar function a [`ScalarMathUdf`] evaluates.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ScalarMathOp {
    Abs,
    Ceil,
    Floor,
    Sqrt,
    Exp,
    Ln,
    Log2,
    Log10,
    Sgn,
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,
    Sinh,
    Cosh,
    Tanh,
    Asinh,
    Acosh,
    Atanh,
    Deg,
    Rad,
    /// `round(v, to_nearest?)` — `to_nearest` is the leading scalar column.
    Round,
    /// `clamp_min(v, min)` — `min` is the leading scalar column.
    ClampMin,
    /// `clamp_max(v, max)` — `max` is the leading scalar column.
    ClampMax,
    /// `clamp(v, min, max)` — `min`, `max` are the two leading scalar columns.
    Clamp,
}

impl ScalarMathOp {
    /// The registered UDF name this op projects.
    #[must_use]
    pub fn udf_name(self) -> &'static str {
        match self {
            Self::Abs => "prom_abs",
            Self::Ceil => "prom_ceil",
            Self::Floor => "prom_floor",
            Self::Sqrt => "prom_sqrt",
            Self::Exp => "prom_exp",
            Self::Ln => "prom_ln",
            Self::Log2 => "prom_log2",
            Self::Log10 => "prom_log10",
            Self::Sgn => "prom_sgn",
            Self::Sin => "prom_sin",
            Self::Cos => "prom_cos",
            Self::Tan => "prom_tan",
            Self::Asin => "prom_asin",
            Self::Acos => "prom_acos",
            Self::Atan => "prom_atan",
            Self::Sinh => "prom_sinh",
            Self::Cosh => "prom_cosh",
            Self::Tanh => "prom_tanh",
            Self::Asinh => "prom_asinh",
            Self::Acosh => "prom_acosh",
            Self::Atanh => "prom_atanh",
            Self::Deg => "prom_deg",
            Self::Rad => "prom_rad",
            Self::Round => "prom_round",
            Self::ClampMin => "prom_clamp_min",
            Self::ClampMax => "prom_clamp_max",
            Self::Clamp => "prom_clamp",
        }
    }

    /// Number of leading `Float64` scalar columns this op threads ahead of the
    /// `value` column (`round`/`clamp_*` take bound args; unary fns take none).
    fn scalar_param_count(self) -> usize {
        match self {
            Self::Round | Self::ClampMin | Self::ClampMax => 1,
            Self::Clamp => 2,
            _ => 0,
        }
    }

    /// Total positional-argument count (`value` plus the leading scalars).
    fn arity(self) -> usize {
        self.scalar_param_count() + 1
    }

    /// Apply the op to one row. `params` are the leading scalar args in call
    /// order (`[to_nearest]` for `round`, `[min]`/`[max]` for
    /// `clamp_min`/`clamp_max`, `[min, max]` for `clamp`); `value` is the per-row
    /// instant-vector value.
    ///
    /// A direct port of the interpreter's `UnaryFloatFn::apply` / `clamp_float`
    /// / `round_to_nearest`, evaluated bit-for-bit.
    fn apply(self, value: f64, params: &[f64]) -> f64 {
        match self {
            Self::Abs => value.abs(),
            Self::Ceil => value.ceil(),
            Self::Floor => value.floor(),
            Self::Sqrt => value.sqrt(),
            Self::Exp => value.exp(),
            Self::Ln => value.ln(),
            Self::Log2 => value.log2(),
            Self::Log10 => value.log10(),
            Self::Sin => value.sin(),
            Self::Cos => value.cos(),
            Self::Tan => value.tan(),
            Self::Asin => value.asin(),
            Self::Acos => value.acos(),
            Self::Atan => value.atan(),
            Self::Sinh => value.sinh(),
            Self::Cosh => value.cosh(),
            Self::Tanh => value.tanh(),
            Self::Asinh => value.asinh(),
            Self::Acosh => value.acosh(),
            Self::Atanh => value.atanh(),
            Self::Deg => value.to_degrees(),
            Self::Rad => value.to_radians(),
            Self::Sgn => {
                if value.is_nan() {
                    f64::NAN
                } else if value > 0.0 {
                    1.0
                } else if value < 0.0 {
                    -1.0
                } else {
                    0.0
                }
            }
            // `round(v / to_nearest + 0.5).floor() * to_nearest`, matching
            // `round_to_nearest` (the `.5`-rounds-up direction included).
            Self::Round => round_to_nearest(value, params[0]),
            Self::ClampMin => clamp_float(value, Some(params[0]), None),
            Self::ClampMax => clamp_float(value, None, Some(params[0])),
            Self::Clamp => clamp_float(value, Some(params[0]), Some(params[1])),
        }
    }
}

/// Port of the interpreter's `round_to_nearest`.
fn round_to_nearest(value: f64, to_nearest: f64) -> f64 {
    (value / to_nearest + 0.5).floor() * to_nearest
}

/// Port of the interpreter's `clamp_float`.
fn clamp_float(value: f64, min: Option<f64>, max: Option<f64>) -> f64 {
    if min.is_some_and(f64::is_nan) || max.is_some_and(f64::is_nan) {
        return f64::NAN;
    }
    if let Some(min) = min
        && value < min
    {
        return min;
    }
    if let Some(max) = max
        && value > max
    {
        return max;
    }
    value
}

/// A `ScalarUDFImpl` over the inner instant vector's `value` column (plus any
/// leading scalar bound columns). One instance per [`ScalarMathOp`].
#[derive(Debug, PartialEq, Eq, Hash)]
struct ScalarMathUdf {
    op: ScalarMathOp,
    signature: Signature,
}

impl ScalarMathUdf {
    fn new(op: ScalarMathOp) -> Self {
        Self {
            op,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ScalarMathUdf {
    fn name(&self) -> &str {
        self.op.udf_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }

    /// `Signature::user_defined` requires bespoke coercion. Every argument is a
    /// `Float64` column (the value plus any leading scalars), so validate arity
    /// and pass the types through unchanged.
    fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
        if arg_types.len() != self.op.arity() {
            return Err(DataFusionError::Plan(format!(
                "{} expects {} arguments, got {}",
                self.name(),
                self.op.arity(),
                arg_types.len()
            )));
        }
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let name = self.name();
        if args.args.len() != self.op.arity() {
            return Err(DataFusionError::Execution(format!(
                "{name} expects {} arguments, got {}",
                self.op.arity(),
                args.args.len()
            )));
        }
        let rows = args.number_rows;
        let params = self.op.scalar_param_count();

        // The leading scalar bound columns (round's `to_nearest`, clamp's
        // bounds) are constant per query; read each once.
        let mut bounds = Vec::with_capacity(params);
        for index in 0..params {
            bounds.push(scalar_f64(&args.args[index], "bound", name)?);
        }

        // The value column trails the scalar params.
        let value_array = args.args[params].clone().into_array(rows)?;
        let values = value_array
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{name}: `value` must be Float64, got {:?}",
                    value_array.data_type()
                ))
            })?;
        if values.len() != rows {
            return Err(DataFusionError::Execution(format!(
                "{name}: row-count mismatch (value={}, rows={rows})",
                values.len()
            )));
        }

        let mut builder = Float64Builder::with_capacity(rows);
        for row in 0..rows {
            // Genuine NaN (including a null cell, treated as NaN) flows through
            // unchanged; the interpreter never drops a float sample here.
            let value = if values.is_null(row) {
                f64::NAN
            } else {
                values.value(row)
            };
            builder.append_value(self.op.apply(value, &bounds));
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

/// The scalar-math UDF for `op`.
#[must_use]
pub fn scalar_math_udf(op: ScalarMathOp) -> ScalarUDF {
    ScalarUDF::from(ScalarMathUdf::new(op))
}

/// Every scalar-math UDF, ready to register on a [`SessionContext`].
#[must_use]
pub fn scalar_math_udfs() -> Vec<ScalarUDF> {
    [
        ScalarMathOp::Abs,
        ScalarMathOp::Ceil,
        ScalarMathOp::Floor,
        ScalarMathOp::Sqrt,
        ScalarMathOp::Exp,
        ScalarMathOp::Ln,
        ScalarMathOp::Log2,
        ScalarMathOp::Log10,
        ScalarMathOp::Sgn,
        ScalarMathOp::Sin,
        ScalarMathOp::Cos,
        ScalarMathOp::Tan,
        ScalarMathOp::Asin,
        ScalarMathOp::Acos,
        ScalarMathOp::Atan,
        ScalarMathOp::Sinh,
        ScalarMathOp::Cosh,
        ScalarMathOp::Tanh,
        ScalarMathOp::Asinh,
        ScalarMathOp::Acosh,
        ScalarMathOp::Atanh,
        ScalarMathOp::Deg,
        ScalarMathOp::Rad,
        ScalarMathOp::Round,
        ScalarMathOp::ClampMin,
        ScalarMathOp::ClampMax,
        ScalarMathOp::Clamp,
    ]
    .into_iter()
    .map(scalar_math_udf)
    .collect()
}

/// Register every scalar-math UDF on `ctx` so a planner can lower onto them.
pub fn register_scalar_math_udfs(ctx: &SessionContext) {
    for udf in scalar_math_udfs() {
        ctx.register_udf(udf);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{array::Float64Array, datatypes::Field};
    use assert2::assert;

    use super::*;

    /// Invoke a `ScalarMathUdf` over a one-batch `value` column plus the given
    /// leading scalar bounds, returning the result column.
    fn run(op: ScalarMathOp, bounds: &[f64], values: &[f64]) -> Vec<f64> {
        let udf = ScalarMathUdf::new(op);
        let rows = values.len();
        let mut call_args = Vec::new();
        let mut arg_fields = Vec::new();
        for bound in bounds {
            call_args.push(ColumnarValue::Scalar(ScalarValue::Float64(Some(*bound))));
            arg_fields.push(Arc::new(Field::new("bound", DataType::Float64, false)));
        }
        let value_col: ArrayRef = Arc::new(Float64Array::from(values.to_vec()));
        call_args.push(ColumnarValue::Array(value_col));
        arg_fields.push(Arc::new(Field::new("value", DataType::Float64, true)));

        let args = ScalarFunctionArgs {
            args: call_args,
            arg_fields,
            number_rows: rows,
            return_field: Arc::new(Field::new("out", DataType::Float64, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
        let out = udf.invoke_with_args(args).unwrap();
        let array = out.into_array(rows).unwrap();
        let floats = array.as_any().downcast_ref::<Float64Array>().unwrap();
        (0..floats.len()).map(|i| floats.value(i)).collect()
    }

    fn invoke_with_columns(
        op: ScalarMathOp,
        call_args: Vec<ColumnarValue>,
        rows: usize,
    ) -> DfResult<ColumnarValue> {
        let udf = ScalarMathUdf::new(op);
        let arg_fields = call_args
            .iter()
            .enumerate()
            .map(|(index, _)| Arc::new(Field::new(format!("arg_{index}"), DataType::Float64, true)))
            .collect();
        udf.invoke_with_args(ScalarFunctionArgs {
            args: call_args,
            arg_fields,
            number_rows: rows,
            return_field: Arc::new(Field::new("out", DataType::Float64, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        })
    }

    fn bits_eq(left: f64, right: f64) -> bool {
        left.to_bits() == right.to_bits() || (left.is_nan() && right.is_nan())
    }

    #[test]
    fn unary_matches_rust_f64() {
        // `bits_eq` treats any-NaN == any-NaN, so NaN expectations
        // (sqrt/ln of a negative, preserved rather than dropped) sit in the
        // same table as the exact cases; ln(0) -> -inf.
        for (op, input, want) in [
            (ScalarMathOp::Abs, -3.0, 3.0),
            (ScalarMathOp::Sqrt, 4.0, 2.0),
            (ScalarMathOp::Sqrt, -1.0, f64::NAN),
            (ScalarMathOp::Ln, 0.0, f64::NEG_INFINITY),
            (ScalarMathOp::Ln, -1.0, f64::NAN),
            (ScalarMathOp::Log2, 8.0, 3.0),
        ] {
            let got = run(op, &[], &[input])[0];
            assert!(
                bits_eq(got, want),
                "case {op:?}({input}): got {got}, want {want}"
            );
        }
    }

    #[test]
    fn sgn_handles_nan_and_signed_zero() {
        // -0.0 is neither > 0 nor < 0, so sgn(-0.0) = 0.0 (positive zero,
        // pinned by the bit comparison); sgn(NaN) stays NaN.
        for (input, want) in [
            (5.0, 1.0),
            (-5.0, -1.0),
            (0.0, 0.0),
            (-0.0, 0.0),
            (f64::NAN, f64::NAN),
        ] {
            let got = run(ScalarMathOp::Sgn, &[], &[input])[0];
            assert!(
                bits_eq(got, want),
                "case sgn({input}): got {got}, want {want}"
            );
        }
    }

    #[test]
    fn round_matches_interpreter_half_up() {
        // .5 rounds up (toward +inf), matching `round_to_nearest`.
        for (to_nearest, value, want) in [
            (1.0, 2.5, 3.0),
            (1.0, -2.5, -2.0),
            (5.0, 12.0, 10.0),
            (5.0, 13.0, 15.0),
        ] {
            let got = run(ScalarMathOp::Round, &[to_nearest], &[value])[0];
            assert!(
                bits_eq(got, want),
                "case round({value}, to_nearest={to_nearest}): got {got}, want {want}"
            );
        }
    }

    #[test]
    fn clamp_family_bounds_values() {
        // Signed zeros pass through unclamped (pinned by the bit comparison);
        // a NaN bound yields NaN.
        let cases: &[(ScalarMathOp, &[f64], f64, f64)] = &[
            (ScalarMathOp::ClampMin, &[0.0], -3.0, 0.0),
            (ScalarMathOp::ClampMin, &[0.0], 3.0, 3.0),
            (ScalarMathOp::ClampMax, &[10.0], 42.0, 10.0),
            (ScalarMathOp::Clamp, &[0.0, 100.0], 150.0, 100.0),
            (ScalarMathOp::Clamp, &[0.0, 100.0], -5.0, 0.0),
            (ScalarMathOp::ClampMin, &[0.0], -0.0, -0.0),
            (ScalarMathOp::ClampMax, &[-0.0], 0.0, 0.0),
            (ScalarMathOp::ClampMin, &[f64::NAN], 3.0, f64::NAN),
        ];
        for &(op, bounds, value, want) in cases {
            let got = run(op, bounds, &[value])[0];
            assert!(
                bits_eq(got, want),
                "case {op:?}(bounds={bounds:?}, value={value}): got {got}, want {want}"
            );
        }
    }

    #[test]
    fn scalar_bound_array_must_have_non_null_first_value() {
        let values: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));

        assert!(
            invoke_with_columns(
                ScalarMathOp::ClampMin,
                vec![
                    ColumnarValue::Array(Arc::new(Float64Array::from(Vec::<f64>::new()))),
                    ColumnarValue::Array(Arc::clone(&values)),
                ],
                1,
            )
            .is_err()
        );
        assert!(
            invoke_with_columns(
                ScalarMathOp::ClampMin,
                vec![
                    ColumnarValue::Array(Arc::new(Float64Array::from(vec![None::<f64>]))),
                    ColumnarValue::Array(values),
                ],
                1,
            )
            .is_err()
        );
    }

    #[test]
    fn nan_value_flows_through() {
        for op in [ScalarMathOp::Sin, ScalarMathOp::Abs, ScalarMathOp::Ceil] {
            assert!(run(op, &[], &[f64::NAN])[0].is_nan(), "case {op:?}");
        }
    }
}
