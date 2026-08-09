#[cfg(feature = "experimental-functions")]
use crabka_units::prelude::*;
use num_traits::ToPrimitive;
use time::OffsetDateTime;

#[cfg(feature = "experimental-functions")]
use super::{QUERY_RANGE_CONTEXT, planned::PlannedInstant};
use super::{histogram::scaled_native_histogram, labels::labels_without_metric_name};
#[cfg(test)]
use crate::planner::label_ops::SortOrder;
use crate::{
    PromqlError,
    error::Result,
    result::{QueryResult, SampleValue},
};

#[cfg(test)]
#[derive(Clone, Copy)]
pub(super) enum ClampKind {
    Both,
    Min,
    Max,
}

#[cfg(test)]
impl ClampKind {
    pub(super) fn argument_count(self) -> usize {
        match self {
            Self::Both => 3,
            Self::Min | Self::Max => 2,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum CalendarFn {
    Year,
    Month,
    DayOfMonth,
    DayOfWeek,
    DayOfYear,
    DaysInMonth,
    Hour,
    Minute,
}

impl CalendarFn {
    pub(super) fn apply(self, unix_seconds: f64) -> f64 {
        if !unix_seconds.is_finite() {
            return f64::NAN;
        }
        let Some(unix_seconds) = unix_seconds.to_i64() else {
            return f64::NAN;
        };
        let Ok(timestamp) = OffsetDateTime::from_unix_timestamp(unix_seconds) else {
            return f64::NAN;
        };
        match self {
            Self::Year => f64::from(timestamp.year()),
            Self::Month => f64::from(timestamp.month() as u8),
            Self::DayOfMonth => f64::from(timestamp.day()),
            Self::DayOfWeek => f64::from(timestamp.weekday().number_days_from_sunday()),
            Self::DayOfYear => f64::from(timestamp.ordinal()),
            Self::DaysInMonth => {
                f64::from(days_in_month(timestamp.year(), timestamp.month() as u8))
            }
            Self::Hour => f64::from(timestamp.hour()),
            Self::Minute => f64::from(timestamp.minute()),
        }
    }
}

/// Maps a `PromQL` calendar-function name to its `CalendarFn` variant.
///
/// The mapping mirrors the calendar arms of `PromqlEngine::eval_instant_call`.
/// This function returns `None` for any other function, so the planner dispatch
/// falls through.
pub(super) fn calendar_fn_from_function_name(name: &str) -> Option<CalendarFn> {
    Some(match name {
        "year" => CalendarFn::Year,
        "month" => CalendarFn::Month,
        "day_of_month" => CalendarFn::DayOfMonth,
        "day_of_week" => CalendarFn::DayOfWeek,
        "day_of_year" => CalendarFn::DayOfYear,
        "days_in_month" => CalendarFn::DaysInMonth,
        "hour" => CalendarFn::Hour,
        "minute" => CalendarFn::Minute,
        _ => return None,
    })
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(super) enum SortDirection {
    Ascending,
    Descending,
}

#[cfg(test)]
impl From<SortDirection> for SortOrder {
    fn from(direction: SortDirection) -> Self {
        match direction {
            SortDirection::Ascending => Self::Ascending,
            SortDirection::Descending => Self::Descending,
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(super) enum UnaryFloatFn {
    Ceil,
    Floor,
    Sgn,
    Abs,
    Sqrt,
    Exp,
    Ln,
    Log2,
    Log10,
    Sin,
    Sinh,
    Cos,
    Cosh,
    Tan,
    Tanh,
    Asin,
    Asinh,
    Acos,
    Acosh,
    Atan,
    Atanh,
    Deg,
    Rad,
}

#[cfg(test)]
impl UnaryFloatFn {
    pub(super) fn apply(self, value: f64) -> f64 {
        match self {
            Self::Ceil => value.ceil(),
            Self::Floor => value.floor(),
            Self::Abs => value.abs(),
            Self::Sqrt => value.sqrt(),
            Self::Exp => value.exp(),
            Self::Ln => value.ln(),
            Self::Log2 => value.log2(),
            Self::Log10 => value.log10(),
            Self::Sin => value.sin(),
            Self::Sinh => value.sinh(),
            Self::Cos => value.cos(),
            Self::Cosh => value.cosh(),
            Self::Tan => value.tan(),
            Self::Tanh => value.tanh(),
            Self::Asin => value.asin(),
            Self::Asinh => value.asinh(),
            Self::Acos => value.acos(),
            Self::Acosh => value.acosh(),
            Self::Atan => value.atan(),
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
        }
    }
}

#[cfg(test)]
pub(super) fn clamp_float(value: f64, min: Option<f64>, max: Option<f64>) -> f64 {
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

#[cfg(test)]
pub(super) fn round_to_nearest(value: f64, to_nearest: f64) -> f64 {
    (value / to_nearest + 0.5).floor() * to_nearest
}

fn days_in_month(year: i32, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

#[cfg(feature = "experimental-functions")]
#[derive(Clone, Copy)]
pub(super) enum ScalarExtremaFn {
    Max,
    Min,
}

#[cfg(feature = "experimental-functions")]
impl ScalarExtremaFn {
    pub(super) fn apply(self, left: f64, right: f64) -> f64 {
        match self {
            Self::Max => left.max(right),
            Self::Min => left.min(right),
        }
    }
}

/// Wraps a scalar `QueryResult` from a delegated interpreter call.
///
/// The result becomes a `PlannedInstant::PrecomputedScalar`. A non-scalar result
/// is impossible for these callers. This function still maps such a result to a
/// canonical error instead of a panic.
#[cfg(feature = "experimental-functions")]
pub(super) fn scalar_call_to_planned(result: &QueryResult) -> Result<PlannedInstant> {
    match *result {
        QueryResult::Scalar { ts_ms, value } => {
            Ok(PlannedInstant::PrecomputedScalar { ts_ms, value })
        }
        _ => Err(PromqlError::Plan(
            "expected a scalar result from an experimental scalar call".to_string(),
        )),
    }
}

/// Negates an already-evaluated instant query result.
///
/// This function mirrors the `PromQL` unary `-` operator. A scalar flips sign.
/// An instant vector flips each sample and drops `__name__`: floats flip by
/// negation, and native histograms flip through
/// `scaled_native_histogram(_, -1.0)`. A range-matrix or string input is a hard
/// error. Both the interpreter and the operator path route through this
/// function, so they cannot diverge.
pub(super) fn negate_query_result(operand: QueryResult) -> Result<QueryResult> {
    match operand {
        QueryResult::Scalar { ts_ms, value } => Ok(QueryResult::Scalar {
            ts_ms,
            value: -value,
        }),
        QueryResult::InstantVector(samples) => Ok(QueryResult::InstantVector(
            samples
                .into_iter()
                .map(|mut sample| {
                    sample.value = match sample.value {
                        SampleValue::Float(value) => SampleValue::Float(-value),
                        SampleValue::Histogram(histogram) => {
                            SampleValue::Histogram(scaled_native_histogram(&histogram, -1.0))
                        }
                    };
                    sample.labels = labels_without_metric_name(&sample.labels);
                    sample
                })
                .collect(),
        )),
        QueryResult::RangeMatrix(_) => Err(PromqlError::Plan(
            "unary expression requires scalar or instant-vector input".to_string(),
        )),
        QueryResult::Str { .. } => Err(PromqlError::Plan(
            "unary expression does not support string input".to_string(),
        )),
    }
}

#[cfg(feature = "experimental-functions")]
#[derive(Clone, Copy)]
pub(super) enum DurationHelper {
    Range,
    Step,
    Start,
    End,
}

#[cfg(feature = "experimental-functions")]
impl DurationHelper {
    pub(super) fn value_ms(self) -> i64 {
        QUERY_RANGE_CONTEXT
            .try_with(|context| match self {
                Self::Range => context.end_ms.saturating_sub(context.start_ms),
                Self::Step => context.step.millis_i64(),
                Self::Start => context.start_ms,
                Self::End => context.end_ms,
            })
            .unwrap_or(0)
    }
}
