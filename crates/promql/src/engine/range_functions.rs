use crabka_blockstore::Labels;
use crabka_metrics::{BucketSpan, NativeHistogram, ResetHint};
use crabka_units::prelude::*;
use num_traits::ToPrimitive;

use super::{
    RangeEval, add_compatible_native_histogram, labels::labels_without_metric_name,
    native_histograms_are_range_compatible, result_utils::quantile_value,
    scale_native_histogram_values, selector::timestamp_seconds,
};
#[cfg(feature = "experimental-functions")]
use crate::error::{PromqlError, Result};
use crate::{
    planner::ExtendedSelectorModifier,
    result::{InstantSample, RangeSeries, SampleValue},
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RangeFn {
    Rate,
    Increase,
    Delta,
    Changes,
    Resets,
}

#[derive(Clone, Copy)]
pub(super) enum IrateFn {
    Irate,
    Idelta,
}

#[derive(Clone, Copy)]
pub(super) enum OverTimeFn {
    Sum,
    Avg,
    Count,
    Min,
    Max,
    Stddev,
    Stdvar,
    Mad,
    First,
    Last,
    TsOfFirst,
    TsOfLast,
    TsOfMin,
    TsOfMax,
    Present,
}

impl OverTimeFn {
    fn preserves_metric_name(self) -> bool {
        matches!(self, Self::First | Self::Last)
    }
}

/// A range or `*_over_time` function applied to an evaluated range vector.
///
/// The range vector is a [`RangeEval`]. This type holds any scalar parameters
/// the caller resolved. It is the outer half of a range-function evaluation:
/// the per-series fold that turns each window of `(end - range, end]` samples
/// into one instant sample. The interpreter (`eval_*_call`) and the recursive
/// planner's subquery dispatch both build one of these and apply it through
/// [`apply_outer_range_fn`]. The operator path therefore matches the
/// interpreter byte-for-byte for any range vector it gets.
///
/// `absent` and `absent_over_time` build an absent-labels series, and the
/// scalar-typed helpers `time` and `pi` return scalars. None of them are
/// range-vector folds, so this type does not cover them. The experimental
/// `double_exponential_smoothing` holds its two factors. The non-experimental
/// build cannot reach it.
#[derive(Clone, Copy)]
pub(super) enum OuterRangeFn {
    Range(RangeFn),
    InstantDelta(IrateFn),
    Deriv,
    OverTime(OverTimeFn),
    QuantileOverTime(f64),
    PredictLinear(Time),
    #[cfg(feature = "experimental-functions")]
    DoubleExponentialSmoothing {
        smoothing: f64,
        trend: f64,
    },
}

/// Applies an [`OuterRangeFn`] over an evaluated range vector.
///
/// This function returns the instant vector at `time_ms`. It is the one shared
/// implementation of every range and `*_over_time` function's per-series fold.
/// The interpreter and the planner's subquery path both route through it, so
/// they cannot diverge.
pub(super) fn apply_outer_range_fn(
    range: RangeEval,
    outer: OuterRangeFn,
    time_ms: i64,
) -> Vec<InstantSample> {
    range
        .series
        .into_iter()
        .filter_map(|series| {
            outer_range_sample_from_series(
                &series,
                range.end_ms,
                range.range,
                outer,
                range.modifier,
            )
            .map(|(labels, value)| InstantSample {
                labels,
                ts_ms: time_ms,
                value,
            })
        })
        .collect()
}

/// Folds one series' window into its `(result labels, value)`.
///
/// The fold matches what each interpreter `eval_*_call` does per series. This
/// function returns `None` for a no-value window, and the result drops that
/// series.
fn outer_range_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    outer: OuterRangeFn,
    modifier: Option<ExtendedSelectorModifier>,
) -> Option<(Labels, SampleValue)> {
    match outer {
        OuterRangeFn::Range(kind) => {
            range_function_sample_from_series(series, range_end_ms, range, kind, modifier)
                .map(|value| (labels_without_metric_name(&series.labels), value))
        }
        OuterRangeFn::InstantDelta(kind) => {
            instant_delta_sample_from_series(series, range_end_ms, range, kind).map(|value| {
                (
                    labels_without_metric_name(&series.labels),
                    SampleValue::Float(value),
                )
            })
        }
        OuterRangeFn::Deriv => deriv_sample_from_series(series, range_end_ms, range).map(|value| {
            (
                labels_without_metric_name(&series.labels),
                SampleValue::Float(value),
            )
        }),
        OuterRangeFn::OverTime(kind) => {
            over_time_sample_from_series(series, range_end_ms, range, kind).map(|value| {
                let labels = if kind.preserves_metric_name() {
                    series.labels.clone()
                } else {
                    labels_without_metric_name(&series.labels)
                };
                (labels, value)
            })
        }
        OuterRangeFn::QuantileOverTime(quantile) => {
            quantile_over_time_sample_from_series(series, range_end_ms, range, quantile).map(
                |value| {
                    (
                        labels_without_metric_name(&series.labels),
                        SampleValue::Float(value),
                    )
                },
            )
        }
        OuterRangeFn::PredictLinear(duration) => {
            predict_linear_sample_from_series(series, range_end_ms, range, duration).map(|value| {
                (
                    labels_without_metric_name(&series.labels),
                    SampleValue::Float(value),
                )
            })
        }
        #[cfg(feature = "experimental-functions")]
        OuterRangeFn::DoubleExponentialSmoothing { smoothing, trend } => {
            double_exponential_smoothing_sample_from_series(
                series,
                range_end_ms,
                range,
                smoothing,
                trend,
            )
            .map(|value| {
                (
                    labels_without_metric_name(&series.labels),
                    SampleValue::Float(value),
                )
            })
        }
    }
}

fn range_function_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    kind: RangeFn,
    modifier: Option<ExtendedSelectorModifier>,
) -> Option<SampleValue> {
    let range_start_ms = range_end_ms.saturating_sub(range.millis_i64());
    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    let mut histograms = Vec::new();
    for (timestamp, value) in &series.samples {
        let in_range = match modifier {
            Some(ExtendedSelectorModifier::Anchored) => *timestamp <= range_end_ms,
            Some(ExtendedSelectorModifier::Smoothed) => true,
            None => *timestamp > range_start_ms && *timestamp <= range_end_ms,
        };
        if !in_range {
            continue;
        }
        match value {
            SampleValue::Float(value) => {
                if !histograms.is_empty() {
                    return None;
                }
                timestamps.push(*timestamp);
                values.push(*value);
            }
            SampleValue::Histogram(histogram) => {
                if !values.is_empty() {
                    return None;
                }
                timestamps.push(*timestamp);
                histograms.push(histogram.clone());
            }
        }
    }

    if matches!(modifier, Some(ExtendedSelectorModifier::Anchored)) && !values.is_empty() {
        let value = anchored_float_range_value(&timestamps, &values, range_start_ms, range, kind)?;
        return Some(SampleValue::Float(value));
    }
    if matches!(modifier, Some(ExtendedSelectorModifier::Smoothed)) && !values.is_empty() {
        let value = smoothed_float_range_value(
            &timestamps,
            &values,
            range_start_ms,
            range_end_ms,
            range,
            kind,
        )?;
        return Some(SampleValue::Float(value));
    }

    if !histograms.is_empty() {
        if matches!(kind, RangeFn::Resets) {
            return count_histogram_resets(&histograms).map(SampleValue::Float);
        }
        return range_histogram_sample(
            &timestamps,
            &histograms,
            range_start_ms,
            range_end_ms,
            range,
            kind,
        )
        .map(SampleValue::Histogram);
    }
    let value = match kind {
        RangeFn::Changes => count_changes(&values),
        RangeFn::Resets => count_resets(&values),
        RangeFn::Rate | RangeFn::Increase | RangeFn::Delta => extrapolated_rate(
            &timestamps,
            &values,
            range_start_ms,
            range_end_ms,
            range,
            kind,
        ),
    }?;
    Some(SampleValue::Float(value))
}

fn anchored_float_range_value(
    timestamps: &[i64],
    values: &[f64],
    range_start_ms: i64,
    range: Time,
    kind: RangeFn,
) -> Option<f64> {
    let mut selected = Vec::new();
    if matches!(kind, RangeFn::Changes | RangeFn::Resets) {
        let has_after_start = timestamps
            .iter()
            .any(|timestamp| *timestamp > range_start_ms);
        if has_after_start {
            if let Some(index) = timestamps
                .iter()
                .rposition(|timestamp| *timestamp <= range_start_ms)
            {
                selected.push((*timestamps.get(index)?, values.get(index).copied()?));
            }
            selected.extend(timestamps.iter().zip(values.iter()).filter_map(
                |(timestamp, value)| (*timestamp > range_start_ms).then_some((*timestamp, *value)),
            ));
        } else if let Some(start_index) = timestamps
            .iter()
            .position(|timestamp| *timestamp == range_start_ms)
        {
            if let Some(previous_index) = timestamps[..start_index]
                .iter()
                .rposition(|timestamp| *timestamp < range_start_ms)
            {
                selected.push((
                    *timestamps.get(previous_index)?,
                    values.get(previous_index).copied()?,
                ));
            }
            selected.push((
                *timestamps.get(start_index)?,
                values.get(start_index).copied()?,
            ));
        }
    } else {
        if let Some(index) = timestamps
            .iter()
            .rposition(|timestamp| *timestamp <= range_start_ms)
        {
            selected.push((*timestamps.get(index)?, values.get(index).copied()?));
        }
        selected.extend(
            timestamps
                .iter()
                .zip(values.iter())
                .filter_map(|(timestamp, value)| {
                    (*timestamp > range_start_ms).then_some((*timestamp, *value))
                }),
        );
    }
    if selected.is_empty() {
        return None;
    }
    if selected.len() == 1 && selected[0].0 <= range_start_ms {
        return None;
    }
    let selected_values = selected.iter().map(|(_, value)| *value).collect::<Vec<_>>();

    match kind {
        RangeFn::Changes => count_changes(&selected_values),
        RangeFn::Resets => count_resets(&selected_values),
        RangeFn::Delta => Some(selected_values.last()? - selected_values.first()?),
        RangeFn::Increase | RangeFn::Rate => {
            let result = counter_delta(&selected_values)?;
            if kind == RangeFn::Rate {
                let range_seconds = range.secs_f64();
                if range_seconds <= 0.0 {
                    return None;
                }
                Some(result / range_seconds)
            } else {
                let _ = timestamps;
                Some(result)
            }
        }
    }
}

fn counter_delta(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return Some(0.0);
    }
    let mut result = values.last()? - values.first()?;
    for window in values.windows(2) {
        if window[1] < window[0] {
            result += window[0];
        }
    }
    Some(result)
}

fn smoothed_float_range_value(
    timestamps: &[i64],
    values: &[f64],
    range_start_ms: i64,
    range_end_ms: i64,
    range: Time,
    kind: RangeFn,
) -> Option<f64> {
    if !matches!(kind, RangeFn::Delta | RangeFn::Increase | RangeFn::Rate) {
        return None;
    }
    if timestamps.len() != values.len() || timestamps.is_empty() {
        return None;
    }

    let smoothed_values = if matches!(kind, RangeFn::Increase | RangeFn::Rate) {
        counter_corrected_values(values)?
    } else {
        values.to_vec()
    };
    let start = boundary_value(timestamps, &smoothed_values, range_start_ms)?;
    let end = boundary_value(timestamps, &smoothed_values, range_end_ms)?;
    let mut result = end - start;
    if matches!(kind, RangeFn::Increase | RangeFn::Rate) && result < 0.0 {
        result = 0.0;
    }
    if kind == RangeFn::Rate {
        let range_seconds = range.secs_f64();
        if range_seconds <= 0.0 {
            return None;
        }
        result /= range_seconds;
    }
    Some(result)
}

fn counter_corrected_values(values: &[f64]) -> Option<Vec<f64>> {
    let mut out = Vec::with_capacity(values.len());
    let mut correction = 0.0;
    let mut previous = *values.first()?;
    out.push(previous);
    for &value in &values[1..] {
        if value < previous {
            correction += previous;
        }
        out.push(value + correction);
        previous = value;
    }
    Some(out)
}

fn boundary_value(timestamps: &[i64], values: &[f64], target_ms: i64) -> Option<f64> {
    if timestamps.len() != values.len() || timestamps.is_empty() {
        return None;
    }
    if timestamps.len() == 1 {
        return values.first().copied();
    }
    if let Some(index) = timestamps
        .iter()
        .position(|timestamp| *timestamp == target_ms)
    {
        return values.get(index).copied();
    }
    if let Some(after_index) = timestamps
        .iter()
        .position(|timestamp| *timestamp > target_ms)
    {
        if after_index == 0 {
            return values.first().copied();
        }
        return interpolate_boundary(
            timestamps[after_index - 1],
            values[after_index - 1],
            timestamps[after_index],
            values[after_index],
            target_ms,
        );
    }
    let last_index = timestamps.len() - 1;
    let interval = timestamps[last_index].saturating_sub(timestamps[last_index - 1]);
    if target_ms.saturating_sub(timestamps[last_index]).to_f64()? > interval.to_f64()? * 1.1 {
        return values.last().copied();
    }
    interpolate_boundary(
        timestamps[last_index - 1],
        values[last_index - 1],
        timestamps[last_index],
        values[last_index],
        target_ms,
    )
}

pub(super) fn instant_smoothed_boundary_value(
    timestamps: &[i64],
    values: &[f64],
    target_ms: i64,
) -> Option<f64> {
    if timestamps.len() != values.len() || timestamps.is_empty() {
        return None;
    }
    if target_ms <= *timestamps.first()? {
        return values.first().copied();
    }
    if target_ms >= *timestamps.last()? {
        return values.last().copied();
    }
    boundary_value(timestamps, values, target_ms)
}

fn interpolate_boundary(
    left_ts: i64,
    left_value: f64,
    right_ts: i64,
    right_value: f64,
    target_ms: i64,
) -> Option<f64> {
    let interval = (right_ts - left_ts).to_f64()?;
    if interval <= 0.0 {
        return None;
    }
    let ratio = (target_ms - left_ts).to_f64()? / interval;
    Some(left_value + (right_value - left_value) * ratio)
}

fn range_histogram_sample(
    timestamps: &[i64],
    histograms: &[NativeHistogram],
    range_start_ms: i64,
    range_end_ms: i64,
    range: Time,
    kind: RangeFn,
) -> Option<NativeHistogram> {
    if !matches!(kind, RangeFn::Rate | RangeFn::Increase | RangeFn::Delta) || histograms.len() < 2 {
        return None;
    }
    let first = histograms.first()?;
    let last = histograms.last()?;
    if !histograms
        .windows(2)
        .all(|window| native_histograms_are_range_compatible(&window[0], &window[1]))
    {
        return None;
    }
    let resets = histogram_reset_indices(histograms);
    let extrapolation = HistogramExtrapolation {
        timestamps,
        reset_indices: &resets,
        range_start_ms,
        range_end_ms,
        range,
        kind,
    };

    let mut out = last.clone();
    out.count = extrapolated_histogram_component(
        &extrapolation,
        &histograms
            .iter()
            .map(|histogram| histogram.count)
            .collect::<Vec<_>>(),
    )?;
    out.sum = extrapolated_histogram_component(
        &extrapolation,
        &histograms
            .iter()
            .map(|histogram| histogram.sum)
            .collect::<Vec<_>>(),
    )?;
    out.zero_count = extrapolated_histogram_component(
        &extrapolation,
        &histograms
            .iter()
            .map(|histogram| histogram.zero_count)
            .collect::<Vec<_>>(),
    )?;
    out.positive_counts = extrapolated_histogram_counts(&extrapolation, histograms, |histogram| {
        &histogram.positive_counts
    })?;
    (out.positive_spans, out.positive_counts) =
        compact_histogram_spans(&out.positive_spans, &out.positive_counts);
    out.negative_counts = extrapolated_histogram_counts(&extrapolation, histograms, |histogram| {
        &histogram.negative_counts
    })?;
    (out.negative_spans, out.negative_counts) =
        compact_histogram_spans(&out.negative_spans, &out.negative_counts);
    if matches!(kind, RangeFn::Delta) || out.is_nhcb() && !resets.is_empty() {
        out.reset_hint = ResetHint::Gauge;
    }
    out.start_timestamp_ms = first.start_timestamp_ms.or(last.start_timestamp_ms);
    Some(out)
}

fn compact_histogram_spans(spans: &[BucketSpan], counts: &[f64]) -> (Vec<BucketSpan>, Vec<f64>) {
    let mut index = 0;
    let mut count_index = 0;
    let mut buckets = Vec::new();
    for (span_index, span) in spans.iter().enumerate() {
        if span_index == 0 {
            index = span.offset;
        } else {
            index += span.offset;
        }
        for _ in 0..span.length {
            let Some(count) = counts.get(count_index).copied() else {
                break;
            };
            buckets.push((index, count));
            index += 1;
            count_index += 1;
        }
    }
    let Some(first_non_zero) = buckets.iter().position(|(_, count)| *count != 0.0) else {
        return (Vec::new(), Vec::new());
    };
    let last_non_zero = buckets
        .iter()
        .rposition(|(_, count)| *count != 0.0)
        .expect("first non-zero bucket exists");
    let buckets = &buckets[first_non_zero..=last_non_zero];

    let mut compacted_spans = Vec::new();
    let mut compacted_counts = Vec::with_capacity(buckets.len());
    let mut span_start = None;
    let mut previous_index = 0;
    let mut previous_span_end = 0;
    for &(index, count) in buckets {
        if span_start.is_none() {
            span_start = Some(index);
        } else if index != previous_index + 1 {
            let start = span_start.expect("checked is_some");
            compacted_spans.push(BucketSpan {
                offset: start - previous_span_end,
                length: u32::try_from(previous_index - start + 1).unwrap_or(u32::MAX),
            });
            previous_span_end = previous_index + 1;
            span_start = Some(index);
        }
        compacted_counts.push(count);
        previous_index = index;
    }
    if let Some(start) = span_start {
        compacted_spans.push(BucketSpan {
            offset: start - previous_span_end,
            length: u32::try_from(previous_index - start + 1).unwrap_or(u32::MAX),
        });
    }
    (compacted_spans, compacted_counts)
}

fn extrapolated_histogram_counts(
    extrapolation: &HistogramExtrapolation<'_>,
    histograms: &[NativeHistogram],
    counts: impl Fn(&NativeHistogram) -> &[f64],
) -> Option<Vec<f64>> {
    let bucket_count = counts(histograms.first()?).len();
    let mut out = Vec::with_capacity(bucket_count);
    for index in 0..bucket_count {
        let values = histograms
            .iter()
            .map(|histogram| counts(histogram).get(index).copied())
            .collect::<Option<Vec<_>>>()?;
        out.push(extrapolated_histogram_component(extrapolation, &values)?);
    }
    Some(out)
}

struct HistogramExtrapolation<'a> {
    timestamps: &'a [i64],
    reset_indices: &'a [usize],
    range_start_ms: i64,
    range_end_ms: i64,
    range: Time,
    kind: RangeFn,
}

fn count_histogram_resets(histograms: &[NativeHistogram]) -> Option<f64> {
    if histograms.len() < 2
        || !histograms
            .windows(2)
            .all(|window| native_histograms_are_range_compatible(&window[0], &window[1]))
    {
        return None;
    }
    Some(
        histogram_reset_indices(histograms)
            .iter()
            .map(|_| 1.0)
            .sum(),
    )
}

fn histogram_reset_indices(histograms: &[NativeHistogram]) -> Vec<usize> {
    histograms
        .windows(2)
        .enumerate()
        .filter_map(|(index, window)| {
            histogram_reset_between(&window[0], &window[1]).then_some(index + 1)
        })
        .collect()
}

fn histogram_reset_between(previous: &NativeHistogram, current: &NativeHistogram) -> bool {
    current.count < previous.count
        || current.sum < previous.sum
        || current.zero_count < previous.zero_count
        || histogram_counts_reset(&previous.positive_counts, &current.positive_counts)
        || histogram_counts_reset(&previous.negative_counts, &current.negative_counts)
}

fn histogram_counts_reset(previous: &[f64], current: &[f64]) -> bool {
    previous
        .iter()
        .zip(current.iter())
        .any(|(previous, current)| current < previous)
}

fn extrapolated_histogram_component(
    extrapolation: &HistogramExtrapolation<'_>,
    values: &[f64],
) -> Option<f64> {
    if matches!(extrapolation.kind, RangeFn::Delta) {
        return extrapolated_rate(
            extrapolation.timestamps,
            values,
            extrapolation.range_start_ms,
            extrapolation.range_end_ms,
            extrapolation.range,
            extrapolation.kind,
        );
    }

    let n = extrapolation.timestamps.len();
    if n < 2 || values.len() != n {
        return None;
    }
    let mut result = values[n - 1] - values[0];
    for &reset_index in extrapolation.reset_indices {
        result += values.get(reset_index.checked_sub(1)?)?;
    }

    extrapolate_histogram_delta(
        extrapolation.timestamps,
        result,
        extrapolation.range_start_ms,
        extrapolation.range_end_ms,
        extrapolation.range,
        extrapolation.kind,
    )
}

fn extrapolate_histogram_delta(
    timestamps: &[i64],
    mut result: f64,
    range_start_ms: i64,
    range_end_ms: i64,
    range: Time,
    kind: RangeFn,
) -> Option<f64> {
    let n = timestamps.len();
    let first_ts = timestamps[0];
    let last_ts = timestamps[n - 1];
    let sampled_interval = (last_ts - first_ts).to_f64()? / 1000.0;
    if sampled_interval <= 0.0 {
        return None;
    }

    let average_duration_between_samples = sampled_interval / (n - 1).to_f64()?;
    let extrapolation_threshold = average_duration_between_samples * 1.1;
    let mut duration_to_start = (first_ts - range_start_ms).to_f64()? / 1000.0;
    let mut duration_to_end = (range_end_ms - last_ts).to_f64()? / 1000.0;

    if duration_to_start >= extrapolation_threshold {
        duration_to_start = average_duration_between_samples / 2.0;
    }
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_duration_between_samples / 2.0;
    }

    let extrapolated_interval = sampled_interval + duration_to_start + duration_to_end;
    result *= extrapolated_interval / sampled_interval;
    if kind == RangeFn::Rate {
        result /= range.secs_f64();
    }
    Some(result)
}

fn instant_delta_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    kind: IrateFn,
) -> Option<f64> {
    let range_start_ms = range_end_ms.saturating_sub(range.millis_i64());
    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    for (timestamp, value) in &series.samples {
        if *timestamp <= range_start_ms || *timestamp > range_end_ms {
            continue;
        }
        let SampleValue::Float(value) = value else {
            return None;
        };
        timestamps.push(*timestamp);
        values.push(*value);
    }
    instant_delta(&timestamps, &values, kind)
}

fn over_time_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    kind: OverTimeFn,
) -> Option<SampleValue> {
    if matches!(
        kind,
        OverTimeFn::Count | OverTimeFn::First | OverTimeFn::Last | OverTimeFn::Present
    ) {
        let sample_count = range_sample_count(series, range_end_ms, range);
        if sample_count == 0 {
            return None;
        }
        return match kind {
            OverTimeFn::Count => Some(SampleValue::Float((0..sample_count).map(|_| 1.0).sum())),
            OverTimeFn::First => range_samples(series, range_end_ms, range)
                .min_by_key(|(timestamp, _)| *timestamp)
                .map(|(_, value)| value.clone()),
            OverTimeFn::Last => range_samples(series, range_end_ms, range)
                .max_by_key(|(timestamp, _)| *timestamp)
                .map(|(_, value)| value.clone()),
            OverTimeFn::Present => Some(SampleValue::Float(1.0)),
            _ => unreachable!("over_time histogram-safe kind checked above"),
        };
    }

    if matches!(kind, OverTimeFn::Sum | OverTimeFn::Avg) {
        let histograms = histogram_range_samples(series, range_end_ms, range);
        if !histograms.is_empty() {
            return over_time_histogram_sample(&histograms, kind).map(SampleValue::Histogram);
        }
    }

    let samples = float_range_samples(series, range_end_ms, range);
    if samples.is_empty() {
        return None;
    }

    let value = match kind {
        OverTimeFn::Sum => samples.iter().map(|(_, value)| value).sum(),
        OverTimeFn::Avg => over_time_mean(samples.iter().map(|(_, value)| *value)),
        OverTimeFn::Count => unreachable!("count_over_time handled before float extraction"),
        OverTimeFn::Min => fold_over_time_extremum(&samples, ExtremumKind::Min),
        OverTimeFn::Max => fold_over_time_extremum(&samples, ExtremumKind::Max),
        OverTimeFn::Stddev => over_time_variance(&samples).sqrt(),
        OverTimeFn::Stdvar => over_time_variance(&samples),
        OverTimeFn::Mad => over_time_mad(&samples).expect("non-empty samples"),
        OverTimeFn::First => samples
            .into_iter()
            .min_by_key(|(timestamp, _)| *timestamp)
            .map(|(_, value)| value)
            .expect("non-empty samples"),
        OverTimeFn::Last => samples
            .into_iter()
            .max_by_key(|(timestamp, _)| *timestamp)
            .map(|(_, value)| value)
            .expect("non-empty samples"),
        OverTimeFn::TsOfFirst => timestamp_seconds(
            samples
                .into_iter()
                .min_by_key(|(timestamp, _)| *timestamp)
                .map(|(timestamp, _)| timestamp)
                .expect("non-empty samples"),
        ),
        OverTimeFn::TsOfLast => timestamp_seconds(
            samples
                .into_iter()
                .max_by_key(|(timestamp, _)| *timestamp)
                .map(|(timestamp, _)| timestamp)
                .expect("non-empty samples"),
        ),
        OverTimeFn::TsOfMin => timestamp_seconds(
            samples
                .into_iter()
                .min_by(|left, right| {
                    left.1
                        .total_cmp(&right.1)
                        .then_with(|| right.0.cmp(&left.0))
                })
                .map(|(timestamp, _)| timestamp)
                .expect("non-empty samples"),
        ),
        OverTimeFn::TsOfMax => timestamp_seconds(
            samples
                .into_iter()
                .max_by(|left, right| {
                    left.1
                        .total_cmp(&right.1)
                        .then_with(|| left.0.cmp(&right.0))
                })
                .map(|(timestamp, _)| timestamp)
                .expect("non-empty samples"),
        ),
        OverTimeFn::Present => unreachable!("present_over_time handled before float extraction"),
    };
    Some(SampleValue::Float(value))
}

fn over_time_histogram_sample(
    histograms: &[NativeHistogram],
    kind: OverTimeFn,
) -> Option<NativeHistogram> {
    let mut out = histograms.first()?.clone();
    for histogram in &histograms[1..] {
        add_compatible_native_histogram(&mut out, histogram).ok()?;
    }
    if matches!(kind, OverTimeFn::Avg) {
        let count: f64 = histograms.iter().map(|_| 1.0).sum();
        scale_native_histogram_values(&mut out, 1.0 / count);
    }
    Some(out)
}

fn quantile_over_time_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    quantile: f64,
) -> Option<f64> {
    let mut values = float_range_samples(series, range_end_ms, range)
        .into_iter()
        .map(|(_, value)| value)
        .collect::<Vec<_>>();
    quantile_value(quantile, &mut values)
}

fn deriv_sample_from_series(series: &RangeSeries, range_end_ms: i64, range: Time) -> Option<f64> {
    let samples = float_range_samples(series, range_end_ms, range);
    regression_slope(&samples, range_end_ms)
}

fn float_range_samples(series: &RangeSeries, range_end_ms: i64, range: Time) -> Vec<(i64, f64)> {
    let range_start_ms = range_end_ms.saturating_sub(range.millis_i64());
    series
        .samples
        .iter()
        .filter_map(|(timestamp, value)| {
            if *timestamp <= range_start_ms || *timestamp > range_end_ms {
                return None;
            }
            let SampleValue::Float(value) = value else {
                return None;
            };
            Some((*timestamp, *value))
        })
        .collect()
}

fn histogram_range_samples(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
) -> Vec<NativeHistogram> {
    range_samples(series, range_end_ms, range)
        .filter_map(|(_, value)| match value {
            SampleValue::Histogram(histogram) => Some(histogram.clone()),
            SampleValue::Float(_) => None,
        })
        .collect()
}

pub(super) fn range_has_samples(series: &RangeSeries, range_end_ms: i64, range: Time) -> bool {
    range_sample_count(series, range_end_ms, range) != 0
}

fn range_sample_count(series: &RangeSeries, range_end_ms: i64, range: Time) -> usize {
    range_samples(series, range_end_ms, range).count()
}

fn range_samples(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
) -> impl Iterator<Item = (i64, &SampleValue)> {
    let range_start_ms = range_end_ms.saturating_sub(range.millis_i64());
    series
        .samples
        .iter()
        .filter(move |(timestamp, _)| *timestamp > range_start_ms && *timestamp <= range_end_ms)
        .map(|(timestamp, value)| (*timestamp, value))
}

/// Which extremum [`fold_over_time_extremum`] tracks.
#[derive(Clone, Copy)]
enum ExtremumKind {
    Min,
    Max,
}

impl ExtremumKind {
    /// Returns true if `candidate` should replace the running value `running`.
    ///
    /// The rule is Prometheus' NaN-ignoring float order. `AggregateState::push_float`
    /// and the `prom_min`/`prom_max` aggregate UDAF apply the same rule. This
    /// method always replaces a NaN running value. A NaN candidate never
    /// replaces a non-NaN running value, because `NaN > _` and `NaN < _` are
    /// both false.
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

/// Folds a non-empty sample window for `min_over_time` or `max_over_time`.
///
/// The fold ignores NaN. It seeds with the first sample, NaN included, then
/// replaces the running value under [`ExtremumKind::should_replace`]. The
/// result is NaN only when every sample is NaN. This matches Prometheus, the
/// `*_over_time` UDF, and the `min`/`max` aggregate.
fn fold_over_time_extremum(samples: &[(i64, f64)], extremum: ExtremumKind) -> f64 {
    let mut running = samples[0].1;
    for (_, candidate) in &samples[1..] {
        if extremum.should_replace(running, *candidate) {
            running = *candidate;
        }
    }
    running
}

/// Returns the population variance of a sample window.
///
/// The fold uses Welford's online algorithm with Kahan-compensated
/// accumulation, and matches Prometheus' `stdvar_over_time` and
/// `stddev_over_time`. The naive `E[x^2] - E[x]^2` form suffers catastrophic
/// cancellation for large-magnitude close-valued windows and gives a negative
/// variance whose `sqrt` is NaN. Welford stays numerically stable.
fn over_time_variance(samples: &[(i64, f64)]) -> f64 {
    let mut count = 0.0_f64;
    let (mut mean, mut mean_comp) = (0.0_f64, 0.0_f64);
    let (mut aux, mut aux_comp) = (0.0_f64, 0.0_f64);
    for (_, value) in samples {
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

/// Returns the arithmetic mean of a non-empty float window.
///
/// The fold uses Prometheus' incremental Kahan-compensated mean
/// (`avg_over_time` in `promql/engine.go`). The naive `sum / count` overflows
/// to ±Inf for very-large-magnitude windows. The incremental form keeps the
/// running mean finite. Once it does saturate to ±Inf, it keeps Prometheus'
/// same-sign-infinity handling.
fn over_time_mean(values: impl Iterator<Item = f64>) -> f64 {
    let mut count = 0.0_f64;
    let (mut mean, mut comp) = (0.0_f64, 0.0_f64);
    for value in values {
        count += 1.0;
        if mean.is_infinite() {
            if value.is_infinite() && (value > 0.0) == (mean > 0.0) {
                // Same-sign infinity: the mean stays that infinity.
                continue;
            }
            if !value.is_infinite() && !value.is_nan() {
                // A finite sample cannot pull an already-infinite mean back.
                continue;
            }
        }
        let (new_mean, new_comp) = kahan_sum_inc(value / count - mean / count, mean, comp);
        mean = new_mean;
        comp = new_comp;
    }
    mean + comp
}

/// Does one Kahan-compensated incremental sum step.
///
/// This function adds `increment` to the running sum `(sum, comp)` and returns
/// the updated `(sum, comp)`. It is a direct port of Prometheus' `kahanSumInc`
/// (`promql/engine.go`). The numerically stable mean and variance folds use it,
/// so the operator and interpreter agree bit-for-bit.
pub(super) fn kahan_sum_inc(increment: f64, sum: f64, comp: f64) -> (f64, f64) {
    let new_sum = sum + increment;
    // Recover the rounding error lost when `increment` is small relative to
    // `sum` (or vice versa), matching Prometheus' branch on magnitude.
    let new_comp = if sum.abs() >= increment.abs() {
        comp + ((sum - new_sum) + increment)
    } else {
        comp + ((increment - new_sum) + sum)
    };
    (new_sum, new_comp)
}

fn over_time_mad(samples: &[(i64, f64)]) -> Option<f64> {
    let mut values = samples.iter().map(|(_, value)| *value).collect::<Vec<_>>();
    let median = quantile_value(0.5, &mut values)?;
    let mut deviations = samples
        .iter()
        .map(|(_, value)| (value - median).abs())
        .collect::<Vec<_>>();
    quantile_value(0.5, &mut deviations)
}

fn predict_linear_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    duration: Time,
) -> Option<f64> {
    let samples = float_range_samples(series, range_end_ms, range);
    predict_linear(&samples, range_end_ms, duration)
}

#[cfg(feature = "experimental-functions")]
fn double_exponential_smoothing_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    smoothing_factor: f64,
    trend_factor: f64,
) -> Option<f64> {
    let samples = float_range_samples(series, range_end_ms, range);
    double_exponential_smoothing(&samples, smoothing_factor, trend_factor)
}

// Prometheus computes extrapolation in f64 seconds; timestamp/range deltas
// intentionally enter that float domain here.
fn extrapolated_rate(
    timestamps: &[i64],
    values: &[f64],
    range_start_ms: i64,
    range_end_ms: i64,
    range: Time,
    kind: RangeFn,
) -> Option<f64> {
    let n = timestamps.len();
    if n < 2 || values.len() != n {
        return None;
    }

    let is_counter = matches!(kind, RangeFn::Rate | RangeFn::Increase);

    let mut result = values[n - 1] - values[0];
    if is_counter {
        for window in values.windows(2) {
            if window[1] < window[0] {
                result += window[0];
            }
        }
    }

    let first_ts = timestamps[0];
    let last_ts = timestamps[n - 1];
    let sampled_interval = (last_ts - first_ts).to_f64()? / 1000.0;
    if sampled_interval <= 0.0 {
        return None;
    }

    let average_duration_between_samples = sampled_interval / (n - 1).to_f64()?;
    let extrapolation_threshold = average_duration_between_samples * 1.1;
    let mut duration_to_start = (first_ts - range_start_ms).to_f64()? / 1000.0;
    let mut duration_to_end = (range_end_ms - last_ts).to_f64()? / 1000.0;

    if duration_to_start >= extrapolation_threshold {
        duration_to_start = average_duration_between_samples / 2.0;
    }
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_duration_between_samples / 2.0;
    }

    if is_counter && result > 0.0 && values[0] >= 0.0 {
        let duration_to_zero = sampled_interval * (values[0] / result);
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }

    let extrapolate_to_interval = sampled_interval + duration_to_start + duration_to_end;
    result *= extrapolate_to_interval / sampled_interval;
    if kind == RangeFn::Rate {
        let range_seconds = range.secs_f64();
        if range_seconds <= 0.0 {
            return None;
        }
        result /= range_seconds;
    }
    Some(result)
}

fn count_changes(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    if values.len() < 2 {
        return Some(0.0);
    }

    let changes = values
        .windows(2)
        .filter(|window| window[0].to_bits() != window[1].to_bits())
        .fold(0.0, |count, _| count + 1.0);
    Some(changes)
}

fn count_resets(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    if values.len() < 2 {
        return Some(0.0);
    }

    let resets = values
        .windows(2)
        .filter(|window| window[1] < window[0])
        .fold(0.0, |count, _| count + 1.0);
    Some(resets)
}

pub(super) fn align_subquery_start(start_ms: i64, step: Time) -> i64 {
    let step_ms = step.millis_i64();
    let remainder = start_ms.rem_euclid(step_ms);
    if remainder == 0 {
        start_ms
    } else {
        start_ms.saturating_add(step_ms - remainder)
    }
}

// Prometheus predicts gauges from a simple linear regression in f64 seconds.
fn predict_linear(samples: &[(i64, f64)], range_end_ms: i64, duration: Time) -> Option<f64> {
    let (slope, intercept) = regression_slope_and_intercept(samples, range_end_ms)?;
    Some(intercept + (slope * duration.secs_f64()))
}

#[cfg(feature = "experimental-functions")]
pub(super) fn validate_smoothing_factor(name: &str, value: f64) -> Result<()> {
    if value <= 0.0 || value >= 1.0 {
        return Err(PromqlError::Plan(format!(
            "invalid {name}. Expected: 0 < factor < 1, got: {value}"
        )));
    }
    Ok(())
}

#[cfg(feature = "experimental-functions")]
fn double_exponential_smoothing(
    samples: &[(i64, f64)],
    smoothing_factor: f64,
    trend_factor: f64,
) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }

    let mut previous_smoothed = 0.0;
    let mut smoothed = samples[0].1;
    let mut trend = samples[1].1 - samples[0].1;

    for (index, (_, value)) in samples.iter().enumerate().skip(1) {
        if index != 1 {
            trend =
                trend_factor.mul_add(smoothed - previous_smoothed, (1.0 - trend_factor) * trend);
        }
        let scaled_value = smoothing_factor * value;
        let smoothed_with_trend = (1.0 - smoothing_factor) * (smoothed + trend);
        previous_smoothed = smoothed;
        smoothed = scaled_value + smoothed_with_trend;
    }

    Some(smoothed)
}

fn regression_slope(samples: &[(i64, f64)], range_end_ms: i64) -> Option<f64> {
    regression_slope_and_intercept(samples, range_end_ms).map(|(slope, _)| slope)
}

fn regression_slope_and_intercept(samples: &[(i64, f64)], range_end_ms: i64) -> Option<(f64, f64)> {
    if samples.len() < 2 {
        return None;
    }

    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut count = 0.0;
    for (timestamp, value) in samples {
        sum_x += (*timestamp - range_end_ms).to_f64()? / 1000.0;
        sum_y += value;
        count += 1.0;
    }
    let mean_x = sum_x / count;
    let mean_y = sum_y / count;

    let mut covariance = 0.0;
    let mut variance = 0.0;
    for (timestamp, value) in samples {
        let x = (*timestamp - range_end_ms).to_f64()? / 1000.0;
        let x_delta = x - mean_x;
        covariance += x_delta * (value - mean_y);
        variance += x_delta * x_delta;
    }
    if variance == 0.0 {
        return None;
    }

    let slope = covariance / variance;
    let intercept = mean_y - (slope * mean_x);
    Some((slope, intercept))
}

// Prometheus computes instant rate deltas in f64 seconds; timestamp deltas
// intentionally enter that float domain here.
fn instant_delta(timestamps: &[i64], values: &[f64], kind: IrateFn) -> Option<f64> {
    let n = timestamps.len();
    if n < 2 || values.len() != n {
        return None;
    }
    let previous = values[n - 2];
    let last = values[n - 1];
    let mut result = last - previous;
    if matches!(kind, IrateFn::Irate) && result < 0.0 {
        result = last;
    }

    if matches!(kind, IrateFn::Irate) {
        let interval = (timestamps[n - 1] - timestamps[n - 2]).to_f64()? / 1000.0;
        if interval <= 0.0 {
            return None;
        }
        result /= interval;
    }
    Some(result)
}
