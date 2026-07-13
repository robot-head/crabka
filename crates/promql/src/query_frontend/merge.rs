use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};

use crabka_blockstore::{Labels, SeriesFingerprint};
use crabka_metrics::{BucketSpan, NativeHistogram};
use promql_parser::parser::LabelModifier;

use super::{MomentReduction, QueryShardReducer, RankReduction};
use crate::{PromqlError, QueryResult, RangeSeries, SampleValue};

/// Merge range-matrix subquery results back into one Prometheus matrix.
///
/// This is the query-frontend counterpart to [`super::plan_range_query`]: time-split
/// subqueries for the same series are stitched together, while sharded subqueries
/// naturally contribute distinct series.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn merge_range_query_results(results: Vec<QueryResult>) -> Result<QueryResult, PromqlError> {
    merge_range_query_results_with_reducer(results, QueryShardReducer::Sum)
}

pub(super) fn merge_range_query_results_with_reducer(
    results: Vec<QueryResult>,
    reducer: QueryShardReducer,
) -> Result<QueryResult, PromqlError> {
    let mut by_fp = BTreeMap::<SeriesFingerprint, RangeSeries>::new();

    for result in results {
        let QueryResult::RangeMatrix(series) = result else {
            return Err(PromqlError::Plan(
                "query-frontend range merge requires range matrix subquery results".into(),
            ));
        };
        for mut series in series {
            by_fp
                .entry(series.labels.fingerprint())
                .and_modify(|existing| existing.samples.append(&mut series.samples))
                .or_insert(series);
        }
    }

    let mut series = by_fp.into_values().collect::<Vec<_>>();
    series.sort_by_key(|series| label_sort_key(&series.labels));
    for series in &mut series {
        series.samples.sort_by_key(|(ts_ms, _)| *ts_ms);
        reduce_duplicate_step_samples(&mut series.samples, reducer)?;
    }
    Ok(QueryResult::RangeMatrix(series))
}

fn reduce_duplicate_step_samples(
    samples: &mut Vec<(i64, SampleValue)>,
    reducer: QueryShardReducer,
) -> Result<(), PromqlError> {
    let mut merged_samples = Vec::<(i64, SampleValue)>::with_capacity(samples.len());
    for (ts_ms, value) in samples.drain(..) {
        match merged_samples.last_mut() {
            Some((last_ts, SampleValue::Float(last_value))) if *last_ts == ts_ms => {
                if let SampleValue::Float(value) = value {
                    *last_value = match reducer {
                        QueryShardReducer::First => *last_value,
                        QueryShardReducer::Sum => *last_value + value,
                        QueryShardReducer::Min => last_value.min(value),
                        QueryShardReducer::Max => last_value.max(value),
                    };
                }
            }
            Some((last_ts, SampleValue::Histogram(last_value)))
                if *last_ts == ts_ms && reducer == QueryShardReducer::Sum =>
            {
                if let SampleValue::Histogram(value) = value {
                    add_compatible_native_histogram(last_value, &value)?;
                }
            }
            Some((last_ts, _)) if *last_ts == ts_ms => {}
            _ => merged_samples.push((ts_ms, value)),
        }
    }
    *samples = merged_samples;
    Ok(())
}

pub(super) fn divide_range_query_results(
    sums: QueryResult,
    counts: QueryResult,
) -> Result<QueryResult, PromqlError> {
    let QueryResult::RangeMatrix(sum_series) = sums else {
        return Err(PromqlError::Plan(
            "avg query-frontend sum merge requires range matrix results".into(),
        ));
    };
    let QueryResult::RangeMatrix(count_series) = counts else {
        return Err(PromqlError::Plan(
            "avg query-frontend count merge requires range matrix results".into(),
        ));
    };
    let counts_by_fp = count_series
        .into_iter()
        .map(|series| (series.labels.fingerprint(), series))
        .collect::<BTreeMap<_, _>>();
    let mut avg_series = Vec::new();

    for series in sum_series {
        let Some(count_series) = counts_by_fp.get(&series.labels.fingerprint()) else {
            continue;
        };
        let counts_by_ts = count_series
            .samples
            .iter()
            .filter_map(|(ts_ms, value)| match value {
                SampleValue::Float(value) => Some((*ts_ms, *value)),
                SampleValue::Histogram(_) => None,
            })
            .collect::<BTreeMap<_, _>>();
        let samples = series
            .samples
            .into_iter()
            .filter_map(|(ts_ms, value)| {
                let count = *counts_by_ts.get(&ts_ms)?;
                if count == 0.0 {
                    return None;
                }
                Some((
                    ts_ms,
                    match value {
                        SampleValue::Float(value) => SampleValue::Float(value / count),
                        SampleValue::Histogram(histogram) => {
                            SampleValue::Histogram(scaled_native_histogram(&histogram, 1.0 / count))
                        }
                    },
                ))
            })
            .collect::<Vec<_>>();
        if !samples.is_empty() {
            avg_series.push(RangeSeries {
                labels: series.labels,
                samples,
            });
        }
    }

    Ok(QueryResult::RangeMatrix(avg_series))
}

pub(super) fn reduce_moment_range_query_results(
    sums: QueryResult,
    counts: QueryResult,
    sum_squares: QueryResult,
    kind: MomentReduction,
) -> Result<QueryResult, PromqlError> {
    let QueryResult::RangeMatrix(sum_series) = sums else {
        return Err(PromqlError::Plan(
            "moment query-frontend sum merge requires range matrix results".into(),
        ));
    };
    let QueryResult::RangeMatrix(count_series) = counts else {
        return Err(PromqlError::Plan(
            "moment query-frontend count merge requires range matrix results".into(),
        ));
    };
    let QueryResult::RangeMatrix(sum_squares_series) = sum_squares else {
        return Err(PromqlError::Plan(
            "moment query-frontend sum-squares merge requires range matrix results".into(),
        ));
    };
    let counts_by_fp = float_samples_by_fingerprint(count_series);
    let sum_squares_by_fp = float_samples_by_fingerprint(sum_squares_series);
    let mut out_series = Vec::new();

    for series in sum_series {
        let fingerprint = series.labels.fingerprint();
        let Some(counts_by_ts) = counts_by_fp.get(&fingerprint) else {
            continue;
        };
        let Some(sum_squares_by_ts) = sum_squares_by_fp.get(&fingerprint) else {
            continue;
        };
        let samples = series
            .samples
            .into_iter()
            .filter_map(|(ts_ms, value)| {
                let SampleValue::Float(sum) = value else {
                    return None;
                };
                let count = *counts_by_ts.get(&ts_ms)?;
                let sum_squares = *sum_squares_by_ts.get(&ts_ms)?;
                if count == 0.0 {
                    return None;
                }
                let mean = sum / count;
                let variance = ((sum_squares / count) - (mean * mean)).max(0.0);
                let value = match kind {
                    MomentReduction::Stddev => variance.sqrt(),
                    MomentReduction::Stdvar => variance,
                };
                Some((ts_ms, SampleValue::Float(value)))
            })
            .collect::<Vec<_>>();
        if !samples.is_empty() {
            out_series.push(RangeSeries {
                labels: series.labels,
                samples,
            });
        }
    }

    Ok(QueryResult::RangeMatrix(out_series))
}

fn float_samples_by_fingerprint(
    series: Vec<RangeSeries>,
) -> BTreeMap<SeriesFingerprint, BTreeMap<i64, f64>> {
    series
        .into_iter()
        .map(|series| {
            (
                series.labels.fingerprint(),
                series
                    .samples
                    .into_iter()
                    .filter_map(|(ts_ms, value)| match value {
                        SampleValue::Float(value) => Some((ts_ms, value)),
                        SampleValue::Histogram(_) => None,
                    })
                    .collect::<BTreeMap<_, _>>(),
            )
        })
        .collect()
}

pub(super) fn reduce_rank_range_query_results(
    result: QueryResult,
    k: usize,
    kind: RankReduction,
    modifier: Option<&LabelModifier>,
) -> Result<QueryResult, PromqlError> {
    let QueryResult::RangeMatrix(mut series) = result else {
        return Err(PromqlError::Plan(
            "rank query-frontend merge requires range matrix results".into(),
        ));
    };
    if k == 0 {
        return Ok(QueryResult::RangeMatrix(Vec::new()));
    }

    let mut keep = BTreeSet::<(SeriesFingerprint, usize)>::new();
    let mut candidates_by_step_and_group = BTreeMap::<(i64, String), Vec<RankCandidate>>::new();
    for (series_index, series) in series.iter().enumerate() {
        let group = label_sort_key(&aggregate_labels(&series.labels, modifier));
        let labels_key = label_sort_key(&series.labels);
        let fingerprint = series.labels.fingerprint();
        for (sample_index, (ts_ms, value)) in series.samples.iter().enumerate() {
            if let SampleValue::Float(value) = value {
                candidates_by_step_and_group
                    .entry((*ts_ms, group.clone()))
                    .or_default()
                    .push(RankCandidate {
                        fingerprint,
                        labels_key: labels_key.clone(),
                        sample_index,
                        series_index,
                        value: *value,
                    });
            }
        }
    }

    for mut candidates in candidates_by_step_and_group.into_values() {
        candidates.sort_by(|left, right| compare_rank_candidates(kind, left, right));
        candidates.truncate(k.min(candidates.len()));
        keep.extend(
            candidates
                .into_iter()
                .map(|candidate| (candidate.fingerprint, candidate.sample_index)),
        );
    }

    for series in &mut series {
        let fingerprint = series.labels.fingerprint();
        let mut sample_index = 0_usize;
        series.samples.retain(|_| {
            let keep_sample = keep.contains(&(fingerprint, sample_index));
            sample_index += 1;
            keep_sample
        });
    }
    series.retain(|series| !series.samples.is_empty());
    series.sort_by_key(|series| label_sort_key(&series.labels));
    Ok(QueryResult::RangeMatrix(series))
}

#[derive(Clone)]
struct RankCandidate {
    fingerprint: SeriesFingerprint,
    labels_key: String,
    sample_index: usize,
    series_index: usize,
    value: f64,
}

fn compare_rank_candidates(
    kind: RankReduction,
    left: &RankCandidate,
    right: &RankCandidate,
) -> std::cmp::Ordering {
    let by_value = match kind {
        RankReduction::Top => right.value.total_cmp(&left.value),
        RankReduction::Bottom => left.value.total_cmp(&right.value),
    };
    by_value
        .then_with(|| left.labels_key.cmp(&right.labels_key))
        .then_with(|| left.series_index.cmp(&right.series_index))
        .then_with(|| left.sample_index.cmp(&right.sample_index))
}

fn aggregate_labels(input: &Labels, modifier: Option<&LabelModifier>) -> Labels {
    let mut labels = Labels::new();
    match modifier {
        Some(LabelModifier::Include(include)) => {
            for name in &include.labels {
                if name == "__name__" {
                    continue;
                }
                if let Some(value) = input.get(name) {
                    labels.insert(name, value);
                }
            }
        }
        Some(LabelModifier::Exclude(exclude)) => {
            let excluded = exclude.labels.iter().collect::<BTreeSet<_>>();
            for (name, value) in input.iter() {
                if name == "__name__" || excluded.contains(name) {
                    continue;
                }
                labels.insert(name, value);
            }
        }
        None => {}
    }
    labels
}

fn add_compatible_native_histogram(
    left: &mut NativeHistogram,
    right: &NativeHistogram,
) -> Result<(), PromqlError> {
    if !native_histograms_are_compatible(left, right) {
        return Err(PromqlError::Unsupported(
            "incompatible native histogram query-frontend merge is not implemented yet".to_string(),
        ));
    }

    left.zero_count += right.zero_count;
    left.count += right.count;
    left.sum += right.sum;
    (left.positive_spans, left.positive_counts) = add_spanned_histogram_counts(
        &left.positive_spans,
        &left.positive_counts,
        &right.positive_spans,
        &right.positive_counts,
    );
    (left.negative_spans, left.negative_counts) = add_spanned_histogram_counts(
        &left.negative_spans,
        &left.negative_counts,
        &right.negative_spans,
        &right.negative_counts,
    );
    left.start_timestamp_ms = match (left.start_timestamp_ms, right.start_timestamp_ms) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    };
    Ok(())
}

fn native_histograms_are_compatible(left: &NativeHistogram, right: &NativeHistogram) -> bool {
    left.schema == right.schema
        && left.is_float == right.is_float
        && left.reset_hint == right.reset_hint
        && left.zero_threshold.to_bits() == right.zero_threshold.to_bits()
        && left.custom_values == right.custom_values
}

fn add_spanned_histogram_counts(
    left_spans: &[BucketSpan],
    left_counts: &[f64],
    right_spans: &[BucketSpan],
    right_counts: &[f64],
) -> (Vec<BucketSpan>, Vec<f64>) {
    let mut buckets = spanned_histogram_counts(left_spans, left_counts);
    for (index, count) in spanned_histogram_counts(right_spans, right_counts) {
        *buckets.entry(index).or_insert(0.0) += count;
    }
    compact_spanned_histogram_counts(buckets)
}

fn spanned_histogram_counts(spans: &[BucketSpan], counts: &[f64]) -> BTreeMap<i32, f64> {
    let mut buckets = BTreeMap::new();
    let mut index = 0_i32;
    let mut count_index = 0_usize;
    for (span_index, span) in spans.iter().enumerate() {
        if span_index == 0 {
            index = span.offset;
        } else {
            index += span.offset;
        }
        for _ in 0..span.length {
            let Some(count) = counts.get(count_index).copied() else {
                return buckets;
            };
            buckets.insert(index, count);
            index += 1;
            count_index += 1;
        }
    }
    buckets
}

fn compact_spanned_histogram_counts(buckets: BTreeMap<i32, f64>) -> (Vec<BucketSpan>, Vec<f64>) {
    let buckets = buckets
        .into_iter()
        .filter(|(_, count)| *count != 0.0)
        .collect::<Vec<_>>();
    let mut spans = Vec::new();
    let mut counts = Vec::with_capacity(buckets.len());
    let mut span_start = None;
    let mut previous_index = 0_i32;
    let mut previous_span_end = 0_i32;
    for (index, count) in buckets {
        if span_start.is_none() {
            span_start = Some(index);
        } else if index != previous_index + 1 {
            let start = span_start.expect("checked is_some");
            spans.push(BucketSpan {
                offset: start - previous_span_end,
                length: u32::try_from(previous_index - start + 1).unwrap_or(u32::MAX),
            });
            previous_span_end = previous_index + 1;
            span_start = Some(index);
        }
        counts.push(count);
        previous_index = index;
    }
    if let Some(start) = span_start {
        spans.push(BucketSpan {
            offset: start - previous_span_end,
            length: u32::try_from(previous_index - start + 1).unwrap_or(u32::MAX),
        });
    }
    (spans, counts)
}

fn scaled_native_histogram(histogram: &NativeHistogram, factor: f64) -> NativeHistogram {
    let mut out = histogram.clone();
    out.zero_count *= factor;
    out.count *= factor;
    out.sum *= factor;
    for count in &mut out.positive_counts {
        *count *= factor;
    }
    for count in &mut out.negative_counts {
        *count *= factor;
    }
    out
}

fn label_sort_key(labels: &Labels) -> String {
    labels.iter().fold(String::new(), |mut out, (name, value)| {
        let _ = writeln!(out, "{name}={value}");
        out
    })
}
