use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};

use crabka_blockstore::{Labels, SeriesFingerprint};
use crabka_metrics::NativeHistogram;
use promql_parser::parser::LabelModifier;

use super::{MomentReduction, QueryShardReducer, RankReduction};
use crate::{
    PromqlError, QueryResult, RangeSeries, SampleValue, engine::add_compatible_native_histogram,
};

/// Merges range-matrix subquery results back into one Prometheus matrix.
///
/// This function is the query-frontend counterpart to
/// [`super::plan_range_query`]. It joins the time-split subqueries for the same
/// series. Sharded subqueries contribute distinct series.
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
