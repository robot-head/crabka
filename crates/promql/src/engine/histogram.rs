use std::collections::{BTreeMap, BTreeSet};

use crabka_blockstore::Labels;
use crabka_metrics::{BucketSpan, NativeHistogram, ResetHint};

use super::{
    annotations::warn_mixed_histograms,
    labels::{
        float_sample_value, labels_key, labels_without_metric_and_label,
        labels_without_metric_name, record_metric_name,
    },
};
use crate::{
    error::{PromqlError, Result},
    result::{InstantSample, SampleValue},
};

#[derive(Clone, Copy, Debug)]
struct ClassicBucket {
    upper_bound: f64,
    count: f64,
}

fn parse_classic_bucket_bound(value: &str) -> Result<f64> {
    match value {
        "+Inf" | "Inf" => Ok(f64::INFINITY),
        "-Inf" => Ok(f64::NEG_INFINITY),
        _ => value.parse::<f64>().map_err(|error| {
            PromqlError::Plan(format!(
                "invalid classic histogram bucket `{value}`: {error}"
            ))
        }),
    }
}

/// Shared `histogram_quantile(phi, v)` core over an already-evaluated instant
/// vector.
///
/// Backs both the interpreter (`PromqlEngine::eval_histogram_quantile_call`)
/// and the recursive operator path (a `PlannedInstant::Precomputed` result), so
/// the two are identical by construction. Native-histogram samples are reduced
/// via [`native_histogram_quantile`]; classic `<metric>_bucket{le}` float series
/// are grouped by their labels (excluding `le`), folded by
/// [`classic_histogram_quantile`] (which forces bucket monotonicity, parses each
/// `le` bound incl. `+Inf`, handles `<2`-bucket / `phi` out of `[0, 1]` / the
/// negative-first-bucket lower bound, and linearly interpolates). A series whose
/// labelset (sans `le`) appears as both a native histogram and a classic bucket
/// group is dropped from the output with a mixed-schema warning, matching
/// Apply a native-histogram accessor (`histogram_count` / `sum` / `avg` /
/// `stddev` / `stdvar`) to an instant vector, mirroring
/// `PromqlEngine::eval_histogram_accessor_call` exactly.
///
/// Only `SampleValue::Histogram` rows are kept (a float row carries no histogram
/// to read, so it is dropped); each surviving row keeps its source timestamp,
/// drops `__name__`, and carries the scalar accessor value. Shared by the
/// interpreter and the operator path so the two are parity-exact.
pub(super) fn apply_histogram_accessor(
    samples: Vec<InstantSample>,
    accessor: HistogramAccessor,
) -> Vec<InstantSample> {
    samples
        .into_iter()
        .filter_map(|sample| {
            let SampleValue::Histogram(hist) = sample.value else {
                return None;
            };
            Some(InstantSample {
                labels: labels_without_metric_name(&sample.labels),
                ts_ms: sample.ts_ms,
                value: SampleValue::Float(accessor.value(&hist)),
            })
        })
        .collect()
}

/// Applies `histogram_fraction(lower, upper, v)` to an instant vector `v`.
///
/// This function mirrors `PromqlEngine::eval_histogram_fraction_call` exactly.
/// Native-histogram rows fold through [`native_histogram_fraction`] and keep the
/// source timestamp. This function groups classic `<metric>_bucket{le}` float
/// rows by labelset and drops `__name__` and `le` from the group. Each group
/// then folds through [`classic_histogram_fraction`] and carries `time_ms`.
///
/// This function drops a labelset that carries both a classic and a native
/// histogram from the output. It raises the
/// `MixedClassicNativeHistogramsWarning` through the in-scope annotation sink,
/// exactly as the interpreter does. The interpreter and the operator path share
/// this function, so the two are parity-exact.
///
/// # Errors
///
/// Returns [`PromqlError`] for an unparseable `le` bound. Returns
/// [`PromqlError`] for a non-float classic bucket count. These are exactly the
/// errors the interpreter raised inline.
pub(super) fn apply_histogram_fraction(
    lower: f64,
    upper: f64,
    samples: Vec<InstantSample>,
    time_ms: i64,
) -> Result<Vec<InstantSample>> {
    let mut native_samples = BTreeMap::new();
    let mut groups: BTreeMap<String, (Labels, Vec<ClassicBucket>)> = BTreeMap::new();
    let mut metric_names: BTreeMap<String, String> = BTreeMap::new();
    for sample in samples {
        if let SampleValue::Histogram(hist) = sample.value {
            let labels = labels_without_metric_name(&sample.labels);
            let key = labels_key(&labels);
            record_metric_name(&mut metric_names, &key, &sample.labels);
            native_samples.insert(
                key,
                InstantSample {
                    labels,
                    ts_ms: sample.ts_ms,
                    value: SampleValue::Float(native_histogram_fraction(lower, upper, &hist)),
                },
            );
            continue;
        }
        let Some(le) = sample.labels.get("le") else {
            continue;
        };
        let upper_bound = parse_classic_bucket_bound(le)?;
        let count = float_sample_value(&sample)?;
        let labels = labels_without_metric_and_label(&sample.labels, "le");
        let key = labels_key(&labels);
        record_metric_name(&mut metric_names, &key, &sample.labels);
        groups
            .entry(key)
            .or_insert_with(|| (labels, Vec::new()))
            .1
            .push(ClassicBucket { upper_bound, count });
    }

    let mixed_histogram_keys = native_samples
        .keys()
        .filter(|key| groups.contains_key(*key))
        .cloned()
        .collect::<BTreeSet<_>>();
    warn_mixed_histograms(&mixed_histogram_keys, &metric_names);
    let mut out = native_samples
        .into_iter()
        .filter_map(|(key, sample)| (!mixed_histogram_keys.contains(&key)).then_some(sample))
        .collect::<Vec<_>>();
    out.extend(
        groups
            .into_iter()
            .filter_map(|(key, (labels, mut buckets))| {
                (!mixed_histogram_keys.contains(&key)).then_some(InstantSample {
                    labels,
                    ts_ms: time_ms,
                    value: SampleValue::Float(classic_histogram_fraction(
                        lower,
                        upper,
                        &mut buckets,
                    )),
                })
            }),
    );
    Ok(out)
}

/// Prometheus. Both the `__name__` and `le` labels are dropped from every output
/// series. Classic output samples carry `time_ms`; native ones keep the source
/// sample timestamp.
///
/// # Errors
///
/// Returns [`PromqlError`] for an unparseable `le` bound. Returns
/// [`PromqlError`] for a non-float classic bucket count. These are exactly the
/// errors the interpreter raised inline.
pub(super) fn apply_histogram_quantile(
    quantile: f64,
    samples: Vec<InstantSample>,
    time_ms: i64,
) -> Result<Vec<InstantSample>> {
    let mut groups: BTreeMap<String, (Labels, Vec<ClassicBucket>)> = BTreeMap::new();
    let mut native_samples = BTreeMap::new();
    let mut metric_names: BTreeMap<String, String> = BTreeMap::new();
    for sample in samples {
        if let SampleValue::Histogram(histogram) = &sample.value {
            let labels = labels_without_metric_name(&sample.labels);
            let key = labels_key(&labels);
            record_metric_name(&mut metric_names, &key, &sample.labels);
            native_samples.insert(
                key,
                InstantSample {
                    labels,
                    ts_ms: sample.ts_ms,
                    value: SampleValue::Float(native_histogram_quantile(quantile, histogram)),
                },
            );
            continue;
        }
        let Some(le) = sample.labels.get("le") else {
            continue;
        };
        let upper_bound = parse_classic_bucket_bound(le)?;
        let count = float_sample_value(&sample)?;
        let labels = labels_without_metric_and_label(&sample.labels, "le");
        let key = labels_key(&labels);
        record_metric_name(&mut metric_names, &key, &sample.labels);
        groups
            .entry(key)
            .or_insert_with(|| (labels, Vec::new()))
            .1
            .push(ClassicBucket { upper_bound, count });
    }

    let mixed_histogram_keys = native_samples
        .keys()
        .filter(|key| groups.contains_key(*key))
        .cloned()
        .collect::<BTreeSet<_>>();
    warn_mixed_histograms(&mixed_histogram_keys, &metric_names);
    let mut out = native_samples
        .into_iter()
        .filter_map(|(key, sample)| (!mixed_histogram_keys.contains(&key)).then_some(sample))
        .collect::<Vec<_>>();
    out.extend(
        groups
            .into_iter()
            .filter_map(|(key, (labels, mut buckets))| {
                (!mixed_histogram_keys.contains(&key)).then_some(InstantSample {
                    labels,
                    ts_ms: time_ms,
                    value: SampleValue::Float(classic_histogram_quantile(quantile, &mut buckets)),
                })
            }),
    );
    Ok(out)
}

/// Applies the experimental `histogram_quantiles(label, v, phi...)` fold.
///
/// The input is an already-evaluated instant vector. This function emits one
/// output series for each `(input series, quantile)` pair and writes the
/// quantile into the label that `label` names.
///
/// The interpreter method `PromqlEngine::eval_histogram_quantiles_call` and the
/// operator-path `histogram_quantiles` dispatch share this function, so the two
/// match Prometheus by construction. This holds for classic
/// `<metric>_bucket{le}` float-bucket vectors and for native-histogram vectors.
/// This function skips a mixed classic and native key silently, with no
/// annotation, unlike `histogram_quantile`, and so matches the interpreter's
/// `histogram_quantiles` behaviour. Classic output samples carry `time_ms`, and
/// native output samples keep the source sample timestamp. Both drop `__name__`,
/// and classic buckets also drop `le`.
///
/// # Errors
///
/// Returns [`PromqlError`] for an unparseable `le` bound. Returns
/// [`PromqlError`] for a non-float classic bucket count. These are exactly the
/// errors the interpreter raised inline.
#[cfg(feature = "experimental-functions")]
pub(super) fn apply_histogram_quantiles(
    samples: Vec<InstantSample>,
    label_name: &str,
    quantiles: &[f64],
    time_ms: i64,
) -> Result<Vec<InstantSample>> {
    let mut groups: BTreeMap<String, (Labels, Vec<ClassicBucket>)> = BTreeMap::new();
    let mut native_samples = BTreeMap::new();
    for sample in samples {
        if let SampleValue::Histogram(histogram) = &sample.value {
            let labels = labels_without_metric_name(&sample.labels);
            native_samples.insert(
                labels_key(&labels),
                (labels, sample.ts_ms, histogram.clone()),
            );
            continue;
        }
        let Some(le) = sample.labels.get("le") else {
            continue;
        };
        let upper_bound = parse_classic_bucket_bound(le)?;
        let count = float_sample_value(&sample)?;
        let labels = labels_without_metric_and_label(&sample.labels, "le");
        groups
            .entry(labels_key(&labels))
            .or_insert_with(|| (labels, Vec::new()))
            .1
            .push(ClassicBucket { upper_bound, count });
    }

    let mixed_histogram_keys = native_samples
        .keys()
        .filter(|key| groups.contains_key(*key))
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut out = Vec::new();
    for (key, (labels, ts_ms, histogram)) in native_samples {
        if mixed_histogram_keys.contains(&key) {
            continue;
        }
        out.extend(quantiles.iter().map(|quantile| {
            let mut labels = labels.clone();
            labels.insert(label_name, quantile.to_string());
            InstantSample {
                labels,
                ts_ms,
                value: SampleValue::Float(native_histogram_quantile(*quantile, &histogram)),
            }
        }));
    }
    for (key, (labels, buckets)) in groups {
        if mixed_histogram_keys.contains(&key) {
            continue;
        }
        out.extend(quantiles.iter().map(|quantile| {
            let mut labels = labels.clone();
            let mut buckets = buckets.clone();
            labels.insert(label_name, quantile.to_string());
            InstantSample {
                labels,
                ts_ms: time_ms,
                value: SampleValue::Float(classic_histogram_quantile(*quantile, &mut buckets)),
            }
        }));
    }
    Ok(out)
}

fn classic_histogram_quantile(quantile: f64, buckets: &mut [ClassicBucket]) -> f64 {
    if quantile.is_nan() {
        return f64::NAN;
    }
    if quantile < 0.0 {
        return f64::NEG_INFINITY;
    }
    if quantile > 1.0 {
        return f64::INFINITY;
    }

    let buckets = normalized_classic_histogram_buckets(buckets);
    if buckets.len() < 2
        || !buckets.last().is_some_and(|bucket| {
            bucket.upper_bound.is_infinite() && bucket.upper_bound.is_sign_positive()
        })
    {
        return f64::NAN;
    }

    let total = buckets.last().map_or(0.0, |bucket| bucket.count);
    if total <= 0.0 || total.is_nan() {
        return f64::NAN;
    }
    let rank = quantile * total;
    let bucket_index = buckets
        .iter()
        .position(|bucket| bucket.count >= rank)
        .unwrap_or(buckets.len() - 1);

    if bucket_index == buckets.len() - 1 {
        return buckets[bucket_index - 1].upper_bound;
    }

    let bucket = buckets[bucket_index];
    let (lower_bound, previous_count) = if bucket_index == 0 {
        if bucket.upper_bound <= 0.0 {
            return bucket.upper_bound;
        }
        (0.0, 0.0)
    } else {
        let previous = buckets[bucket_index - 1];
        (previous.upper_bound, previous.count)
    };

    let bucket_count = bucket.count - previous_count;
    if bucket_count <= 0.0 {
        return bucket.upper_bound;
    }
    lower_bound + (bucket.upper_bound - lower_bound) * ((rank - previous_count) / bucket_count)
}

fn native_histogram_quantile(quantile: f64, hist: &NativeHistogram) -> f64 {
    if quantile.is_nan() {
        return f64::NAN;
    }
    if quantile < 0.0 {
        return f64::NEG_INFINITY;
    }
    if quantile > 1.0 {
        return f64::INFINITY;
    }
    if hist.count <= 0.0 || hist.count.is_nan() {
        return f64::NAN;
    }

    let mut buckets = native_histogram_buckets(hist);
    buckets.sort_by(|left, right| left.lower.total_cmp(&right.lower));
    let rank = quantile * hist.count;
    let mut cumulative = 0.0;
    for bucket in buckets {
        let previous = cumulative;
        cumulative += bucket.count;
        if cumulative < rank {
            continue;
        }
        if bucket.count <= 0.0 {
            return bucket.upper;
        }
        if bucket.lower.is_infinite() && bucket.lower.is_sign_negative() {
            return bucket.upper;
        }
        if bucket.upper.is_infinite() && bucket.upper.is_sign_positive() {
            return bucket.lower;
        }
        return native_histogram_bucket_quantile(hist, bucket, (rank - previous) / bucket.count);
    }
    f64::NAN
}

fn native_histogram_fraction(lower: f64, upper: f64, hist: &NativeHistogram) -> f64 {
    if lower.is_nan() || upper.is_nan() || hist.count <= 0.0 || hist.count.is_nan() {
        return f64::NAN;
    }
    if lower >= upper {
        return 0.0;
    }

    let in_range = native_histogram_buckets(hist)
        .into_iter()
        .map(|bucket| bucket.count * bucket_overlap_fraction(bucket, lower, upper))
        .sum::<f64>();
    in_range / hist.count
}

fn classic_histogram_fraction(lower: f64, upper: f64, buckets: &mut [ClassicBucket]) -> f64 {
    if lower.is_nan() || upper.is_nan() {
        return f64::NAN;
    }
    if lower >= upper {
        return 0.0;
    }

    let buckets = normalized_classic_histogram_buckets(buckets);
    if !buckets.last().is_some_and(|bucket| {
        bucket.upper_bound.is_infinite() && bucket.upper_bound.is_sign_positive()
    }) {
        return f64::NAN;
    }

    let total = buckets.last().map_or(0.0, |bucket| bucket.count);
    if total <= 0.0 || total.is_nan() {
        return f64::NAN;
    }

    classic_histogram_buckets(&buckets)
        .into_iter()
        .map(|bucket| bucket.count * bucket_overlap_fraction(bucket, lower, upper))
        .sum::<f64>()
        / total
}

fn normalized_classic_histogram_buckets(buckets: &mut [ClassicBucket]) -> Vec<ClassicBucket> {
    buckets.sort_by(|left, right| left.upper_bound.total_cmp(&right.upper_bound));

    let mut out: Vec<ClassicBucket> = Vec::with_capacity(buckets.len());
    for bucket in buckets.iter().copied() {
        if let Some(previous) = out.last_mut()
            && previous.upper_bound.total_cmp(&bucket.upper_bound).is_eq()
        {
            previous.count += bucket.count;
            continue;
        }
        out.push(bucket);
    }

    let mut max_count = 0.0_f64;
    for bucket in &mut out {
        max_count = max_count.max(bucket.count);
        bucket.count = max_count;
    }
    out
}

fn classic_histogram_buckets(buckets: &[ClassicBucket]) -> Vec<NativeQuantileBucket> {
    let mut out = Vec::with_capacity(buckets.len());
    let mut lower = if buckets
        .first()
        .is_some_and(|bucket| bucket.upper_bound <= 0.0)
    {
        f64::NEG_INFINITY
    } else {
        0.0
    };
    let mut previous_count = 0.0;
    for bucket in buckets {
        let count = bucket.count - previous_count;
        previous_count = bucket.count;
        out.push(NativeQuantileBucket {
            lower,
            upper: bucket.upper_bound,
            count,
        });
        lower = bucket.upper_bound;
    }
    out
}

fn native_histogram_stdvar(hist: &NativeHistogram) -> f64 {
    if hist.count <= 0.0 || hist.count.is_nan() {
        return f64::NAN;
    }

    let mean = hist.sum / hist.count;
    native_histogram_buckets(hist)
        .into_iter()
        .map(|bucket| {
            let bucket_mean = native_histogram_bucket_mean(hist, bucket);
            bucket.count * (bucket_mean - mean).powi(2)
        })
        .sum::<f64>()
        / hist.count
}

pub(crate) fn add_compatible_native_histogram(
    left: &mut NativeHistogram,
    right: &NativeHistogram,
) -> Result<()> {
    if left.is_nhcb() != right.is_nhcb() {
        return Err(PromqlError::Unsupported(
            "cannot combine exponential and custom-bucket native histograms".to_string(),
        ));
    }

    left.reset_hint = combined_reset_hint(left.reset_hint, right.reset_hint);
    left.is_float |= right.is_float;
    left.count += right.count;
    left.sum += right.sum;
    if left.is_nhcb() {
        add_custom_histogram(left, right);
    } else {
        add_exponential_histogram(left, right);
    }
    left.start_timestamp_ms = match (left.start_timestamp_ms, right.start_timestamp_ms) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    };
    Ok(())
}

fn combined_reset_hint(left: ResetHint, right: ResetHint) -> ResetHint {
    match (left, right) {
        (left, right) if left == right => left,
        (ResetHint::Gauge, _) | (_, ResetHint::Gauge) => ResetHint::Gauge,
        _ => ResetHint::Unknown,
    }
}

fn add_exponential_histogram(left: &mut NativeHistogram, right: &NativeHistogram) {
    let mut threshold = left.zero_threshold.max(right.zero_threshold);
    let (left_zero_count, right_zero_count) = loop {
        let (left_count, left_threshold) = zero_count_at_threshold(left, threshold);
        let (right_count, right_threshold) = zero_count_at_threshold(right, threshold);
        let reconciled = left_threshold.max(right_threshold);
        if reconciled.to_bits() == threshold.to_bits() {
            break (left_count, right_count);
        }
        threshold = reconciled;
    };

    let target_schema = left.schema.min(right.schema);
    let left_positive = reduced_counts_outside_zero(
        left,
        &left.positive_spans,
        &left.positive_counts,
        threshold,
        target_schema,
    );
    let right_positive = reduced_counts_outside_zero(
        right,
        &right.positive_spans,
        &right.positive_counts,
        threshold,
        target_schema,
    );
    let left_negative = reduced_counts_outside_zero(
        left,
        &left.negative_spans,
        &left.negative_counts,
        threshold,
        target_schema,
    );
    let right_negative = reduced_counts_outside_zero(
        right,
        &right.negative_spans,
        &right.negative_counts,
        threshold,
        target_schema,
    );

    left.schema = target_schema;
    left.zero_threshold = threshold;
    left.zero_count = left_zero_count + right_zero_count;
    (left.positive_spans, left.positive_counts) = add_bucket_maps(left_positive, right_positive);
    (left.negative_spans, left.negative_counts) = add_bucket_maps(left_negative, right_negative);
    left.custom_values = None;
}

fn zero_count_at_threshold(histogram: &NativeHistogram, mut threshold: f64) -> (f64, f64) {
    loop {
        let mut count = histogram.zero_count;
        let mut expanded = threshold;
        for buckets in [
            spanned_histogram_counts(&histogram.positive_spans, &histogram.positive_counts),
            spanned_histogram_counts(&histogram.negative_spans, &histogram.negative_counts),
        ] {
            for (index, bucket_count) in buckets {
                let lower = standard_histogram_bound(index - 1, histogram.schema);
                if lower >= threshold {
                    break;
                }
                count += bucket_count;
                let upper = standard_histogram_bound(index, histogram.schema);
                if bucket_count != 0.0 && upper > expanded {
                    expanded = upper;
                }
            }
        }
        if expanded.to_bits() == threshold.to_bits() {
            return (count, threshold);
        }
        threshold = expanded;
    }
}

fn reduced_counts_outside_zero(
    histogram: &NativeHistogram,
    spans: &[BucketSpan],
    counts: &[f64],
    zero_threshold: f64,
    target_schema: i8,
) -> BTreeMap<i32, f64> {
    let schema_delta =
        u32::try_from(i16::from(histogram.schema) - i16::from(target_schema)).unwrap_or_default();
    let divisor = 1_i32.checked_shl(schema_delta).unwrap_or(i32::MAX);
    let mut out = BTreeMap::new();
    for (index, count) in spanned_histogram_counts(spans, counts) {
        if standard_histogram_bound(index - 1, histogram.schema) < zero_threshold {
            continue;
        }
        let quotient = index.div_euclid(divisor);
        let target_index = quotient + i32::from(index.rem_euclid(divisor) != 0);
        *out.entry(target_index).or_insert(0.0) += count;
    }
    out
}

fn add_bucket_maps(
    mut left: BTreeMap<i32, f64>,
    right: BTreeMap<i32, f64>,
) -> (Vec<BucketSpan>, Vec<f64>) {
    for (index, count) in right {
        *left.entry(index).or_insert(0.0) += count;
    }
    compact_spanned_histogram_counts(left)
}

fn add_custom_histogram(left: &mut NativeHistogram, right: &NativeHistogram) {
    let left_values = left.custom_values.as_deref().unwrap_or_default();
    let right_values = right.custom_values.as_deref().unwrap_or_default();
    let target_values = left_values
        .iter()
        .copied()
        .filter(|value| right_values.contains(value))
        .collect::<Vec<_>>();
    let left_counts = remap_custom_counts(
        &left.positive_spans,
        &left.positive_counts,
        left_values,
        &target_values,
    );
    let right_counts = remap_custom_counts(
        &right.positive_spans,
        &right.positive_counts,
        right_values,
        &target_values,
    );
    (left.positive_spans, left.positive_counts) = add_bucket_maps(left_counts, right_counts);
    left.custom_values = Some(target_values);
    left.zero_threshold = 0.0;
    left.zero_count = 0.0;
    left.negative_spans.clear();
    left.negative_counts.clear();
}

fn remap_custom_counts(
    spans: &[BucketSpan],
    counts: &[f64],
    source_values: &[f64],
    target_values: &[f64],
) -> BTreeMap<i32, f64> {
    let mut out = BTreeMap::new();
    for (index, count) in spanned_histogram_counts(spans, counts) {
        let upper = custom_histogram_bound(index, source_values);
        let target_index = target_values
            .iter()
            .position(|bound| bound.total_cmp(&upper).is_ge())
            .unwrap_or(target_values.len());
        let target_index = i32::try_from(target_index).unwrap_or(i32::MAX);
        *out.entry(target_index).or_insert(0.0) += count;
    }
    out
}

pub(super) fn native_histograms_are_range_compatible(
    left: &NativeHistogram,
    right: &NativeHistogram,
) -> bool {
    left.schema == right.schema
        && left.is_float == right.is_float
        && left.zero_threshold.to_bits() == right.zero_threshold.to_bits()
        && left.custom_values == right.custom_values
        && left.positive_spans == right.positive_spans
        && left.negative_spans == right.negative_spans
        && left.positive_counts.len() == right.positive_counts.len()
        && left.negative_counts.len() == right.negative_counts.len()
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

pub(super) fn scaled_native_histogram(histogram: &NativeHistogram, factor: f64) -> NativeHistogram {
    let mut out = histogram.clone();
    scale_native_histogram_values(&mut out, factor);
    if factor.is_sign_negative() {
        out.reset_hint = ResetHint::Gauge;
    }
    out
}

pub(super) fn scale_native_histogram_values(histogram: &mut NativeHistogram, factor: f64) {
    histogram.zero_count *= factor;
    histogram.count *= factor;
    histogram.sum *= factor;
    for count in &mut histogram.positive_counts {
        *count *= factor;
    }
    for count in &mut histogram.negative_counts {
        *count *= factor;
    }
}

fn native_histogram_bucket_mean(hist: &NativeHistogram, bucket: NativeQuantileBucket) -> f64 {
    if bucket.lower.is_infinite() && bucket.lower.is_sign_negative() {
        return bucket.upper;
    }
    if bucket.upper.is_infinite() && bucket.upper.is_sign_positive() {
        return bucket.lower;
    }
    if hist.is_nhcb() || (bucket.lower <= 0.0 && bucket.upper >= 0.0) {
        return f64::midpoint(bucket.lower, bucket.upper);
    }
    if bucket.upper <= 0.0 {
        return -(bucket.lower * bucket.upper).sqrt();
    }
    (bucket.lower * bucket.upper).sqrt()
}

fn native_histogram_bucket_quantile(
    hist: &NativeHistogram,
    bucket: NativeQuantileBucket,
    fraction: f64,
) -> f64 {
    if hist.is_nhcb() || (bucket.lower <= 0.0 && bucket.upper >= 0.0) {
        return bucket.lower + (bucket.upper - bucket.lower) * fraction;
    }
    if bucket.upper <= 0.0 {
        return -(bucket.lower.abs() * (bucket.upper.abs() / bucket.lower.abs()).powf(fraction));
    }
    bucket.lower * (bucket.upper / bucket.lower).powf(fraction)
}

fn bucket_overlap_fraction(bucket: NativeQuantileBucket, lower: f64, upper: f64) -> f64 {
    if bucket.count == 0.0 || bucket.upper <= lower || bucket.lower >= upper {
        return 0.0;
    }
    let overlap_lower = bucket.lower.max(lower);
    let overlap_upper = bucket.upper.min(upper);
    if overlap_lower >= overlap_upper {
        return 0.0;
    }
    if bucket.lower.is_infinite() || bucket.upper.is_infinite() {
        if bucket.lower.is_infinite() && bucket.lower.is_sign_negative() {
            return f64::from(lower.is_infinite() && lower.is_sign_negative());
        }
        if bucket.upper.is_infinite() && bucket.upper.is_sign_positive() {
            return f64::from(upper.is_infinite() && upper.is_sign_positive());
        }
        let covers_left = if bucket.lower.is_infinite() && bucket.lower.is_sign_negative() {
            lower.is_infinite() && lower.is_sign_negative()
        } else {
            lower <= bucket.lower
        };
        let covers_right = if bucket.upper.is_infinite() && bucket.upper.is_sign_positive() {
            upper.is_infinite() && upper.is_sign_positive()
        } else {
            upper >= bucket.upper
        };
        return f64::from(covers_left && covers_right);
    }
    (overlap_upper - overlap_lower) / (bucket.upper - bucket.lower)
}

fn native_histogram_buckets(hist: &NativeHistogram) -> Vec<NativeQuantileBucket> {
    let mut buckets = Vec::new();
    if hist.is_nhcb() {
        let custom_values = hist.custom_values.as_deref().unwrap_or_default();
        append_native_spanned_buckets(
            &mut buckets,
            &hist.positive_spans,
            &hist.positive_counts,
            |index| NativeQuantileBucket {
                lower: custom_histogram_bound(index - 1, custom_values),
                upper: custom_histogram_bound(index, custom_values),
                count: 0.0,
            },
        );
        return buckets;
    }

    append_native_spanned_buckets(
        &mut buckets,
        &hist.negative_spans,
        &hist.negative_counts,
        |index| NativeQuantileBucket {
            lower: -standard_histogram_bound(index, hist.schema),
            upper: -standard_histogram_bound(index - 1, hist.schema),
            count: 0.0,
        },
    );
    if hist.zero_count != 0.0 {
        buckets.push(NativeQuantileBucket {
            lower: -hist.zero_threshold,
            upper: hist.zero_threshold,
            count: hist.zero_count,
        });
    }
    append_native_spanned_buckets(
        &mut buckets,
        &hist.positive_spans,
        &hist.positive_counts,
        |index| NativeQuantileBucket {
            lower: standard_histogram_bound(index - 1, hist.schema),
            upper: standard_histogram_bound(index, hist.schema),
            count: 0.0,
        },
    );
    buckets
}

fn append_native_spanned_buckets(
    buckets: &mut Vec<NativeQuantileBucket>,
    spans: &[BucketSpan],
    counts: &[f64],
    mut bucket_for_index: impl FnMut(i32) -> NativeQuantileBucket,
) {
    let mut index: i32 = 0;
    let mut count_index = 0;
    for (span_index, span) in spans.iter().enumerate() {
        // A malformed span whose offset overflows the running bucket index is
        // dropped (the rest of the spans with it) rather than overflow-panicking
        // on the `i32` accumulation.
        index = if span_index == 0 {
            span.offset
        } else {
            let Some(next) = index.checked_add(span.offset) else {
                return;
            };
            next
        };
        for _ in 0..span.length {
            let Some(count) = counts.get(count_index).copied() else {
                return;
            };
            let mut bucket = bucket_for_index(index);
            bucket.count = count;
            buckets.push(bucket);
            // A span that would walk the index past `i32::MAX` is similarly
            // dropped rather than wrapping.
            let Some(next) = index.checked_add(1) else {
                return;
            };
            index = next;
            count_index += 1;
        }
    }
}

fn standard_histogram_bound(index: i32, schema: i8) -> f64 {
    2_f64.powf(f64::from(index) * 2_f64.powi(-i32::from(schema)))
}

fn custom_histogram_bound(index: i32, custom_values: &[f64]) -> f64 {
    match index {
        -1 if custom_values.first().is_some_and(|value| *value > 0.0) => 0.0,
        -1 => f64::NEG_INFINITY,
        _ => usize::try_from(index)
            .ok()
            .and_then(|index| custom_values.get(index).copied())
            .unwrap_or(f64::INFINITY),
    }
}

#[derive(Clone, Copy)]
struct NativeQuantileBucket {
    lower: f64,
    upper: f64,
    count: f64,
}

#[derive(Clone, Copy)]
pub(super) enum HistogramAccessor {
    Count,
    Sum,
    Avg,
    Stddev,
    Stdvar,
}

impl HistogramAccessor {
    fn value(self, hist: &NativeHistogram) -> f64 {
        match self {
            Self::Count => hist.count,
            Self::Sum => hist.sum,
            Self::Avg => hist.sum / hist.count,
            Self::Stddev => native_histogram_stdvar(hist).sqrt(),
            Self::Stdvar => native_histogram_stdvar(hist),
        }
    }
}

/// Maps a native-histogram accessor function name to its [`HistogramAccessor`] variant.
///
/// This function mirrors the accessor arms of `PromqlEngine::eval_instant_call`.
/// It returns `None` for any other function, so the planner dispatch falls
/// through.
pub(super) fn histogram_accessor_from_function_name(name: &str) -> Option<HistogramAccessor> {
    Some(match name {
        "histogram_count" => HistogramAccessor::Count,
        "histogram_sum" => HistogramAccessor::Sum,
        "histogram_avg" => HistogramAccessor::Avg,
        "histogram_stddev" => HistogramAccessor::Stddev,
        "histogram_stdvar" => HistogramAccessor::Stdvar,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn histogram(schema: i8, reset_hint: ResetHint) -> NativeHistogram {
        NativeHistogram {
            schema,
            is_float: true,
            reset_hint,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 0.0,
            sum: 0.0,
            positive_spans: Vec::new(),
            positive_counts: Vec::new(),
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: (schema == -53).then(Vec::new),
            start_timestamp_ms: None,
        }
    }

    #[test]
    fn add_downscales_exponential_buckets() {
        let mut left = histogram(1, ResetHint::No);
        left.positive_spans = vec![BucketSpan {
            offset: -1,
            length: 4,
        }];
        left.positive_counts = vec![1.0, 2.0, 4.0, 8.0];
        let mut right = histogram(0, ResetHint::No);
        right.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 2,
        }];
        right.positive_counts = vec![10.0, 20.0];

        add_compatible_native_histogram(&mut left, &right).unwrap();

        assert2::assert!(left.schema == 0);
        assert2::assert!(
            left.positive_spans
                == vec![BucketSpan {
                    offset: 0,
                    length: 2
                }]
        );
        assert2::assert!(left.positive_counts == vec![13.0, 32.0]);
    }

    #[test]
    fn add_expands_zero_bucket_to_populated_bucket_boundary() {
        let mut left = histogram(0, ResetHint::No);
        left.zero_count = 1.0;
        left.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 1,
        }];
        left.positive_counts = vec![2.0];
        let mut right = histogram(0, ResetHint::No);
        right.zero_threshold = 0.75;
        right.zero_count = 3.0;

        add_compatible_native_histogram(&mut left, &right).unwrap();

        assert2::assert!((left.zero_threshold - 1.0).abs() < f64::EPSILON);
        assert2::assert!((left.zero_count - 6.0).abs() < f64::EPSILON);
        assert2::assert!(left.positive_spans.is_empty());
        assert2::assert!(left.positive_counts.is_empty());
    }

    #[test]
    fn add_reconciles_custom_bucket_bounds() {
        let mut left = histogram(-53, ResetHint::No);
        left.custom_values = Some(vec![1.0, 2.0, 4.0]);
        left.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 4,
        }];
        left.positive_counts = vec![1.0, 2.0, 3.0, 4.0];
        let mut right = histogram(-53, ResetHint::No);
        right.custom_values = Some(vec![1.0, 3.0, 4.0]);
        right.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 4,
        }];
        right.positive_counts = vec![10.0, 20.0, 30.0, 40.0];

        add_compatible_native_histogram(&mut left, &right).unwrap();

        assert2::assert!(left.custom_values == Some(vec![1.0, 4.0]));
        assert2::assert!(
            left.positive_spans
                == vec![BucketSpan {
                    offset: 0,
                    length: 3
                }]
        );
        assert2::assert!(left.positive_counts == vec![11.0, 55.0, 44.0]);
    }

    #[test]
    fn add_reconciles_counter_reset_hints() {
        let cases = [
            (ResetHint::No, ResetHint::No, ResetHint::No),
            (ResetHint::Yes, ResetHint::No, ResetHint::Unknown),
            (ResetHint::Unknown, ResetHint::No, ResetHint::Unknown),
            (ResetHint::No, ResetHint::Gauge, ResetHint::Gauge),
        ];
        for (left_hint, right_hint, expected) in cases {
            let mut left = histogram(0, left_hint);
            let right = histogram(0, right_hint);
            add_compatible_native_histogram(&mut left, &right).unwrap();
            assert2::assert!(left.reset_hint == expected);
        }
    }

    #[test]
    fn add_rejects_exponential_and_custom_bucket_mix() {
        let mut exponential = histogram(0, ResetHint::No);
        let custom = histogram(-53, ResetHint::No);

        let error = add_compatible_native_histogram(&mut exponential, &custom).unwrap_err();

        assert2::assert!(format!("{error}").contains("exponential and custom-bucket"));
    }
}
