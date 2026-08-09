use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};

use axum::{
    Json,
    response::{IntoResponse, Response},
};
use crabka_blockstore::{Labels, SeriesFingerprint};
use crabka_metrics::{BucketSpan, NativeHistogram};
use serde_json::{Map, Value, json};

use super::apply_limit;
use crate::{QueryResult, RangeSeries, SampleValue, store::ExemplarRecord};

pub(super) fn success_response(result: QueryResult) -> Response {
    Json(json!({
        "status": "success",
        "data": result_json(result),
    }))
    .into_response()
}

pub(super) fn success_data_response(data: impl serde::Serialize) -> Response {
    Json(json!({
        "status": "success",
        "data": data,
    }))
    .into_response()
}

fn result_json(result: QueryResult) -> Value {
    match result {
        QueryResult::Scalar { ts_ms, value } => json!({
            "resultType": "scalar",
            "result": [timestamp_seconds(ts_ms), sample_string(value)],
        }),
        QueryResult::InstantVector(samples) => {
            let result = samples
                .into_iter()
                .map(|sample| match sample.value {
                    SampleValue::Float(value) => json!({
                        "metric": labels_json(&sample.labels),
                        "value": [timestamp_seconds(sample.ts_ms), sample_string(value)],
                    }),
                    SampleValue::Histogram(histogram) => json!({
                        "metric": labels_json(&sample.labels),
                        "histogram": [timestamp_seconds(sample.ts_ms), native_histogram_json(&histogram)],
                    }),
                })
                .collect::<Vec<_>>();
            json!({
                "resultType": "vector",
                "result": result,
            })
        }
        QueryResult::RangeMatrix(series) => json!({
            "resultType": "matrix",
            "result": range_matrix_json(series),
        }),
        QueryResult::Str { ts_ms, value } => json!({
            "resultType": "string",
            "result": [timestamp_seconds(ts_ms), value],
        }),
    }
}

fn range_matrix_json(series: Vec<RangeSeries>) -> Vec<Value> {
    series
        .into_iter()
        .map(|series| {
            let mut values = Vec::new();
            let mut histograms = Vec::new();
            for (ts_ms, sample) in series.samples {
                match sample {
                    SampleValue::Float(value) => {
                        values.push(json!([timestamp_seconds(ts_ms), sample_string(value)]));
                    }
                    SampleValue::Histogram(histogram) => {
                        histograms.push(json!([
                            timestamp_seconds(ts_ms),
                            native_histogram_json(&histogram)
                        ]));
                    }
                }
            }
            let mut object = Map::new();
            object.insert("metric".to_string(), labels_json(&series.labels));
            if !values.is_empty() {
                object.insert("values".to_string(), Value::Array(values));
            }
            if !histograms.is_empty() {
                object.insert("histograms".to_string(), Value::Array(histograms));
            }
            Value::Object(object)
        })
        .collect()
}

fn native_histogram_json(histogram: &NativeHistogram) -> Value {
    json!({
        "count": sample_string(histogram.count),
        "sum": sample_string(histogram.sum),
        "buckets": native_histogram_buckets_json(histogram),
    })
}

fn native_histogram_buckets_json(histogram: &NativeHistogram) -> Vec<Value> {
    let mut buckets = Vec::new();
    if histogram.is_nhcb() {
        append_custom_histogram_buckets(&mut buckets, histogram);
    } else {
        append_standard_histogram_buckets(&mut buckets, histogram);
    }
    buckets.sort_by(|left, right| left.lower.total_cmp(&right.lower));
    buckets
        .into_iter()
        .map(|bucket| {
            json!([
                bucket.boundary_rule,
                sample_string(bucket.lower),
                sample_string(bucket.upper),
                sample_string(bucket.count),
            ])
        })
        .collect()
}

fn append_standard_histogram_buckets(
    buckets: &mut Vec<HistogramBucketJson>,
    hist: &NativeHistogram,
) {
    append_spanned_buckets(
        buckets,
        &hist.negative_spans,
        &hist.negative_counts,
        |index| HistogramBucketJson {
            boundary_rule: BOUNDARY_OPEN_RIGHT,
            lower: -standard_histogram_bound(index, hist.schema),
            upper: -standard_histogram_bound(index - 1, hist.schema),
            count: 0.0,
        },
    );
    if hist.zero_count != 0.0 {
        buckets.push(HistogramBucketJson {
            boundary_rule: BOUNDARY_CLOSED_BOTH,
            lower: -hist.zero_threshold,
            upper: hist.zero_threshold,
            count: hist.zero_count,
        });
    }
    append_spanned_buckets(
        buckets,
        &hist.positive_spans,
        &hist.positive_counts,
        |index| HistogramBucketJson {
            boundary_rule: BOUNDARY_OPEN_LEFT,
            lower: standard_histogram_bound(index - 1, hist.schema),
            upper: standard_histogram_bound(index, hist.schema),
            count: 0.0,
        },
    );
}

fn append_custom_histogram_buckets(buckets: &mut Vec<HistogramBucketJson>, hist: &NativeHistogram) {
    let custom_values = hist.custom_values.as_deref().unwrap_or_default();
    append_spanned_buckets(
        buckets,
        &hist.positive_spans,
        &hist.positive_counts,
        |index| HistogramBucketJson {
            boundary_rule: BOUNDARY_OPEN_LEFT,
            lower: custom_histogram_bound(index - 1, custom_values),
            upper: custom_histogram_bound(index, custom_values),
            count: 0.0,
        },
    );
}

fn append_spanned_buckets(
    buckets: &mut Vec<HistogramBucketJson>,
    spans: &[BucketSpan],
    counts: &[f64],
    mut bucket_for_index: impl FnMut(i32) -> HistogramBucketJson,
) {
    let mut index = 0;
    let mut count_index = 0;
    for (span_index, span) in spans.iter().enumerate() {
        if span_index == 0 {
            index = span.offset;
        } else {
            index += span.offset;
        }
        for _ in 0..span.length {
            let Some(count) = counts.get(count_index).copied() else {
                return;
            };
            let mut bucket = bucket_for_index(index);
            bucket.count = count;
            buckets.push(bucket);
            index += 1;
            count_index += 1;
        }
    }
}

fn standard_histogram_bound(index: i32, schema: i8) -> f64 {
    2_f64.powf(f64::from(index) * 2_f64.powi(-i32::from(schema)))
}

fn custom_histogram_bound(index: i32, custom_values: &[f64]) -> f64 {
    match index {
        -1 => f64::NEG_INFINITY,
        _ => usize::try_from(index)
            .ok()
            .and_then(|index| custom_values.get(index).copied())
            .unwrap_or(f64::INFINITY),
    }
}

const BOUNDARY_OPEN_LEFT: u8 = 0;
const BOUNDARY_OPEN_RIGHT: u8 = 1;
const BOUNDARY_CLOSED_BOTH: u8 = 3;

struct HistogramBucketJson {
    boundary_rule: u8,
    lower: f64,
    upper: f64,
    count: f64,
}

pub(super) fn exemplars_json(exemplars: Vec<ExemplarRecord>) -> Vec<Value> {
    let mut groups = BTreeMap::<String, (Labels, Vec<Value>)>::new();
    for exemplar in exemplars {
        let key = labels_key(&exemplar.series_labels);
        let labels_json = labels_json(&exemplar.labels);
        let value = json!({
            "labels": labels_json,
            "value": sample_string(exemplar.value),
            "timestamp": timestamp_seconds(exemplar.ts_ms),
        });
        groups
            .entry(key)
            .or_insert_with(|| (exemplar.series_labels, Vec::new()))
            .1
            .push(value);
    }

    groups
        .into_values()
        .map(|(series_labels, exemplars)| {
            json!({
                "seriesLabels": labels_json(&series_labels),
                "exemplars": exemplars,
            })
        })
        .collect()
}

/// Builds the Grafana Mimir `/cardinality/label_names` response from a series set.
///
/// Shape: `{ "label_values_count_total": N, "label_names_count": M,
/// "cardinality": [{ "label_name": .., "label_values_count": k }, ..] }`.
///
/// This function sorts the `cardinality` array by `label_values_count` DESC,
/// then by `label_name` ASC. A `limit` greater than 0 truncates that array. This
/// function computes the two totals over the full, unlimited series set.
pub(super) fn cardinality_label_names_response(series: &[Labels], limit: Option<usize>) -> Value {
    let mut values_by_name = BTreeMap::<String, BTreeSet<String>>::new();
    for labels in series {
        for (name, value) in labels.iter() {
            values_by_name
                .entry(name.clone())
                .or_default()
                .insert(value.clone());
        }
    }

    let label_names_count = values_by_name.len();
    let label_values_count_total: usize = values_by_name.values().map(BTreeSet::len).sum();

    let mut cardinality = values_by_name
        .into_iter()
        .map(|(name, values)| (name, values.len()))
        .collect::<Vec<_>>();
    cardinality.sort_by(|(left_name, left_count), (right_name, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_name.cmp(right_name))
    });
    apply_limit(&mut cardinality, limit);

    let entries = cardinality
        .into_iter()
        .map(|(label_name, label_values_count)| {
            json!({
                "label_name": label_name,
                "label_values_count": label_values_count,
            })
        })
        .collect::<Vec<_>>();

    json!({
        "label_values_count_total": label_values_count_total,
        "label_names_count": label_names_count,
        "cardinality": entries,
    })
}

/// Builds the Grafana Mimir `/cardinality/label_values` response from a series set.
///
/// Shape: `{ "series_count_total": N, "labels": [{ "label_name": ..,
/// "label_values_count": k, "series_count": s, "cardinality": [{
/// "label_value": .., "series_count": c }, ..] }, ..] }`.
///
/// This function sorts `labels` by `series_count` DESC, then by `label_name`
/// ASC. It sorts each nested `cardinality` by `series_count` DESC, then by
/// `label_value` ASC. A `limit` greater than 0 truncates each nested
/// `cardinality` array, as the per-label limit of Mimir does.
pub(super) fn cardinality_label_values_response(
    series: &[Labels],
    label_names: &[String],
    limit: Option<usize>,
) -> Value {
    // For each (label_name, label_value), the distinct series carrying it.
    let mut series_by_value =
        BTreeMap::<String, BTreeMap<String, BTreeSet<SeriesFingerprint>>>::new();
    let mut total_series = BTreeSet::<SeriesFingerprint>::new();
    for labels in series {
        let fp = labels.fingerprint();
        total_series.insert(fp);
        for (name, value) in labels.iter() {
            if !label_names.is_empty() && !label_names.iter().any(|wanted| wanted == name) {
                continue;
            }
            series_by_value
                .entry(name.clone())
                .or_default()
                .entry(value.clone())
                .or_default()
                .insert(fp);
        }
    }

    let mut labels_out = series_by_value
        .into_iter()
        .map(|(label_name, values)| {
            let label_values_count = values.len();
            let series_count: usize = values.values().flatten().collect::<BTreeSet<_>>().len();
            let mut value_cardinality = values
                .into_iter()
                .map(|(label_value, fingerprints)| (label_value, fingerprints.len()))
                .collect::<Vec<_>>();
            value_cardinality.sort_by(|(left_value, left_count), (right_value, right_count)| {
                right_count
                    .cmp(left_count)
                    .then_with(|| left_value.cmp(right_value))
            });
            apply_limit(&mut value_cardinality, limit);
            (
                label_name,
                label_values_count,
                series_count,
                value_cardinality,
            )
        })
        .collect::<Vec<_>>();
    labels_out.sort_by(
        |(left_name, _, left_series, _), (right_name, _, right_series, _)| {
            right_series
                .cmp(left_series)
                .then_with(|| left_name.cmp(right_name))
        },
    );

    let labels_json = labels_out
        .into_iter()
        .map(
            |(label_name, label_values_count, series_count, value_cardinality)| {
                let cardinality = value_cardinality
                    .into_iter()
                    .map(|(label_value, count)| {
                        json!({
                            "label_value": label_value,
                            "series_count": count,
                        })
                    })
                    .collect::<Vec<_>>();
                json!({
                    "label_name": label_name,
                    "label_values_count": label_values_count,
                    "series_count": series_count,
                    "cardinality": cardinality,
                })
            },
        )
        .collect::<Vec<_>>();

    json!({
        "series_count_total": total_series.len(),
        "labels": labels_json,
    })
}

/// Builds the Grafana Mimir `/cardinality/active_series` response.
///
/// The response is a bare object with one `data` array of flat label maps. It
/// has no `status` envelope and no `seriesLabels` or `metric` wrapper.
pub(super) fn active_series_response(series: Vec<Labels>) -> Value {
    let data = series
        .into_iter()
        .map(|labels| labels_json(&labels))
        .collect::<Vec<_>>();
    json!({ "data": data })
}

pub(super) fn labels_json(labels: &Labels) -> Value {
    let pairs = labels
        .iter()
        .map(|(name, value)| (name.clone(), Value::String(value.clone())))
        .collect::<BTreeMap<_, _>>();
    Value::Object(Map::from_iter(pairs))
}

pub(super) fn labels_key(labels: &Labels) -> String {
    labels.iter().fold(String::new(), |mut out, (name, value)| {
        let _ = writeln!(out, "{name}={value}");
        out
    })
}

pub(super) fn exemplar_key(exemplar: &ExemplarRecord) -> String {
    format!(
        "{}\n{}\n{}\n{}",
        labels_key(&exemplar.series_labels),
        labels_key(&exemplar.labels),
        exemplar.ts_ms,
        exemplar.value.to_bits()
    )
}

/// Encodes a millisecond timestamp as the JSON number Prometheus emits.
///
/// Prometheus emits this number from `jsonutil.MarshalTimestamp`. The number is
/// a bare integer for whole seconds, and otherwise a fraction with the trailing
/// zeros trimmed. Examples: `10`, `1435781430.781`, `-0.5`.
///
/// `serde_json` renders an `f64` of `10` as `10.0`. This function therefore
/// carries the value as a pre-formatted
/// [`RawValue`](serde_json::value::RawValue) number token, which keeps the
/// output byte-exact.
fn timestamp_seconds(ts_ms: i64) -> Box<serde_json::value::RawValue> {
    let token = format_timestamp_token(ts_ms);
    serde_json::value::RawValue::from_string(token)
        .expect("timestamp token is always valid JSON number syntax")
}

/// Builds the JSON number token for a millisecond timestamp.
///
/// This function mirrors Prometheus `MarshalTimestamp`. It writes the sign, then
/// the absolute integer seconds, then a millisecond fraction with the trailing
/// zeros trimmed when that fraction is non-zero.
fn format_timestamp_token(ts_ms: i64) -> String {
    let mut out = String::new();
    if ts_ms < 0 {
        out.push('-');
    }
    let magnitude = ts_ms.unsigned_abs();
    let seconds = magnitude / 1000;
    let fraction = magnitude % 1000;
    out.push_str(&seconds.to_string());
    if fraction != 0 {
        out.push('.');
        let padded = format!("{fraction:03}");
        out.push_str(padded.trim_end_matches('0'));
    }
    out
}

pub(super) fn sample_string(value: f64) -> String {
    format_sample_value(value)
}

/// Formats a float exactly like Prometheus `jsonutil.MarshalFloat`.
///
/// `jsonutil.MarshalFloat` calls the Go function
/// `strconv.AppendFloat(f, fmt, -1, 64)`. Go picks the `'e'` scientific notation
/// when the magnitude is `< 1e-6` or `>= 1e21`, and the `'f'` plain decimal
/// notation otherwise. Precision `-1` means the shortest representation that
/// round-trips back to the same `f64`.
pub(crate) fn format_sample_value(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f == f64::INFINITY {
        return "+Inf".to_string();
    }
    if f == f64::NEG_INFINITY {
        return "-Inf".to_string();
    }

    let abs = f.abs();
    if abs != 0.0 && !(1e-6..1e21).contains(&abs) {
        format_float_exponent(f)
    } else {
        // Rust's `Display` for `f64` already emits the shortest round-tripping
        // plain-decimal form (no exponent), matching Go's `'f'` form: `3.0` ->
        // "3", `1e20` -> "100000000000000000000", `0.000001` -> "0.000001".
        format!("{f}")
    }
}

/// Renders `f` in the Go `'e'` form, for example `1e+21`, `9.999e-07`, `-1.5e-07`.
///
/// The Rust `{:e}` format produces the same shortest mantissa but a bare
/// exponent, such as `1e21` and `9.999e-7`. Go always writes a sign and at least
/// two exponent digits. This function therefore re-assembles the exponent
/// suffix.
fn format_float_exponent(f: f64) -> String {
    let rust = format!("{f:e}");
    let (mantissa, exponent) = rust
        .split_once('e')
        .expect("Rust {:e} formatting always contains an exponent marker");
    let exponent: i32 = exponent
        .parse()
        .expect("Rust {:e} exponent is always a valid integer");
    let sign = if exponent < 0 { '-' } else { '+' };
    format!("{mantissa}e{sign}{:02}", exponent.abs())
}
