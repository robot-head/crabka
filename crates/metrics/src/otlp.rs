//! OTLP metrics translation into the shared ingest decode target.

#![allow(
    clippy::cast_precision_loss,
    reason = "OTLP metric points expose integer counts that the internal metric sample model stores as f64."
)]

use std::collections::BTreeMap;

use crabka_blockstore::Labels;
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    metrics::v1::{
        AggregationTemporality, Exemplar as OtlpExemplar, ExponentialHistogram,
        ExponentialHistogramDataPoint, Gauge, Histogram, HistogramDataPoint, Metric, MetricsData,
        NumberDataPoint, ScopeMetrics, Sum, Summary, SummaryDataPoint, exemplar as otlp_exemplar,
        exponential_histogram_data_point, metric, number_data_point,
    },
};
use prost::Message as _;

use crate::{
    BucketSpan, NativeHistogram, ResetHint,
    wire::{DecodedExemplar, DecodedMetadata, DecodedSample, DecodedSeries},
};

const MAX_NATIVE_HISTOGRAM_SCHEMA: i32 = 8;
const MIN_NATIVE_HISTOGRAM_SCHEMA: i32 = -4;

/// Sane upper bound for an ingested sample timestamp, in milliseconds. Data
/// points beyond this are rejected rather than translated into an absurd
/// far-future millisecond value that would poison the per-series
/// out-of-order/too-old window. `7_258_118_400_000` is `2200-01-01T00:00:00Z`:
/// well past any legitimate metric timestamp yet still reachable from a `u64`
/// `time_unix_nano` (whose ceiling is ~year 2554), so an absurd point such as
/// `u64::MAX` is rejected consistent with Prometheus future-sample rejection.
const MAX_SAMPLE_TIMESTAMP_MS: u64 = 7_258_118_400_000;

/// Prometheus translation strategy for OTLP metric names.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TranslationStrategy {
    /// Replace unsupported Prometheus metric/label characters with underscores
    /// and apply conventional suffixes such as `_total` for monotonic sums.
    #[default]
    UnderscoreEscapingWithSuffixes,
}

/// Errors from OTLP metrics translation.
#[derive(Debug, thiserror::Error)]
pub enum OtlpError {
    #[error("protobuf decode failed: {0}")]
    ProtobufDecode(#[from] prost::DecodeError),
    #[error("delta temporality is not supported yet for metric `{0}`")]
    DeltaUnsupported(String),
    #[error("invalid OTLP metric `{0}`: {1}")]
    Invalid(String, String),
    #[error("unsupported OTLP metric `{0}`: {1}")]
    Unsupported(String, String),
}

#[derive(Clone, Debug, Default)]
struct DeltaState {
    start_time_unix_nano: u64,
    value: f64,
}

#[derive(Clone, Debug, Default)]
struct DeltaHistogramState {
    start_time_unix_nano: u64,
    value: Option<NativeHistogram>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct DeltaKey {
    labels: Vec<(String, String)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExemplarPolicy {
    Keep,
    Drop,
}

/// Stateful accumulator for OTLP delta-temporality sums and histograms.
#[derive(Clone, Debug, Default)]
pub struct DeltaAccumulator {
    sums: BTreeMap<DeltaKey, DeltaState>,
    histograms: BTreeMap<DeltaKey, DeltaHistogramState>,
}

impl DeltaAccumulator {
    fn accumulate_sum(&mut self, labels: &Labels, start_time_unix_nano: u64, delta: f64) -> f64 {
        let key = delta_key(labels);
        let state = self.sums.entry(key).or_default();
        if start_time_unix_nano != 0
            && state.start_time_unix_nano != 0
            && state.start_time_unix_nano != start_time_unix_nano
        {
            state.value = delta;
        } else {
            state.value += delta;
        }
        if start_time_unix_nano != 0 {
            state.start_time_unix_nano = start_time_unix_nano;
        }
        state.value
    }

    fn accumulate_histogram(
        &mut self,
        metric_name: &str,
        labels: &Labels,
        start_time_unix_nano: u64,
        delta: NativeHistogram,
    ) -> Result<NativeHistogram, OtlpError> {
        let key = delta_key(labels);
        let state = self.histograms.entry(key).or_default();
        if start_time_unix_nano != 0
            && state.start_time_unix_nano != 0
            && state.start_time_unix_nano != start_time_unix_nano
        {
            state.value = Some(delta);
        } else if let Some(cumulative) = &mut state.value {
            add_compatible_native_histogram(metric_name, cumulative, &delta)?;
        } else {
            state.value = Some(delta);
        }
        if start_time_unix_nano != 0 {
            state.start_time_unix_nano = start_time_unix_nano;
        }
        state.value.clone().ok_or_else(|| {
            OtlpError::Invalid(metric_name.into(), "missing accumulated histogram".into())
        })
    }
}

fn delta_key(labels: &Labels) -> DeltaKey {
    DeltaKey {
        labels: labels
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
    }
}

fn add_compatible_native_histogram(
    metric_name: &str,
    cumulative: &mut NativeHistogram,
    delta: &NativeHistogram,
) -> Result<(), OtlpError> {
    if cumulative.schema != delta.schema
        || cumulative.is_float != delta.is_float
        || cumulative.reset_hint != delta.reset_hint
        || cumulative.zero_threshold.to_bits() != delta.zero_threshold.to_bits()
        || cumulative.custom_values != delta.custom_values
    {
        return Err(OtlpError::Invalid(
            metric_name.into(),
            "incompatible delta exponential histogram layout".into(),
        ));
    }

    cumulative.zero_count += delta.zero_count;
    cumulative.count += delta.count;
    cumulative.sum += delta.sum;
    (cumulative.positive_spans, cumulative.positive_counts) = add_spanned_histogram_counts(
        &cumulative.positive_spans,
        &cumulative.positive_counts,
        &delta.positive_spans,
        &delta.positive_counts,
    );
    (cumulative.negative_spans, cumulative.negative_counts) = add_spanned_histogram_counts(
        &cumulative.negative_spans,
        &cumulative.negative_counts,
        &delta.negative_spans,
        &delta.negative_counts,
    );
    Ok(())
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

impl OtlpError {
    /// HTTP status code for OTLP HTTP ingest.
    #[must_use]
    pub fn status_code(&self) -> u16 {
        400
    }
}

/// Decode a protobuf-encoded `MetricsData` body.
pub fn decode_otlp_bytes(
    body: &[u8],
    strategy: TranslationStrategy,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    decode_otlp(&MetricsData::decode(body)?, strategy)
}

/// Decode a protobuf-encoded `MetricsData` body with delta-state handling.
pub fn decode_otlp_stateful_bytes(
    body: &[u8],
    strategy: TranslationStrategy,
    accumulator: &mut DeltaAccumulator,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    decode_otlp_stateful(&MetricsData::decode(body)?, strategy, accumulator)
}

/// Translate OTLP metrics into the common ingest representation.
pub fn decode_otlp(
    data: &MetricsData,
    strategy: TranslationStrategy,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    let mut accumulator = DeltaAccumulator::default();
    decode_otlp_inner(data, strategy, Some(&mut accumulator))
}

/// Translate OTLP metrics with delta-temporality accumulation across calls.
pub fn decode_otlp_stateful(
    data: &MetricsData,
    strategy: TranslationStrategy,
    accumulator: &mut DeltaAccumulator,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    decode_otlp_inner(data, strategy, Some(accumulator))
}

fn decode_otlp_inner(
    data: &MetricsData,
    strategy: TranslationStrategy,
    mut accumulator: Option<&mut DeltaAccumulator>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    let mut out = Vec::new();
    for resource_metrics in &data.resource_metrics {
        let resource_attributes = resource_metrics
            .resource
            .as_ref()
            .map_or(&[][..], |resource| resource.attributes.as_slice());

        if !resource_attributes.is_empty()
            && let Some(timestamp_ms) = resource_metrics_timestamp_ms(resource_metrics)
        {
            out.push(DecodedSeries {
                labels: labels("target_info", resource_attributes, &[], None),
                samples: vec![DecodedSample::new(timestamp_ms, 1.0)],
                histograms: Vec::new(),
                exemplars: Vec::new(),
                metadata: Some(DecodedMetadata {
                    metric_family_name: "target_info".into(),
                    metric_type: "gauge".into(),
                    help: "Target metadata.".into(),
                    unit: String::new(),
                }),
            });
        }

        for scope_metrics in &resource_metrics.scope_metrics {
            let metric_attributes = metric_attributes(resource_attributes, scope_metrics);
            for metric in &scope_metrics.metrics {
                out.extend(metric_series(
                    metric,
                    &metric_attributes,
                    strategy,
                    accumulator.as_deref_mut(),
                )?);
            }
        }
    }
    Ok(out)
}

fn metric_attributes(
    resource_attributes: &[KeyValue],
    scope_metrics: &ScopeMetrics,
) -> Vec<KeyValue> {
    let mut attributes = resource_attributes.to_vec();
    attributes.extend(scope_attributes(scope_metrics));
    attributes
}

fn scope_attributes(scope_metrics: &ScopeMetrics) -> Vec<KeyValue> {
    let mut attributes = Vec::new();
    if let Some(scope) = &scope_metrics.scope {
        attributes.extend(instrumentation_scope_attributes(scope));
    }
    if !scope_metrics.schema_url.is_empty() {
        attributes.push(string_attribute(
            "otel_scope_schema_url",
            &scope_metrics.schema_url,
        ));
    }
    attributes
}

fn instrumentation_scope_attributes(scope: &InstrumentationScope) -> Vec<KeyValue> {
    let mut attributes = Vec::new();
    if !scope.name.is_empty() {
        attributes.push(string_attribute("otel_scope_name", &scope.name));
    }
    if !scope.version.is_empty() {
        attributes.push(string_attribute("otel_scope_version", &scope.version));
    }
    for attribute in &scope.attributes {
        let key = format!("otel_scope_{}", attribute.key);
        let normalized = normalize_name(&key, TranslationStrategy::default());
        if matches!(
            normalized.as_str(),
            "otel_scope_name" | "otel_scope_version" | "otel_scope_schema_url"
        ) {
            continue;
        }
        let mut attribute = attribute.clone();
        attribute.key = key;
        attributes.push(attribute);
    }
    attributes
}

fn string_attribute(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
        key_strindex: 0,
    }
}

fn resource_metrics_timestamp_ms(
    resource_metrics: &opentelemetry_proto::tonic::metrics::v1::ResourceMetrics,
) -> Option<i64> {
    for scope_metrics in &resource_metrics.scope_metrics {
        for metric in &scope_metrics.metrics {
            let Some(data) = &metric.data else {
                continue;
            };
            let timestamp = match data {
                metric::Data::Gauge(gauge) => {
                    gauge.data_points.first().map(|point| point.time_unix_nano)
                }
                metric::Data::Sum(sum) => sum.data_points.first().map(|point| point.time_unix_nano),
                metric::Data::Histogram(histogram) => histogram
                    .data_points
                    .first()
                    .map(|point| point.time_unix_nano),
                metric::Data::ExponentialHistogram(histogram) => histogram
                    .data_points
                    .first()
                    .map(|point| point.time_unix_nano),
                metric::Data::Summary(summary) => summary
                    .data_points
                    .first()
                    .map(|point| point.time_unix_nano),
            };
            if let Some(timestamp) = timestamp {
                return Some(nanos_to_millis(timestamp));
            }
        }
    }
    None
}

/// Normalize an OTLP identifier into a Prometheus-compatible metric or label name.
#[must_use]
pub fn normalize_name(name: &str, _strategy: TranslationStrategy) -> String {
    let mut out = String::with_capacity(name.len());
    for (index, ch) in name.chars().enumerate() {
        let valid = ch == '_' || ch.is_ascii_alphanumeric();
        if valid && (index != 0 || !ch.is_ascii_digit()) {
            out.push(ch);
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    if out.is_empty() { "_".to_string() } else { out }
}

fn translated_metric_name(
    metric: &Metric,
    strategy: TranslationStrategy,
    add_total: bool,
) -> String {
    let mut name = normalize_name(&metric.name, strategy);
    if add_total && let Some(base) = name.strip_suffix("_total") {
        name = base.to_string();
    }
    if let Some(unit_suffix) = prometheus_unit_suffix(&metric.unit)
        && !name.ends_with(&unit_suffix)
    {
        name.push('_');
        name.push_str(&unit_suffix);
    }
    if add_total && !name.ends_with("_total") {
        name.push_str("_total");
    }
    name
}

fn prometheus_unit_suffix(unit: &str) -> Option<String> {
    let cleaned = strip_ucum_annotations(unit.trim());
    let unit = cleaned.trim();
    if unit.is_empty() || unit == "1" {
        return None;
    }
    if let Some(numerator) = unit.strip_suffix("/s")
        && let Some(numerator) = prometheus_base_unit_suffix(numerator)
    {
        return Some(format!("{numerator}_per_second"));
    }
    prometheus_base_unit_suffix(unit).map(str::to_string)
}

fn strip_ucum_annotations(unit: &str) -> String {
    let mut out = String::with_capacity(unit.len());
    let mut in_annotation = false;
    for ch in unit.chars() {
        match ch {
            '{' => in_annotation = true,
            '}' if in_annotation => in_annotation = false,
            _ if !in_annotation => out.push(ch),
            _ => {}
        }
    }
    out
}

fn prometheus_base_unit_suffix(unit: &str) -> Option<&'static str> {
    match unit {
        "d" => Some("days"),
        "h" => Some("hours"),
        "min" => Some("minutes"),
        "s" => Some("seconds"),
        "ms" => Some("milliseconds"),
        "us" => Some("microseconds"),
        "ns" => Some("nanoseconds"),
        "m" => Some("meters"),
        "By" => Some("bytes"),
        "KiBy" => Some("kibibytes"),
        "MiBy" => Some("mebibytes"),
        "GiBy" => Some("gibibytes"),
        "TiBy" => Some("tebibytes"),
        "kBy" => Some("kilobytes"),
        "MBy" => Some("megabytes"),
        "GBy" => Some("gigabytes"),
        "TBy" => Some("terabytes"),
        "bit" => Some("bits"),
        "V" => Some("volts"),
        "A" => Some("amperes"),
        "J" => Some("joules"),
        "W" => Some("watts"),
        "g" => Some("grams"),
        "Cel" => Some("celsius"),
        "Hz" => Some("hertz"),
        "%" => Some("percent"),
        _ => None,
    }
}

/// Convert one OTLP exponential histogram point to a native histogram sample.
pub fn exponential_histogram_to_native(
    point: &ExponentialHistogramDataPoint,
) -> Result<NativeHistogram, OtlpError> {
    if point.scale < MIN_NATIVE_HISTOGRAM_SCHEMA {
        return Err(OtlpError::Invalid(
            "exponential histogram".into(),
            format!(
                "scale {} is below native histogram minimum schema -4",
                point.scale
            ),
        ));
    }
    let schema = point.scale.min(MAX_NATIVE_HISTOGRAM_SCHEMA);
    let (positive_spans, positive_counts) =
        downscaled_spans(point.positive.as_ref(), point.scale, schema)?;
    let (negative_spans, negative_counts) =
        downscaled_spans(point.negative.as_ref(), point.scale, schema)?;

    Ok(NativeHistogram {
        schema: i8::try_from(schema).map_err(|_| {
            OtlpError::Invalid(
                "exponential histogram".into(),
                format!("scale {schema} out of range"),
            )
        })?,
        is_float: false,
        reset_hint: ResetHint::Unknown,
        zero_threshold: point.zero_threshold,
        zero_count: point.zero_count as f64,
        count: point.count as f64,
        sum: point.sum.unwrap_or(0.0),
        positive_spans,
        positive_counts,
        negative_spans,
        negative_counts,
        custom_values: None,
        start_timestamp_ms: (point.start_time_unix_nano != 0)
            .then_some(nanos_to_millis(point.start_time_unix_nano)),
    })
}

fn metric_series(
    metric: &Metric,
    resource_attributes: &[KeyValue],
    strategy: TranslationStrategy,
    accumulator: Option<&mut DeltaAccumulator>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    let Some(data) = &metric.data else {
        return Ok(Vec::new());
    };

    reject_far_future_points(&metric.name, data)?;

    match data {
        metric::Data::Gauge(gauge) => gauge_series(metric, gauge, resource_attributes, strategy),
        metric::Data::Sum(sum) => {
            sum_series(metric, sum, resource_attributes, strategy, accumulator)
        }
        metric::Data::Histogram(histogram) => histogram_series(
            metric,
            histogram,
            resource_attributes,
            strategy,
            accumulator,
        ),
        metric::Data::ExponentialHistogram(histogram) => exponential_histogram_series(
            metric,
            histogram,
            resource_attributes,
            strategy,
            accumulator,
        ),
        metric::Data::Summary(summary) => Ok(summary_series(
            metric,
            summary,
            resource_attributes,
            strategy,
        )),
    }
}

/// Reject any data point whose `time_unix_nano` is beyond the sane future bound.
/// Clamping such a value to `i64::MAX` would poison the per-series
/// out-of-order/too-old window downstream, so we drop the request instead,
/// matching Prometheus's rejection of samples too far in the future.
fn reject_far_future_points(name: &str, data: &metric::Data) -> Result<(), OtlpError> {
    let mut timestamps = Vec::new();
    match data {
        metric::Data::Gauge(gauge) => {
            timestamps.extend(gauge.data_points.iter().map(|point| point.time_unix_nano));
        }
        metric::Data::Sum(sum) => {
            timestamps.extend(sum.data_points.iter().map(|point| point.time_unix_nano));
        }
        metric::Data::Histogram(histogram) => {
            timestamps.extend(
                histogram
                    .data_points
                    .iter()
                    .map(|point| point.time_unix_nano),
            );
        }
        metric::Data::ExponentialHistogram(histogram) => {
            timestamps.extend(
                histogram
                    .data_points
                    .iter()
                    .map(|point| point.time_unix_nano),
            );
        }
        metric::Data::Summary(summary) => {
            timestamps.extend(summary.data_points.iter().map(|point| point.time_unix_nano));
        }
    }
    if let Some(time_unix_nano) = timestamps
        .into_iter()
        .find(|time_unix_nano| time_unix_nano / 1_000_000 > MAX_SAMPLE_TIMESTAMP_MS)
    {
        return Err(OtlpError::Invalid(
            name.into(),
            format!("data point timestamp {time_unix_nano}ns is too far in the future"),
        ));
    }
    Ok(())
}

fn gauge_series(
    metric: &Metric,
    gauge: &Gauge,
    resource_attributes: &[KeyValue],
    strategy: TranslationStrategy,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    gauge
        .data_points
        .iter()
        .map(|point| {
            let name = translated_metric_name(metric, strategy, false);
            let metadata = metric_metadata(metric, &name, "gauge");
            scalar_series(
                &name,
                point,
                resource_attributes,
                Some(metadata),
                ExemplarPolicy::Drop,
            )
        })
        .collect()
}

fn sum_series(
    metric: &Metric,
    sum: &Sum,
    resource_attributes: &[KeyValue],
    strategy: TranslationStrategy,
    accumulator: Option<&mut DeltaAccumulator>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    let name = translated_metric_name(metric, strategy, sum.is_monotonic);

    if sum.aggregation_temporality == AggregationTemporality::Delta as i32 {
        let Some(accumulator) = accumulator else {
            return Err(OtlpError::DeltaUnsupported(metric.name.clone()));
        };
        return sum
            .data_points
            .iter()
            .map(|point| {
                delta_sum_series(
                    &name,
                    point,
                    resource_attributes,
                    accumulator,
                    Some(metric_metadata(metric, &name, sum_metadata_type(sum))),
                )
            })
            .collect();
    }

    if sum.aggregation_temporality != AggregationTemporality::Cumulative as i32
        && sum.aggregation_temporality != AggregationTemporality::Unspecified as i32
    {
        return Err(OtlpError::DeltaUnsupported(metric.name.clone()));
    }

    sum.data_points
        .iter()
        .map(|point| {
            let exemplar_policy = if sum.is_monotonic {
                ExemplarPolicy::Keep
            } else {
                ExemplarPolicy::Drop
            };
            scalar_series(
                &name,
                point,
                resource_attributes,
                Some(metric_metadata(metric, &name, sum_metadata_type(sum))),
                exemplar_policy,
            )
        })
        .collect()
}

fn sum_metadata_type(sum: &Sum) -> &'static str {
    if sum.is_monotonic { "counter" } else { "gauge" }
}

fn delta_sum_series(
    name: &str,
    point: &NumberDataPoint,
    resource_attributes: &[KeyValue],
    accumulator: &mut DeltaAccumulator,
    metadata: Option<DecodedMetadata>,
) -> Result<DecodedSeries, OtlpError> {
    let delta = number_value(point)
        .ok_or_else(|| OtlpError::Invalid(name.into(), "missing number datapoint value".into()))?;
    let labels = labels(name, resource_attributes, &point.attributes, None);
    let cumulative = accumulator.accumulate_sum(&labels, point.start_time_unix_nano, delta);
    Ok(DecodedSeries {
        labels,
        samples: vec![DecodedSample::with_start_timestamp(
            nanos_to_millis(point.time_unix_nano),
            cumulative,
            (point.start_time_unix_nano != 0)
                .then_some(nanos_to_millis(point.start_time_unix_nano)),
        )],
        histograms: Vec::new(),
        exemplars: exemplars_from_number_point(point),
        metadata,
    })
}

fn scalar_series(
    name: &str,
    point: &NumberDataPoint,
    resource_attributes: &[KeyValue],
    metadata: Option<DecodedMetadata>,
    exemplar_policy: ExemplarPolicy,
) -> Result<DecodedSeries, OtlpError> {
    let value = number_value(point)
        .ok_or_else(|| OtlpError::Invalid(name.into(), "missing number datapoint value".into()))?;
    let exemplars = match exemplar_policy {
        ExemplarPolicy::Keep => exemplars_from_number_point(point),
        ExemplarPolicy::Drop => Vec::new(),
    };
    Ok(DecodedSeries {
        labels: labels(name, resource_attributes, &point.attributes, None),
        samples: vec![DecodedSample::with_start_timestamp(
            nanos_to_millis(point.time_unix_nano),
            value,
            (point.start_time_unix_nano != 0)
                .then_some(nanos_to_millis(point.start_time_unix_nano)),
        )],
        histograms: Vec::new(),
        exemplars,
        metadata,
    })
}

fn histogram_series(
    metric: &Metric,
    histogram: &Histogram,
    resource_attributes: &[KeyValue],
    strategy: TranslationStrategy,
    mut accumulator: Option<&mut DeltaAccumulator>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    let name = translated_metric_name(metric, strategy, false);
    let metadata = metric_metadata(metric, &name, "histogram");
    let mut out = Vec::new();
    for point in &histogram.data_points {
        let mut point_series =
            classic_histogram_series(&name, point, resource_attributes, Some(&metadata))?;
        if histogram.aggregation_temporality == AggregationTemporality::Delta as i32 {
            let Some(accumulator) = accumulator.as_deref_mut() else {
                return Err(OtlpError::DeltaUnsupported(metric.name.clone()));
            };
            accumulate_delta_float_series(
                &mut point_series,
                point.start_time_unix_nano,
                accumulator,
            );
        } else if histogram.aggregation_temporality != AggregationTemporality::Cumulative as i32
            && histogram.aggregation_temporality != AggregationTemporality::Unspecified as i32
        {
            return Err(OtlpError::DeltaUnsupported(metric.name.clone()));
        }
        out.extend(point_series);
    }
    Ok(out)
}

fn accumulate_delta_float_series(
    series: &mut [DecodedSeries],
    start_time_unix_nano: u64,
    accumulator: &mut DeltaAccumulator,
) {
    for series in series {
        for sample in &mut series.samples {
            sample.value =
                accumulator.accumulate_sum(&series.labels, start_time_unix_nano, sample.value);
            if start_time_unix_nano != 0 {
                sample.start_timestamp_ms = Some(nanos_to_millis(start_time_unix_nano));
            }
        }
    }
}

fn classic_histogram_series(
    name: &str,
    point: &HistogramDataPoint,
    resource_attributes: &[KeyValue],
    metadata: Option<&DecodedMetadata>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    if !point.bucket_counts.is_empty()
        && point.bucket_counts.len() != point.explicit_bounds.len() + 1
    {
        return Err(OtlpError::Invalid(
            name.into(),
            "bucket_counts length must be explicit_bounds length plus one".into(),
        ));
    }

    let timestamp = nanos_to_millis(point.time_unix_nano);
    let point_exemplars = exemplars_from_histogram_point(point);
    let mut out = Vec::new();
    let base_name = format!("{name}_bucket");
    let mut cumulative = 0_u64;
    for (idx, count) in point.bucket_counts.iter().enumerate() {
        cumulative = cumulative.saturating_add(*count);
        let le = point
            .explicit_bounds
            .get(idx)
            .map_or_else(|| "+Inf".to_string(), ToString::to_string);
        out.push(DecodedSeries {
            labels: labels(
                &base_name,
                resource_attributes,
                &point.attributes,
                Some(("le", &le)),
            ),
            samples: vec![DecodedSample::with_start_timestamp(
                timestamp,
                cumulative as f64,
                (point.start_time_unix_nano != 0)
                    .then_some(nanos_to_millis(point.start_time_unix_nano)),
            )],
            histograms: Vec::new(),
            exemplars: exemplars_for_bucket(&point_exemplars, point, idx),
            metadata: metadata.cloned(),
        });
    }

    out.push(DecodedSeries {
        labels: labels(
            &format!("{name}_count"),
            resource_attributes,
            &point.attributes,
            None,
        ),
        samples: vec![DecodedSample::with_start_timestamp(
            timestamp,
            point.count as f64,
            (point.start_time_unix_nano != 0)
                .then_some(nanos_to_millis(point.start_time_unix_nano)),
        )],
        histograms: Vec::new(),
        exemplars: Vec::new(),
        metadata: metadata.cloned(),
    });
    if let Some(sum) = point.sum {
        out.push(DecodedSeries {
            labels: labels(
                &format!("{name}_sum"),
                resource_attributes,
                &point.attributes,
                None,
            ),
            samples: vec![DecodedSample::with_start_timestamp(
                timestamp,
                sum,
                (point.start_time_unix_nano != 0)
                    .then_some(nanos_to_millis(point.start_time_unix_nano)),
            )],
            histograms: Vec::new(),
            exemplars: Vec::new(),
            metadata: metadata.cloned(),
        });
    }
    Ok(out)
}

fn exponential_histogram_series(
    metric: &Metric,
    histogram: &ExponentialHistogram,
    resource_attributes: &[KeyValue],
    strategy: TranslationStrategy,
    mut accumulator: Option<&mut DeltaAccumulator>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    let name = translated_metric_name(metric, strategy, false);
    let metadata = metric_metadata(metric, &name, "histogram");
    let mut out = Vec::new();
    for point in &histogram.data_points {
        let labels = labels(&name, resource_attributes, &point.attributes, None);
        let mut native_histogram = exponential_histogram_to_native(point)?;
        if histogram.aggregation_temporality == AggregationTemporality::Delta as i32 {
            let Some(accumulator) = accumulator.as_deref_mut() else {
                return Err(OtlpError::DeltaUnsupported(metric.name.clone()));
            };
            native_histogram = accumulator.accumulate_histogram(
                &metric.name,
                &labels,
                point.start_time_unix_nano,
                native_histogram,
            )?;
        } else if histogram.aggregation_temporality != AggregationTemporality::Cumulative as i32
            && histogram.aggregation_temporality != AggregationTemporality::Unspecified as i32
        {
            return Err(OtlpError::DeltaUnsupported(metric.name.clone()));
        }
        out.push(DecodedSeries {
            labels,
            samples: Vec::new(),
            histograms: vec![(nanos_to_millis(point.time_unix_nano), native_histogram)],
            exemplars: exemplars_from_exponential_histogram_point(point),
            metadata: Some(metadata.clone()),
        });
    }
    Ok(out)
}

fn summary_series(
    metric: &Metric,
    summary: &Summary,
    resource_attributes: &[KeyValue],
    strategy: TranslationStrategy,
) -> Vec<DecodedSeries> {
    let name = translated_metric_name(metric, strategy, false);
    let metadata = metric_metadata(metric, &name, "summary");
    let mut out = Vec::new();
    for point in &summary.data_points {
        out.extend(summary_point_series(
            &name,
            point,
            resource_attributes,
            Some(metadata.clone()),
        ));
    }
    out
}

fn summary_point_series(
    name: &str,
    point: &SummaryDataPoint,
    resource_attributes: &[KeyValue],
    metadata: Option<DecodedMetadata>,
) -> Vec<DecodedSeries> {
    let timestamp = nanos_to_millis(point.time_unix_nano);
    let mut out = Vec::new();
    for quantile in &point.quantile_values {
        let quantile_value = quantile.quantile.to_string();
        out.push(DecodedSeries {
            labels: labels(
                name,
                resource_attributes,
                &point.attributes,
                Some(("quantile", &quantile_value)),
            ),
            samples: vec![DecodedSample::with_start_timestamp(
                timestamp,
                quantile.value,
                (point.start_time_unix_nano != 0)
                    .then_some(nanos_to_millis(point.start_time_unix_nano)),
            )],
            histograms: Vec::new(),
            exemplars: Vec::new(),
            metadata: metadata.clone(),
        });
    }
    out.push(DecodedSeries {
        labels: labels(
            &format!("{name}_count"),
            resource_attributes,
            &point.attributes,
            None,
        ),
        samples: vec![DecodedSample::with_start_timestamp(
            timestamp,
            point.count as f64,
            (point.start_time_unix_nano != 0)
                .then_some(nanos_to_millis(point.start_time_unix_nano)),
        )],
        histograms: Vec::new(),
        exemplars: Vec::new(),
        metadata: metadata.clone(),
    });
    out.push(DecodedSeries {
        labels: labels(
            &format!("{name}_sum"),
            resource_attributes,
            &point.attributes,
            None,
        ),
        samples: vec![DecodedSample::with_start_timestamp(
            timestamp,
            point.sum,
            (point.start_time_unix_nano != 0)
                .then_some(nanos_to_millis(point.start_time_unix_nano)),
        )],
        histograms: Vec::new(),
        exemplars: Vec::new(),
        metadata,
    });
    out
}

fn metric_metadata(
    metric: &Metric,
    metric_family_name: &str,
    metric_type: &str,
) -> DecodedMetadata {
    DecodedMetadata {
        metric_family_name: metric_family_name.to_string(),
        metric_type: metric_type.to_string(),
        help: metric.description.clone(),
        unit: metric.unit.clone(),
    }
}

fn labels(
    name: &str,
    resource_attributes: &[KeyValue],
    point_attributes: &[KeyValue],
    extra: Option<(&str, &str)>,
) -> Labels {
    let mut labels = Labels::new();
    labels.insert("__name__", name);
    insert_attributes(&mut labels, resource_attributes);
    insert_attributes(&mut labels, point_attributes);
    if let Some((name, value)) = extra {
        labels.insert(name, value);
    }
    labels
}

fn insert_attributes(labels: &mut Labels, attributes: &[KeyValue]) {
    for attribute in attributes {
        if let Some(value) = attribute_value(attribute.value.as_ref()) {
            labels.insert(
                normalize_name(&attribute.key, TranslationStrategy::default()),
                value,
            );
        }
    }
}

fn attribute_value(value: Option<&AnyValue>) -> Option<String> {
    match value?.value.as_ref()? {
        any_value::Value::StringValue(value) => Some(value.clone()),
        any_value::Value::BoolValue(value) => Some(value.to_string()),
        any_value::Value::IntValue(value) => Some(value.to_string()),
        any_value::Value::DoubleValue(value) => Some(value.to_string()),
        any_value::Value::BytesValue(value) => Some(format!("{value:x?}")),
        any_value::Value::ArrayValue(_)
        | any_value::Value::KvlistValue(_)
        | any_value::Value::StringValueStrindex(_) => None,
    }
}

fn number_value(point: &NumberDataPoint) -> Option<f64> {
    match point.value {
        Some(number_data_point::Value::AsDouble(value)) => Some(value),
        Some(number_data_point::Value::AsInt(value)) => Some(value as f64),
        None => None,
    }
}

fn exemplars_from_number_point(point: &NumberDataPoint) -> Vec<DecodedExemplar> {
    exemplars_from_otlp(&point.exemplars)
}

fn exemplars_from_histogram_point(point: &HistogramDataPoint) -> Vec<DecodedExemplar> {
    exemplars_from_otlp(&point.exemplars)
}

fn exemplars_from_exponential_histogram_point(
    point: &ExponentialHistogramDataPoint,
) -> Vec<DecodedExemplar> {
    exemplars_from_otlp(&point.exemplars)
}

fn exemplars_from_otlp(exemplars: &[OtlpExemplar]) -> Vec<DecodedExemplar> {
    exemplars.iter().filter_map(exemplar).collect()
}

fn exemplars_for_bucket(
    exemplars: &[DecodedExemplar],
    point: &HistogramDataPoint,
    bucket_idx: usize,
) -> Vec<DecodedExemplar> {
    exemplars
        .iter()
        .filter(|exemplar| exemplar_belongs_to_bucket(exemplar.value, point, bucket_idx))
        .cloned()
        .collect()
}

fn exemplar_belongs_to_bucket(value: f64, point: &HistogramDataPoint, bucket_idx: usize) -> bool {
    let lower_ok = bucket_idx
        .checked_sub(1)
        .and_then(|lower_idx| point.explicit_bounds.get(lower_idx))
        .is_none_or(|lower| value > *lower);
    let upper_ok = point
        .explicit_bounds
        .get(bucket_idx)
        .is_none_or(|upper| value <= *upper);
    lower_ok && upper_ok
}

fn exemplar(exemplar: &OtlpExemplar) -> Option<DecodedExemplar> {
    let value = match exemplar.value {
        Some(otlp_exemplar::Value::AsDouble(value)) => value,
        Some(otlp_exemplar::Value::AsInt(value)) => value as f64,
        None => return None,
    };
    let mut labels = Labels::new();
    insert_attributes(&mut labels, &exemplar.filtered_attributes);
    if !exemplar.trace_id.is_empty() {
        labels.insert("trace_id", bytes_to_hex(&exemplar.trace_id));
    }
    if !exemplar.span_id.is_empty() {
        labels.insert("span_id", bytes_to_hex(&exemplar.span_id));
    }
    Some(DecodedExemplar {
        labels,
        timestamp_ms: nanos_to_millis(exemplar.time_unix_nano),
        value,
    })
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[usize::from(byte >> 4)] as char);
        out.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    out
}

fn nanos_to_millis(nanos: u64) -> i64 {
    i64::try_from(nanos / 1_000_000).unwrap_or(i64::MAX)
}

fn downscaled_spans(
    buckets: Option<&exponential_histogram_data_point::Buckets>,
    source_schema: i32,
    target_schema: i32,
) -> Result<(Vec<BucketSpan>, Vec<f64>), OtlpError> {
    let Some(buckets) = buckets else {
        return Ok((Vec::new(), Vec::new()));
    };
    let shift = u32::try_from(source_schema - target_schema)
        .map_err(|_| OtlpError::Invalid("exponential histogram".into(), "invalid scale".into()))?;
    let divisor = 1_i32.checked_shl(shift).ok_or_else(|| {
        OtlpError::Invalid(
            "exponential histogram".into(),
            format!("scale {source_schema} cannot be downscaled to schema {target_schema}"),
        )
    })?;
    let mut merged = BTreeMap::<i32, u64>::new();
    for (idx, count) in buckets.bucket_counts.iter().enumerate() {
        let idx = i32::try_from(idx).map_err(|_| {
            OtlpError::Invalid("exponential histogram".into(), "too many buckets".into())
        })?;
        let original_offset = buckets.offset.checked_add(idx).ok_or_else(|| {
            OtlpError::Invalid(
                "exponential histogram".into(),
                "bucket offset overflow".into(),
            )
        })?;
        let offset = original_offset
            .div_euclid(divisor)
            .checked_add(1)
            .ok_or_else(|| {
                OtlpError::Invalid(
                    "exponential histogram".into(),
                    "bucket offset overflow".into(),
                )
            })?;
        let merged_count = merged.entry(offset).or_default();
        if target_schema < source_schema && *merged_count != 0 && *count != 0 {
            return Err(OtlpError::Invalid(
                "exponential histogram".into(),
                format!(
                    "scale {source_schema} cannot be downscaled to schema {target_schema} without lossy downscale"
                ),
            ));
        }
        *merged_count += count;
    }

    Ok(spans_from_buckets(merged))
}

fn spans_from_buckets(buckets: BTreeMap<i32, u64>) -> (Vec<BucketSpan>, Vec<f64>) {
    let mut spans = Vec::new();
    let mut counts = Vec::new();
    let mut current_span_start = None::<i32>;
    let mut previous_offset = None::<i32>;

    for (offset, count) in buckets {
        match (current_span_start, previous_offset) {
            (Some(_), Some(previous)) if offset == previous + 1 => {}
            (Some(start), Some(previous)) => spans.push(BucketSpan {
                offset: start,
                length: u32::try_from(previous - start + 1).expect("span length fits u32"),
            }),
            _ => {}
        }
        if previous_offset.is_none_or(|previous| offset != previous + 1) {
            current_span_start = Some(offset);
        }
        previous_offset = Some(offset);
        counts.push(count as f64);
    }

    if let (Some(start), Some(previous)) = (current_span_start, previous_offset) {
        spans.push(BucketSpan {
            offset: start,
            length: u32::try_from(previous - start + 1).expect("span length fits u32"),
        });
    }

    (spans, counts)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_blockstore::Labels;
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
        metrics::v1::{
            AggregationTemporality, Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint,
            Gauge, Histogram, HistogramDataPoint, Metric, MetricsData, NumberDataPoint,
            ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint,
            exemplar as otlp_exemplar, exponential_histogram_data_point, metric, number_data_point,
            summary_data_point,
        },
        resource::v1::Resource,
    };

    use super::{DeltaAccumulator, TranslationStrategy, decode_otlp, decode_otlp_stateful};
    use crate::{
        BucketSpan,
        wire::{DecodedMetadata, DecodedSample, DecodedSeries},
    };

    fn kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_string())),
            }),
            key_strindex: 0,
        }
    }

    fn number_point(value: f64, timestamp: u64, attributes: Vec<KeyValue>) -> NumberDataPoint {
        NumberDataPoint {
            attributes,
            time_unix_nano: timestamp,
            value: Some(number_data_point::Value::AsDouble(value)),
            ..Default::default()
        }
    }

    fn metrics_data(metric: Metric) -> MetricsData {
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![metric],
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn metrics_data_with_resource(
        metric: Metric,
        resource_attributes: Vec<KeyValue>,
    ) -> MetricsData {
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: resource_attributes,
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![metric],
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        let mut labels = Labels::new();
        for (name, value) in pairs {
            labels.insert((*name).to_string(), (*value).to_string());
        }
        labels
    }

    fn sample_value(
        series: &[crate::wire::DecodedSeries],
        name: &str,
        le: Option<&str>,
    ) -> Option<f64> {
        series
            .iter()
            .find(|series| {
                series.labels.get("__name__") == Some(name) && series.labels.get("le") == le
            })
            .and_then(|series| series.samples.first().map(|sample| sample.value))
    }

    #[test]
    fn gauge_datapoint_decodes_to_float_series() {
        let data = metrics_data(Metric {
            name: "system.cpu.utilization".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![number_point(
                    0.42,
                    1_000_000,
                    vec![kv("host.name", "api-1")],
                )],
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        assert2::assert!(
            series
                == vec![DecodedSeries {
                    labels: labels(&[
                        ("__name__", "system_cpu_utilization"),
                        ("host_name", "api-1")
                    ]),
                    samples: vec![DecodedSample {
                        timestamp_ms: 1,
                        value: 0.42,
                        start_timestamp_ms: None,
                    }],
                    histograms: Vec::new(),
                    exemplars: Vec::new(),
                    metadata: Some(DecodedMetadata {
                        metric_family_name: "system_cpu_utilization".into(),
                        metric_type: "gauge".into(),
                        help: String::new(),
                        unit: String::new(),
                    }),
                }]
        );
    }

    #[test]
    fn far_future_datapoint_is_rejected_not_clamped() {
        let data = metrics_data(Metric {
            name: "system.cpu.utilization".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![number_point(0.42, u64::MAX, Vec::new())],
            })),
            ..Default::default()
        });

        let err = decode_otlp(&data, TranslationStrategy::default()).unwrap_err();

        assert2::assert!(matches!(err, super::OtlpError::Invalid(_, _)));
        assert2::assert!(format!("{err}").contains("too far in the future"));
    }

    #[test]
    fn gauge_metric_decodes_metadata() {
        let data = metrics_data(Metric {
            name: "system.cpu.utilization".into(),
            description: "CPU utilization ratio.".into(),
            unit: "1".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![number_point(0.42, 1_000_000, Vec::new())],
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        let metadata = series[0].metadata.as_ref().expect("metric metadata");
        assert2::assert!(
            *metadata
                == DecodedMetadata {
                    metric_family_name: "system_cpu_utilization".into(),
                    metric_type: "gauge".into(),
                    help: "CPU utilization ratio.".into(),
                    unit: "1".into(),
                }
        );
    }

    #[test]
    fn gauge_datapoint_drops_exemplars() {
        let data = metrics_data(Metric {
            name: "system.cpu.utilization".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    time_unix_nano: 2_000_000,
                    value: Some(number_data_point::Value::AsDouble(0.42)),
                    exemplars: vec![Exemplar {
                        filtered_attributes: vec![kv("user.id", "alice")],
                        time_unix_nano: 1_500_000,
                        value: Some(otlp_exemplar::Value::AsDouble(0.9)),
                        span_id: vec![0xab, 0xcd],
                        trace_id: vec![0x01, 0x23, 0x45, 0x67],
                    }],
                    ..Default::default()
                }],
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        assert2::assert!(series.len() == 1);
        assert2::assert!(series[0].exemplars.is_empty());
    }

    #[test]
    fn monotonic_sum_gets_total_suffix() {
        let data = metrics_data(Metric {
            name: "http.server.requests".into(),
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![number_point(7.0, 2_000_000, Vec::new())],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        assert2::assert!(series[0].labels.get("__name__") == Some("http_server_requests_total"));
        assert2::assert!(series[0].samples.as_slice() == &[DecodedSample::new(2, 7.0)][..]);
    }

    #[test]
    fn default_translation_collapses_repeated_replacement_underscores() {
        let data = metrics_data(Metric {
            name: "http--server..requests".into(),
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![number_point(7.0, 2_000_000, Vec::new())],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        assert2::assert!(series[0].labels.get("__name__") == Some("http_server_requests_total"));
    }

    #[test]
    fn default_translation_adds_unit_suffix_before_total_suffix() {
        let data = metrics_data(Metric {
            name: "k8s.pod.cpu.time".into(),
            unit: "s".into(),
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![number_point(7.0, 2_000_000, Vec::new())],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        assert2::assert!(
            series[0].labels.get("__name__") == Some("k8s_pod_cpu_time_seconds_total")
        );
        assert2::assert!(
            series[0]
                .metadata
                .as_ref()
                .map(|metadata| metadata.metric_family_name.as_str())
                == Some("k8s_pod_cpu_time_seconds_total")
        );
    }

    #[test]
    fn default_translation_converts_rate_units_to_prometheus_suffixes() {
        let data = metrics_data(Metric {
            name: "network.io".into(),
            unit: "By/s".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![number_point(1024.0, 2_000_000, Vec::new())],
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        assert2::assert!(series[0].labels.get("__name__") == Some("network_io_bytes_per_second"));
    }

    #[test]
    fn default_translation_converts_meter_rate_unit_to_prometheus_suffix() {
        let data = metrics_data(Metric {
            name: "vehicle.speed".into(),
            unit: "m/s".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![number_point(12.5, 2_000_000, Vec::new())],
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        assert2::assert!(
            series[0].labels.get("__name__") == Some("vehicle_speed_meters_per_second")
        );
    }

    #[test]
    fn default_translation_drops_ucum_unit_annotations_before_suffix_conversion() {
        let data = metrics_data(Metric {
            name: "network.io".into(),
            unit: "By{packet}/s".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![number_point(1024.0, 2_000_000, Vec::new())],
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        assert2::assert!(series[0].labels.get("__name__") == Some("network_io_bytes_per_second"));
    }

    #[test]
    fn default_translation_converts_common_ucum_units_to_prometheus_suffixes() {
        for (metric_name, unit, expected_name) in [
            ("process.uptime", "min", "process_uptime_minutes"),
            ("process.uptime", "h", "process_uptime_hours"),
            ("process.uptime", "d", "process_uptime_days"),
            ("cache.size", "KiBy", "cache_size_kibibytes"),
            ("cache.size", "MiBy", "cache_size_mebibytes"),
            ("cache.size", "GiBy", "cache_size_gibibytes"),
            ("cache.size", "TiBy", "cache_size_tebibytes"),
            ("cache.size", "kBy", "cache_size_kilobytes"),
            ("cache.size", "MBy", "cache_size_megabytes"),
            ("cache.size", "GBy", "cache_size_gigabytes"),
            ("cache.size", "TBy", "cache_size_terabytes"),
            ("sensor.reading", "V", "sensor_reading_volts"),
            ("sensor.reading", "A", "sensor_reading_amperes"),
            ("sensor.reading", "J", "sensor_reading_joules"),
            ("sensor.reading", "W", "sensor_reading_watts"),
            ("sensor.reading", "g", "sensor_reading_grams"),
            ("cache.write", "MiBy/s", "cache_write_mebibytes_per_second"),
        ] {
            let data = metrics_data(Metric {
                name: metric_name.into(),
                unit: unit.into(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![number_point(1.0, 2_000_000, Vec::new())],
                })),
                ..Default::default()
            });

            let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

            assert2::assert!(series[0].labels.get("__name__") == Some(expected_name));
        }
    }

    #[test]
    fn delta_sum_accumulates_to_cumulative_samples() {
        let first = metrics_data(Metric {
            name: "http.server.requests".into(),
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![number_point(7.0, 2_000_000, vec![kv("route", "/v1")])],
                aggregation_temporality: AggregationTemporality::Delta as i32,
                is_monotonic: true,
            })),
            ..Default::default()
        });
        let second = metrics_data(Metric {
            name: "http.server.requests".into(),
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![number_point(5.0, 3_000_000, vec![kv("route", "/v1")])],
                aggregation_temporality: AggregationTemporality::Delta as i32,
                is_monotonic: true,
            })),
            ..Default::default()
        });
        let mut accumulator = DeltaAccumulator::default();

        let first_series =
            decode_otlp_stateful(&first, TranslationStrategy::default(), &mut accumulator).unwrap();
        let second_series =
            decode_otlp_stateful(&second, TranslationStrategy::default(), &mut accumulator)
                .unwrap();

        assert2::assert!(first_series[0].samples == vec![(2, 7.0)]);
        assert2::assert!(second_series[0].samples == vec![(3, 12.0)]);
    }

    #[test]
    fn stateless_decode_accumulates_delta_sums_within_one_payload() {
        let data = metrics_data(Metric {
            name: "http.server.requests".into(),
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![
                    NumberDataPoint {
                        attributes: vec![kv("route", "/v1")],
                        start_time_unix_nano: 1_000_000,
                        time_unix_nano: 2_000_000,
                        value: Some(number_data_point::Value::AsDouble(7.0)),
                        ..Default::default()
                    },
                    NumberDataPoint {
                        attributes: vec![kv("route", "/v1")],
                        start_time_unix_nano: 1_000_000,
                        time_unix_nano: 3_000_000,
                        value: Some(number_data_point::Value::AsDouble(5.0)),
                        ..Default::default()
                    },
                ],
                aggregation_temporality: AggregationTemporality::Delta as i32,
                is_monotonic: true,
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        let expected_labels =
            labels(&[("__name__", "http_server_requests_total"), ("route", "/v1")]);
        let expected_metadata = Some(DecodedMetadata {
            metric_family_name: "http_server_requests_total".into(),
            metric_type: "counter".into(),
            help: String::new(),
            unit: String::new(),
        });
        assert2::assert!(
            series
                == vec![
                    DecodedSeries {
                        labels: expected_labels.clone(),
                        samples: vec![DecodedSample {
                            timestamp_ms: 2,
                            value: 7.0,
                            start_timestamp_ms: Some(1),
                        }],
                        histograms: Vec::new(),
                        exemplars: Vec::new(),
                        metadata: expected_metadata.clone(),
                    },
                    DecodedSeries {
                        labels: expected_labels,
                        samples: vec![DecodedSample {
                            timestamp_ms: 3,
                            value: 12.0,
                            start_timestamp_ms: Some(1),
                        }],
                        histograms: Vec::new(),
                        exemplars: Vec::new(),
                        metadata: expected_metadata,
                    },
                ]
        );
    }

    #[test]
    fn histogram_decodes_exemplar_to_matching_bucket_series() {
        let data = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    time_unix_nano: 2_000_000,
                    count: 3,
                    sum: Some(1.7),
                    bucket_counts: vec![1, 1, 1],
                    explicit_bounds: vec![0.5, 1.0],
                    exemplars: vec![Exemplar {
                        filtered_attributes: vec![kv("http.route", "/v1")],
                        time_unix_nano: 1_500_000,
                        value: Some(otlp_exemplar::Value::AsDouble(0.9)),
                        span_id: vec![0xab, 0xcd],
                        trace_id: vec![0x01, 0x23, 0x45, 0x67],
                    }],
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        let matching_bucket = series
            .iter()
            .find(|series| {
                series.labels.get("__name__") == Some("rpc_server_duration_bucket")
                    && series.labels.get("le") == Some("1")
            })
            .expect("matching bucket series");
        let exemplar = &matching_bucket.exemplars[0];
        check!(
            (
                matching_bucket.exemplars.len(),
                exemplar.timestamp_ms,
                (exemplar.value - 0.9).abs() < f64::EPSILON,
                exemplar.labels.get("trace_id"),
                exemplar.labels.get("span_id"),
                exemplar.labels.get("http_route"),
            ) == (1, 1, true, Some("01234567"), Some("abcd"), Some("/v1"),)
        );

        for series in &series {
            if series.labels.get("le") != Some("1") {
                assert2::assert!(series.exemplars.is_empty());
            }
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn delta_histogram_accumulates_to_cumulative_classic_series() {
        let first = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    attributes: vec![kv("route", "/v1")],
                    time_unix_nano: 2_000_000,
                    start_time_unix_nano: 1_000_000,
                    count: 5,
                    sum: Some(7.0),
                    bucket_counts: vec![1, 4],
                    explicit_bounds: vec![0.5],
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            })),
            ..Default::default()
        });
        let second = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    attributes: vec![kv("route", "/v1")],
                    time_unix_nano: 3_000_000,
                    start_time_unix_nano: 1_000_000,
                    count: 3,
                    sum: Some(4.0),
                    bucket_counts: vec![2, 1],
                    explicit_bounds: vec![0.5],
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            })),
            ..Default::default()
        });
        let mut accumulator = DeltaAccumulator::default();

        let first_series =
            decode_otlp_stateful(&first, TranslationStrategy::default(), &mut accumulator).unwrap();
        let second_series =
            decode_otlp_stateful(&second, TranslationStrategy::default(), &mut accumulator)
                .unwrap();

        let cases = [
            (
                "first payload",
                &first_series,
                "rpc_server_duration_bucket",
                Some("0.5"),
                1.0,
            ),
            (
                "first payload",
                &first_series,
                "rpc_server_duration_bucket",
                Some("+Inf"),
                5.0,
            ),
            (
                "first payload",
                &first_series,
                "rpc_server_duration_count",
                None,
                5.0,
            ),
            (
                "first payload",
                &first_series,
                "rpc_server_duration_sum",
                None,
                7.0,
            ),
            (
                "second payload",
                &second_series,
                "rpc_server_duration_bucket",
                Some("0.5"),
                3.0,
            ),
            (
                "second payload",
                &second_series,
                "rpc_server_duration_bucket",
                Some("+Inf"),
                8.0,
            ),
            (
                "second payload",
                &second_series,
                "rpc_server_duration_count",
                None,
                8.0,
            ),
            (
                "second payload",
                &second_series,
                "rpc_server_duration_sum",
                None,
                11.0,
            ),
        ];
        for (_case, series, name, le, expected) in cases {
            assert2::assert!(sample_value(series, name, le) == Some(expected));
        }
    }

    #[test]
    fn resource_attributes_emit_target_info_series() {
        let data = metrics_data_with_resource(
            Metric {
                name: "system.cpu.utilization".into(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![number_point(
                        0.42,
                        1_000_000,
                        vec![kv("host.name", "api-1")],
                    )],
                })),
                ..Default::default()
            },
            vec![
                kv("service.name", "checkout"),
                kv("telemetry.sdk.language", "rust"),
            ],
        );

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        let target = series
            .iter()
            .find(|series| series.labels.get("__name__") == Some("target_info"))
            .expect("target_info series");
        assert2::assert!(
            &target.labels
                == &labels(&[
                    ("__name__", "target_info"),
                    ("service_name", "checkout"),
                    ("telemetry_sdk_language", "rust"),
                ])
        );
        assert2::assert!(&target.samples == &vec![DecodedSample::new(1, 1.0)]);
    }

    #[test]
    fn scope_metadata_is_added_to_metric_series_labels() {
        let data = MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", "checkout")],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "io.opentelemetry.http".into(),
                        version: "1.2.3".into(),
                        attributes: vec![kv("library.language", "rust")],
                        dropped_attributes_count: 0,
                    }),
                    metrics: vec![Metric {
                        name: "http.server.active_requests".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![number_point(3.0, 2_000_000, Vec::new())],
                        })),
                        ..Default::default()
                    }],
                    schema_url: "https://opentelemetry.io/schemas/1.24.0".into(),
                }],
                schema_url: String::new(),
            }],
        };

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        let metric = series
            .iter()
            .find(|series| series.labels.get("__name__") == Some("http_server_active_requests"))
            .expect("metric series");
        assert2::assert!(
            metric.labels
                == labels(&[
                    ("__name__", "http_server_active_requests"),
                    ("otel_scope_library_language", "rust"),
                    ("otel_scope_name", "io.opentelemetry.http"),
                    (
                        "otel_scope_schema_url",
                        "https://opentelemetry.io/schemas/1.24.0"
                    ),
                    ("otel_scope_version", "1.2.3"),
                    ("service_name", "checkout"),
                ])
        );

        let target = series
            .iter()
            .find(|series| series.labels.get("__name__") == Some("target_info"))
            .expect("target_info series");
        assert2::assert!(target.labels.get("otel_scope_name").is_none());
    }

    #[test]
    fn summary_decodes_to_quantile_sum_and_count_series() {
        let data = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::Summary(Summary {
                data_points: vec![SummaryDataPoint {
                    attributes: vec![kv("route", "/v1")],
                    time_unix_nano: 4_000_000,
                    count: 9,
                    sum: 12.5,
                    quantile_values: vec![
                        summary_data_point::ValueAtQuantile {
                            quantile: 0.5,
                            value: 2.0,
                        },
                        summary_data_point::ValueAtQuantile {
                            quantile: 0.9,
                            value: 4.0,
                        },
                    ],
                    ..Default::default()
                }],
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        let expected_metadata = Some(DecodedMetadata {
            metric_family_name: "rpc_server_duration".into(),
            metric_type: "summary".into(),
            help: String::new(),
            unit: String::new(),
        });
        assert2::assert!(
            series
                == vec![
                    DecodedSeries {
                        labels: labels(&[
                            ("__name__", "rpc_server_duration"),
                            ("quantile", "0.5"),
                            ("route", "/v1")
                        ]),
                        samples: vec![DecodedSample {
                            timestamp_ms: 4,
                            value: 2.0,
                            start_timestamp_ms: None,
                        }],
                        histograms: Vec::new(),
                        exemplars: Vec::new(),
                        metadata: expected_metadata.clone(),
                    },
                    DecodedSeries {
                        labels: labels(&[
                            ("__name__", "rpc_server_duration"),
                            ("quantile", "0.9"),
                            ("route", "/v1")
                        ]),
                        samples: vec![DecodedSample {
                            timestamp_ms: 4,
                            value: 4.0,
                            start_timestamp_ms: None,
                        }],
                        histograms: Vec::new(),
                        exemplars: Vec::new(),
                        metadata: expected_metadata.clone(),
                    },
                    DecodedSeries {
                        labels: labels(&[
                            ("__name__", "rpc_server_duration_count"),
                            ("route", "/v1")
                        ]),
                        samples: vec![DecodedSample {
                            timestamp_ms: 4,
                            value: 9.0,
                            start_timestamp_ms: None,
                        }],
                        histograms: Vec::new(),
                        exemplars: Vec::new(),
                        metadata: expected_metadata.clone(),
                    },
                    DecodedSeries {
                        labels: labels(&[
                            ("__name__", "rpc_server_duration_sum"),
                            ("route", "/v1")
                        ]),
                        samples: vec![DecodedSample {
                            timestamp_ms: 4,
                            value: 12.5,
                            start_timestamp_ms: None,
                        }],
                        histograms: Vec::new(),
                        exemplars: Vec::new(),
                        metadata: expected_metadata,
                    },
                ]
        );
    }

    #[test]
    fn exponential_histogram_decodes_to_native_histogram() {
        let data = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 3_000_000,
                    start_time_unix_nano: 1_000_000,
                    count: 6,
                    sum: Some(12.0),
                    scale: 3,
                    zero_count: 1,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: -1,
                        bucket_counts: vec![2, 3],
                    }),
                    negative: Some(exponential_histogram_data_point::Buckets {
                        offset: 4,
                        bucket_counts: vec![1],
                    }),
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        let (timestamp_ms, hist) = &series[0].histograms[0];
        check!(
            (
                series.len(),
                series[0].labels.get("__name__"),
                series[0].histograms.len(),
                (
                    *timestamp_ms,
                    hist.schema,
                    (hist.count - 6.0).abs() < f64::EPSILON,
                    (hist.sum - 12.0).abs() < f64::EPSILON,
                    (hist.zero_count - 1.0).abs() < f64::EPSILON,
                    hist.positive_spans[0].offset,
                    hist.positive_counts.as_slice() == [2.0, 3.0],
                    hist.negative_spans[0].offset,
                    hist.negative_counts.as_slice() == [1.0],
                    hist.start_timestamp_ms,
                ),
            ) == (
                1,
                Some("rpc_server_duration"),
                1,
                (3, 3, true, true, true, 0, true, 5, true, Some(1)),
            )
        );
    }

    #[test]
    fn exponential_histogram_decodes_exemplar_trace_context_and_filtered_attributes() {
        let data = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 3_000_000,
                    count: 2,
                    sum: Some(5.0),
                    scale: 1,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: 0,
                        bucket_counts: vec![2],
                    }),
                    exemplars: vec![Exemplar {
                        filtered_attributes: vec![kv("span.kind", "server")],
                        time_unix_nano: 2_500_000,
                        value: Some(otlp_exemplar::Value::AsDouble(2.5)),
                        span_id: vec![0xab, 0xcd],
                        trace_id: vec![0x01, 0x23, 0x45, 0x67],
                    }],
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        let exemplar = &series[0].exemplars[0];
        check!(
            (
                series.len(),
                series[0].exemplars.len(),
                exemplar.timestamp_ms,
                (exemplar.value - 2.5).abs() < f64::EPSILON,
                exemplar.labels.get("trace_id"),
                exemplar.labels.get("span_id"),
                exemplar.labels.get("span_kind"),
            ) == (
                1,
                1,
                2,
                true,
                Some("01234567"),
                Some("abcd"),
                Some("server"),
            )
        );
    }

    #[test]
    fn delta_exponential_histogram_accumulates_to_cumulative_native_histogram() {
        let first = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 2_000_000,
                    start_time_unix_nano: 1_000_000,
                    count: 4,
                    sum: Some(6.0),
                    scale: 1,
                    zero_count: 1,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: 0,
                        bucket_counts: vec![2, 1],
                    }),
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            })),
            ..Default::default()
        });
        let second = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 3_000_000,
                    start_time_unix_nano: 1_000_000,
                    count: 3,
                    sum: Some(5.0),
                    scale: 1,
                    zero_count: 2,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: 0,
                        bucket_counts: vec![1, 2],
                    }),
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            })),
            ..Default::default()
        });
        let mut accumulator = DeltaAccumulator::default();

        let first_series =
            decode_otlp_stateful(&first, TranslationStrategy::default(), &mut accumulator).unwrap();
        let second_series =
            decode_otlp_stateful(&second, TranslationStrategy::default(), &mut accumulator)
                .unwrap();

        let first_hist = &first_series[0].histograms[0].1;
        let second_hist = &second_series[0].histograms[0].1;
        check!(
            (
                (
                    (first_hist.count - 4.0).abs() < f64::EPSILON,
                    (first_hist.sum - 6.0).abs() < f64::EPSILON,
                    (first_hist.zero_count - 1.0).abs() < f64::EPSILON,
                    first_hist.positive_counts.as_slice(),
                ),
                (
                    (second_hist.count - 7.0).abs() < f64::EPSILON,
                    (second_hist.sum - 11.0).abs() < f64::EPSILON,
                    (second_hist.zero_count - 3.0).abs() < f64::EPSILON,
                    second_hist.positive_counts.as_slice(),
                    second_hist.start_timestamp_ms,
                ),
            ) == (
                (true, true, true, &[2.0, 1.0][..]),
                (true, true, true, &[3.0, 3.0][..], Some(1)),
            )
        );
    }

    #[test]
    fn delta_exponential_histogram_accumulates_different_span_layouts() {
        let first = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 2_000_000,
                    start_time_unix_nano: 1_000_000,
                    count: 2,
                    sum: Some(3.0),
                    scale: 1,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: 0,
                        bucket_counts: vec![2],
                    }),
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            })),
            ..Default::default()
        });
        let second = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 3_000_000,
                    start_time_unix_nano: 1_000_000,
                    count: 3,
                    sum: Some(5.0),
                    scale: 1,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: 1,
                        bucket_counts: vec![3],
                    }),
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            })),
            ..Default::default()
        });
        let mut accumulator = DeltaAccumulator::default();

        decode_otlp_stateful(&first, TranslationStrategy::default(), &mut accumulator).unwrap();
        let second_series =
            decode_otlp_stateful(&second, TranslationStrategy::default(), &mut accumulator)
                .unwrap();

        let second_hist = &second_series[0].histograms[0].1;
        check!((second_hist.count - 5.0).abs() < f64::EPSILON);
        check!((second_hist.sum - 8.0).abs() < f64::EPSILON);
        assert2::assert!(
            &second_hist.positive_spans
                == &vec![BucketSpan {
                    offset: 1,
                    length: 2
                }]
        );
        assert2::assert!(&second_hist.positive_counts == &vec![2.0, 3.0]);
    }

    #[test]
    fn exponential_histogram_shifts_otlp_lower_boundary_indexes_to_native_upper_boundary_indexes() {
        let data = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    count: 2,
                    scale: 2,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: 0,
                        bucket_counts: vec![1],
                    }),
                    negative: Some(exponential_histogram_data_point::Buckets {
                        offset: 3,
                        bucket_counts: vec![1],
                    }),
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();
        let hist = &series[0].histograms[0].1;

        assert2::assert!(hist.positive_spans[0].offset == 1);
        assert2::assert!(hist.negative_spans[0].offset == 4);
    }

    #[test]
    fn exponential_histogram_rejects_scale_below_native_schema_range() {
        let data = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    count: 1,
                    scale: -5,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: 0,
                        bucket_counts: vec![1],
                    }),
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        });

        let err = decode_otlp(&data, TranslationStrategy::default()).unwrap_err();

        assert2::assert!(matches!(err, super::OtlpError::Invalid(_, _)));
        assert2::assert!(format!("{err}").contains("scale -5"));
    }

    #[test]
    fn exponential_histogram_rejects_unrepresentable_downscale() {
        let data = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    count: 1,
                    scale: 40,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: 0,
                        bucket_counts: vec![1],
                    }),
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        });

        let err = decode_otlp(&data, TranslationStrategy::default()).unwrap_err();

        assert2::assert!(matches!(err, super::OtlpError::Invalid(_, _)));
        assert2::assert!(format!("{err}").contains("scale 40"));
    }

    #[test]
    fn exponential_histogram_rejects_lossy_downscale() {
        let data = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    count: 3,
                    scale: 9,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: 0,
                        bucket_counts: vec![1, 2],
                    }),
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        });

        let err = decode_otlp(&data, TranslationStrategy::default()).unwrap_err();

        assert2::assert!(matches!(err, super::OtlpError::Invalid(_, _)));
        assert2::assert!(format!("{err}").contains("lossy downscale"));
    }
}
