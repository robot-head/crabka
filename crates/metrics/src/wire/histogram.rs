//! Decode `remote_write` histogram samples into absolute native histograms.

#![allow(
    clippy::cast_precision_loss,
    reason = "Prometheus remote_write histograms expose integer counts that the query model stores as f64, matching Prometheus' own histogram math domain."
)]

use super::{WireError, pb};
use crate::{BucketSpan, NativeHistogram, ResetHint};

pub fn v1_histogram_to_native(histogram: &pb::v1::Histogram) -> Result<NativeHistogram, WireError> {
    let schema = schema_i8(histogram.schema)?;
    let positive_spans = v1_spans(&histogram.positive_spans);
    let positive_counts = counts(&histogram.positive_counts, &histogram.positive_deltas);
    let negative_spans = v1_spans(&histogram.negative_spans);
    let negative_counts = counts(&histogram.negative_counts, &histogram.negative_deltas);
    let custom_values =
        (!histogram.custom_values.is_empty()).then(|| histogram.custom_values.clone());
    validate_spans_and_counts(
        schema,
        &positive_spans,
        &positive_counts,
        &negative_spans,
        &negative_counts,
        custom_values.as_deref(),
    )?;

    Ok(NativeHistogram {
        schema,
        is_float: is_v1_float(histogram),
        reset_hint: v1_reset_hint(histogram.reset_hint),
        zero_threshold: histogram.zero_threshold,
        zero_count: v1_zero_count(histogram),
        count: v1_count(histogram),
        sum: histogram.sum,
        positive_spans,
        positive_counts,
        negative_spans,
        negative_counts,
        custom_values,
        start_timestamp_ms: None,
    })
}

pub fn v2_histogram_to_native(histogram: &pb::v2::Histogram) -> Result<NativeHistogram, WireError> {
    let schema = schema_i8(histogram.schema)?;
    let positive_spans = v2_spans(&histogram.positive_spans);
    let positive_counts = counts(&histogram.positive_counts, &histogram.positive_deltas);
    let negative_spans = v2_spans(&histogram.negative_spans);
    let negative_counts = counts(&histogram.negative_counts, &histogram.negative_deltas);
    let custom_values =
        (!histogram.custom_values.is_empty()).then(|| histogram.custom_values.clone());
    validate_spans_and_counts(
        schema,
        &positive_spans,
        &positive_counts,
        &negative_spans,
        &negative_counts,
        custom_values.as_deref(),
    )?;

    Ok(NativeHistogram {
        schema,
        is_float: is_v2_float(histogram),
        reset_hint: v2_reset_hint(histogram.reset_hint),
        zero_threshold: histogram.zero_threshold,
        zero_count: v2_zero_count(histogram),
        count: v2_count(histogram),
        sum: histogram.sum,
        positive_spans,
        positive_counts,
        negative_spans,
        negative_counts,
        custom_values,
        start_timestamp_ms: (histogram.start_timestamp != 0).then_some(histogram.start_timestamp),
    })
}

/// Strict span/count validation matching Prometheus' appender, applied at the
/// wire edge before a histogram is admitted.
///
/// For both the positive and negative buckets the sum of the span lengths must
/// equal the number of decoded counts (Prometheus `Histogram.Validate` /
/// `FloatHistogram.Validate`). For NHCB (schema `-53`, custom buckets) the
/// histogram must carry no negative buckets, and `custom_values` must define an
/// upper bound for every populated positive bucket.
fn validate_spans_and_counts(
    schema: i8,
    positive_spans: &[BucketSpan],
    positive_counts: &[f64],
    negative_spans: &[BucketSpan],
    negative_counts: &[f64],
    custom_values: Option<&[f64]>,
) -> Result<(), WireError> {
    check_side("positive", positive_spans, positive_counts.len())?;
    check_side("negative", negative_spans, negative_counts.len())?;

    if schema == -53 {
        // NHCB: custom buckets are exclusively positive; the boundaries in
        // `custom_values` must cover every populated positive bucket.
        if !negative_spans.is_empty() || !negative_counts.is_empty() {
            return Err(WireError::Invalid(
                "custom-bucket histogram must not carry negative buckets".to_string(),
            ));
        }
        let buckets = span_bucket_total(positive_spans);
        let bounds = custom_values.map_or(0, <[f64]>::len);
        if buckets > bounds {
            return Err(WireError::Invalid(format!(
                "custom-bucket histogram has {buckets} populated buckets but only {bounds} custom values"
            )));
        }
    }

    Ok(())
}

fn check_side(side: &str, spans: &[BucketSpan], counts: usize) -> Result<(), WireError> {
    let expected = span_bucket_total(spans);
    if expected != counts {
        return Err(WireError::Invalid(format!(
            "{side} spans declare {expected} buckets but {counts} counts were decoded"
        )));
    }
    Ok(())
}

fn span_bucket_total(spans: &[BucketSpan]) -> usize {
    spans.iter().map(|span| span.length as usize).sum()
}

fn schema_i8(schema: i32) -> Result<i8, WireError> {
    if schema == -53 || (-4..=8).contains(&schema) {
        i8::try_from(schema)
            .map_err(|_| WireError::Invalid(format!("histogram schema {schema} out of range")))
    } else {
        Err(WireError::Invalid(format!(
            "histogram schema {schema} is not supported"
        )))
    }
}

fn v1_count(histogram: &pb::v1::Histogram) -> f64 {
    use pb::v1::histogram::Count;

    match histogram.count {
        Some(Count::CountInt(value)) => value as f64,
        Some(Count::CountFloat(value)) => value,
        None => 0.0,
    }
}

fn v2_count(histogram: &pb::v2::Histogram) -> f64 {
    use pb::v2::histogram::Count;

    match histogram.count {
        Some(Count::CountInt(value)) => value as f64,
        Some(Count::CountFloat(value)) => value,
        None => 0.0,
    }
}

fn v1_zero_count(histogram: &pb::v1::Histogram) -> f64 {
    use pb::v1::histogram::ZeroCount;

    match histogram.zero_count {
        Some(ZeroCount::ZeroCountInt(value)) => value as f64,
        Some(ZeroCount::ZeroCountFloat(value)) => value,
        None => 0.0,
    }
}

fn v2_zero_count(histogram: &pb::v2::Histogram) -> f64 {
    use pb::v2::histogram::ZeroCount;

    match histogram.zero_count {
        Some(ZeroCount::ZeroCountInt(value)) => value as f64,
        Some(ZeroCount::ZeroCountFloat(value)) => value,
        None => 0.0,
    }
}

fn is_v1_float(histogram: &pb::v1::Histogram) -> bool {
    matches!(
        histogram.count,
        Some(pb::v1::histogram::Count::CountFloat(_))
    ) || matches!(
        histogram.zero_count,
        Some(pb::v1::histogram::ZeroCount::ZeroCountFloat(_))
    ) || !histogram.positive_counts.is_empty()
        || !histogram.negative_counts.is_empty()
}

fn is_v2_float(histogram: &pb::v2::Histogram) -> bool {
    matches!(
        histogram.count,
        Some(pb::v2::histogram::Count::CountFloat(_))
    ) || matches!(
        histogram.zero_count,
        Some(pb::v2::histogram::ZeroCount::ZeroCountFloat(_))
    ) || !histogram.positive_counts.is_empty()
        || !histogram.negative_counts.is_empty()
}

fn v1_reset_hint(value: i32) -> ResetHint {
    match pb::v1::histogram::ResetHint::try_from(value) {
        Ok(pb::v1::histogram::ResetHint::Yes) => ResetHint::Yes,
        Ok(pb::v1::histogram::ResetHint::No) => ResetHint::No,
        Ok(pb::v1::histogram::ResetHint::Gauge) => ResetHint::Gauge,
        Ok(pb::v1::histogram::ResetHint::Unknown) | Err(_) => ResetHint::Unknown,
    }
}

fn v2_reset_hint(value: i32) -> ResetHint {
    match pb::v2::histogram::ResetHint::try_from(value) {
        Ok(pb::v2::histogram::ResetHint::Yes) => ResetHint::Yes,
        Ok(pb::v2::histogram::ResetHint::No) => ResetHint::No,
        Ok(pb::v2::histogram::ResetHint::Gauge) => ResetHint::Gauge,
        Ok(pb::v2::histogram::ResetHint::Unspecified) | Err(_) => ResetHint::Unknown,
    }
}

fn v1_spans(spans: &[pb::v1::BucketSpan]) -> Vec<BucketSpan> {
    spans
        .iter()
        .map(|span| BucketSpan {
            offset: span.offset,
            length: span.length,
        })
        .collect()
}

fn v2_spans(spans: &[pb::v2::BucketSpan]) -> Vec<BucketSpan> {
    spans
        .iter()
        .map(|span| BucketSpan {
            offset: span.offset,
            length: span.length,
        })
        .collect()
}

fn counts(float_counts: &[f64], deltas: &[i64]) -> Vec<f64> {
    if !float_counts.is_empty() {
        return float_counts.to_vec();
    }

    let mut total = 0_i64;
    deltas
        .iter()
        .map(|delta| {
            total += delta;
            total as f64
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn v1_integer_histogram_delta_decodes_to_absolute_counts() {
        let histogram = pb::v1::Histogram {
            schema: 1,
            zero_threshold: 0.001,
            positive_spans: vec![pb::v1::BucketSpan {
                offset: 0,
                length: 3,
            }],
            positive_deltas: vec![4, -1, 3],
            negative_spans: vec![pb::v1::BucketSpan {
                offset: 0,
                length: 2,
            }],
            negative_deltas: vec![2, 1],
            count: Some(pb::v1::histogram::Count::CountInt(9)),
            zero_count: Some(pb::v1::histogram::ZeroCount::ZeroCountInt(1)),
            reset_hint: pb::v1::histogram::ResetHint::Yes as i32,
            timestamp: 42,
            ..Default::default()
        };

        let native = v1_histogram_to_native(&histogram).unwrap();

        check!(!native.is_float);
        check!((native.count - 9.0).abs() < f64::EPSILON);
        check!((native.zero_count - 1.0).abs() < f64::EPSILON);
        check!(native.positive_counts == vec![4.0, 3.0, 6.0]);
        check!(native.negative_counts == vec![2.0, 3.0]);
        check!(native.reset_hint == ResetHint::Yes);
    }

    #[test]
    fn v2_float_histogram_preserves_absolute_counts_and_start_timestamp() {
        let histogram = pb::v2::Histogram {
            schema: -53,
            positive_spans: vec![pb::v2::BucketSpan {
                offset: 0,
                length: 2,
            }],
            positive_counts: vec![1.5, 2.5],
            custom_values: vec![0.1, 0.2, 0.3],
            count: Some(pb::v2::histogram::Count::CountFloat(4.0)),
            zero_count: Some(pb::v2::histogram::ZeroCount::ZeroCountFloat(0.5)),
            reset_hint: pb::v2::histogram::ResetHint::Gauge as i32,
            start_timestamp: 7,
            ..Default::default()
        };

        let native = v2_histogram_to_native(&histogram).unwrap();

        check!(native.is_float);
        check!(native.is_nhcb());
        check!(native.positive_counts == vec![1.5, 2.5]);
        check!(native.custom_values == Some(vec![0.1, 0.2, 0.3]));
        check!(native.start_timestamp_ms == Some(7));
        check!(native.reset_hint == ResetHint::Gauge);
    }

    #[test]
    fn remote_write_histograms_reject_invalid_schemas() {
        for schema in [-54, -5, 9] {
            let v1 = pb::v1::Histogram {
                schema,
                ..Default::default()
            };
            let v2 = pb::v2::Histogram {
                schema,
                ..Default::default()
            };

            assert!(matches!(
                v1_histogram_to_native(&v1),
                Err(WireError::Invalid(_))
            ));
            assert!(matches!(
                v2_histogram_to_native(&v2),
                Err(WireError::Invalid(_))
            ));
        }
    }

    #[test]
    fn v1_histogram_rejects_span_count_mismatch() {
        // positive_spans claim 3 buckets, but only 2 deltas are supplied.
        let histogram = pb::v1::Histogram {
            schema: 1,
            positive_spans: vec![pb::v1::BucketSpan {
                offset: 0,
                length: 3,
            }],
            positive_deltas: vec![1, 2],
            count: Some(pb::v1::histogram::Count::CountInt(3)),
            ..Default::default()
        };

        let err = v1_histogram_to_native(&histogram).unwrap_err();

        assert!(matches!(err, WireError::Invalid(_)));
        assert!(format!("{err}").contains("positive spans declare 3 buckets but 2 counts"));
    }

    #[test]
    fn v2_histogram_rejects_negative_span_count_mismatch() {
        // negative_spans claim 1 bucket, but two float counts are supplied.
        let histogram = pb::v2::Histogram {
            schema: 0,
            negative_spans: vec![pb::v2::BucketSpan {
                offset: 0,
                length: 1,
            }],
            negative_counts: vec![1.0, 2.0],
            count: Some(pb::v2::histogram::Count::CountFloat(3.0)),
            ..Default::default()
        };

        let err = v2_histogram_to_native(&histogram).unwrap_err();

        assert!(matches!(err, WireError::Invalid(_)));
        assert!(format!("{err}").contains("negative spans declare 1 buckets but 2 counts"));
    }

    #[test]
    fn nhcb_histogram_rejects_too_few_custom_values() {
        // NHCB with 2 populated positive buckets but only 1 custom boundary.
        let histogram = pb::v2::Histogram {
            schema: -53,
            positive_spans: vec![pb::v2::BucketSpan {
                offset: 0,
                length: 2,
            }],
            positive_counts: vec![1.0, 2.0],
            custom_values: vec![0.5],
            count: Some(pb::v2::histogram::Count::CountFloat(3.0)),
            ..Default::default()
        };

        let err = v2_histogram_to_native(&histogram).unwrap_err();

        assert!(matches!(err, WireError::Invalid(_)));
        assert!(format!("{err}").contains("custom values"));
    }

    #[test]
    fn nhcb_histogram_rejects_negative_buckets() {
        let histogram = pb::v1::Histogram {
            schema: -53,
            positive_spans: vec![pb::v1::BucketSpan {
                offset: 0,
                length: 1,
            }],
            positive_counts: vec![1.0],
            negative_spans: vec![pb::v1::BucketSpan {
                offset: 0,
                length: 1,
            }],
            negative_counts: vec![1.0],
            custom_values: vec![0.5],
            count: Some(pb::v1::histogram::Count::CountFloat(2.0)),
            ..Default::default()
        };

        let err = v1_histogram_to_native(&histogram).unwrap_err();

        assert!(matches!(err, WireError::Invalid(_)));
        assert!(format!("{err}").contains("must not carry negative buckets"));
    }
}
