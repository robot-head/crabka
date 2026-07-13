//! `remote_write` v1 (`prometheus.WriteRequest`) request decoder.

use std::collections::HashSet;

use crabka_blockstore::Labels;
use prost::Message;

use super::{
    DecodedExemplar, DecodedMetadata, DecodedSample, DecodedSeries, WireError,
    histogram::v1_histogram_to_native, pb, snappy_block_decode,
};

/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn decode_v1(body: &[u8], max_decompressed: usize) -> Result<Vec<DecodedSeries>, WireError> {
    let raw = snappy_block_decode(body, max_decompressed)?;
    let req = pb::v1::WriteRequest::decode(raw.as_slice())
        .map_err(|error| WireError::ProtobufDecode(error.to_string()))?;

    let mut out = Vec::with_capacity(req.timeseries.len());
    for series in req.timeseries {
        let labels = labels_from_v1(&series.labels)?;
        let samples = series
            .samples
            .into_iter()
            .map(|sample| DecodedSample::new(sample.timestamp, sample.value))
            .collect();
        let histograms = series
            .histograms
            .iter()
            .map(|histogram| Ok((histogram.timestamp, v1_histogram_to_native(histogram)?)))
            .collect::<Result<Vec<_>, WireError>>()?;
        let exemplars = series
            .exemplars
            .iter()
            .map(|exemplar| {
                Ok(DecodedExemplar {
                    labels: labels_from_v1(&exemplar.labels)?,
                    timestamp_ms: exemplar.timestamp,
                    value: exemplar.value,
                })
            })
            .collect::<Result<Vec<_>, WireError>>()?;

        out.push(DecodedSeries {
            labels,
            samples,
            histograms,
            exemplars,
            metadata: None,
        });
    }

    for metadata in req.metadata {
        out.push(metadata_series_from_v1(metadata));
    }

    Ok(out)
}

fn labels_from_v1(labels: &[pb::v1::Label]) -> Result<Labels, WireError> {
    let mut names = HashSet::with_capacity(labels.len());
    labels
        .iter()
        .map(|label| {
            if !names.insert(label.name.as_str()) {
                return Err(WireError::Invalid(format!(
                    "duplicate label `{}`",
                    label.name
                )));
            }
            Ok((label.name.clone(), label.value.clone()))
        })
        .collect()
}

fn metadata_series_from_v1(metadata: pb::v1::MetricMetadata) -> DecodedSeries {
    let mut labels = Labels::new();
    labels.insert("__name__", metadata.metric_family_name.as_str());
    DecodedSeries {
        labels,
        samples: Vec::new(),
        histograms: Vec::new(),
        exemplars: Vec::new(),
        metadata: Some(DecodedMetadata {
            metric_family_name: metadata.metric_family_name,
            metric_type: metadata_type(metadata.r#type),
            help: metadata.help,
            unit: metadata.unit,
        }),
    }
}

fn metadata_type(value: i32) -> String {
    match pb::v1::metric_metadata::MetricType::try_from(value) {
        Ok(pb::v1::metric_metadata::MetricType::Counter) => "counter",
        Ok(pb::v1::metric_metadata::MetricType::Gauge) => "gauge",
        Ok(pb::v1::metric_metadata::MetricType::Histogram) => "histogram",
        Ok(pb::v1::metric_metadata::MetricType::Gaugehistogram) => "gaugehistogram",
        Ok(pb::v1::metric_metadata::MetricType::Summary) => "summary",
        Ok(pb::v1::metric_metadata::MetricType::Info) => "info",
        Ok(pb::v1::metric_metadata::MetricType::Stateset) => "stateset",
        Ok(pb::v1::metric_metadata::MetricType::Unknown) | Err(_) => "unknown",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use prost::Message;

    use super::*;

    fn snappy(body: &[u8]) -> Vec<u8> {
        snap::raw::Encoder::new().compress_vec(body).unwrap()
    }

    #[test]
    fn decodes_v1_samples_and_exemplars() {
        let req = pb::v1::WriteRequest {
            timeseries: vec![pb::v1::TimeSeries {
                labels: vec![pb::v1::Label {
                    name: "__name__".into(),
                    value: "up".into(),
                }],
                samples: vec![pb::v1::Sample {
                    value: 1.0,
                    timestamp: 1000,
                }],
                exemplars: vec![pb::v1::Exemplar {
                    labels: vec![pb::v1::Label {
                        name: "trace_id".into(),
                        value: "abc".into(),
                    }],
                    value: 2.0,
                    timestamp: 1100,
                }],
                histograms: Vec::new(),
            }],
            metadata: Vec::new(),
        };

        let decoded = decode_v1(&snappy(&req.encode_to_vec()), 1 << 20).unwrap();

        assert!(decoded.len() == 1);
        check!(decoded[0].labels.get("__name__") == Some("up"));
        check!(decoded[0].samples == vec![DecodedSample::new(1000, 1.0)]);
        check!(decoded[0].exemplars[0].labels.get("trace_id") == Some("abc"));
    }

    #[test]
    fn decodes_v1_histograms() {
        let req = pb::v1::WriteRequest {
            timeseries: vec![pb::v1::TimeSeries {
                histograms: vec![pb::v1::Histogram {
                    timestamp: 10,
                    positive_spans: vec![pb::v1::BucketSpan {
                        offset: 0,
                        length: 2,
                    }],
                    positive_deltas: vec![1, 2],
                    count: Some(pb::v1::histogram::Count::CountInt(3)),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let decoded = decode_v1(&snappy(&req.encode_to_vec()), 1 << 20).unwrap();

        assert!(decoded[0].histograms.len() == 1);
        check!(decoded[0].histograms[0].0 == 10);
        check!(decoded[0].histograms[0].1.positive_counts == vec![1.0, 3.0]);
    }

    #[test]
    fn decode_v1_rejects_duplicate_label_names() {
        let req = pb::v1::WriteRequest {
            timeseries: vec![pb::v1::TimeSeries {
                labels: vec![
                    pb::v1::Label {
                        name: "__name__".into(),
                        value: "up".into(),
                    },
                    pb::v1::Label {
                        name: "job".into(),
                        value: "api".into(),
                    },
                    pb::v1::Label {
                        name: "job".into(),
                        value: "worker".into(),
                    },
                ],
                samples: vec![pb::v1::Sample {
                    value: 1.0,
                    timestamp: 1000,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let err = decode_v1(&snappy(&req.encode_to_vec()), 1 << 20).unwrap_err();

        assert!(matches!(err, WireError::Invalid(_)));
        assert!(format!("{err}").contains("duplicate label `job`"));
    }
}
