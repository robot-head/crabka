//! `remote_write` v2 (`io.prometheus.write.v2.Request`) request decoder.

use crabka_blockstore::Labels;
use prost::Message;

use super::{
    DecodedExemplar, DecodedMetadata, DecodedSample, DecodedSeries, WireError,
    histogram::v2_histogram_to_native, pb, snappy_block_decode,
};
use crate::SymbolTable;

/// Written sample tallies for the v2 response headers.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WrittenCounts {
    pub samples: u64,
    pub histograms: u64,
    pub exemplars: u64,
}

pub fn decode_v2(
    body: &[u8],
    max_decompressed: usize,
) -> Result<(Vec<DecodedSeries>, WrittenCounts), WireError> {
    let raw = snappy_block_decode(body, max_decompressed)?;
    let req = pb::v2::Request::decode(raw.as_slice())
        .map_err(|error| WireError::ProtobufDecode(error.to_string()))?;
    let table = SymbolTable::from_symbols(req.symbols)
        .map_err(|error| WireError::Invalid(error.to_string()))?;

    let mut out = Vec::with_capacity(req.timeseries.len());
    let mut counts = WrittenCounts::default();
    for series in req.timeseries {
        let labels = labels_from_refs(&table, &series.labels_refs)?;
        let metadata = series
            .metadata
            .as_ref()
            .map(|metadata| metadata_from_v2(&table, &labels, metadata))
            .transpose()?;
        let samples = series
            .samples
            .into_iter()
            .map(|sample| {
                DecodedSample::with_start_timestamp(
                    sample.timestamp,
                    sample.value,
                    (sample.start_timestamp != 0).then_some(sample.start_timestamp),
                )
            })
            .collect::<Vec<_>>();
        counts.samples += samples.len() as u64;

        let histograms = series
            .histograms
            .iter()
            .map(|histogram| Ok((histogram.timestamp, v2_histogram_to_native(histogram)?)))
            .collect::<Result<Vec<_>, WireError>>()?;
        counts.histograms += histograms.len() as u64;

        let exemplars = series
            .exemplars
            .iter()
            .map(|exemplar| {
                Ok(DecodedExemplar {
                    labels: labels_from_refs(&table, &exemplar.labels_refs)?,
                    timestamp_ms: exemplar.timestamp,
                    value: exemplar.value,
                })
            })
            .collect::<Result<Vec<_>, WireError>>()?;
        counts.exemplars += exemplars.len() as u64;

        out.push(DecodedSeries {
            labels,
            samples,
            histograms,
            exemplars,
            metadata,
        });
    }

    Ok((out, counts))
}

fn labels_from_refs(table: &SymbolTable, refs: &[u32]) -> Result<Labels, WireError> {
    table
        .resolve_label_refs(refs)
        .map(Labels::from_iter)
        .map_err(|error| WireError::Invalid(error.to_string()))
}

fn metadata_from_v2(
    table: &SymbolTable,
    labels: &Labels,
    metadata: &pb::v2::Metadata,
) -> Result<DecodedMetadata, WireError> {
    Ok(DecodedMetadata {
        metric_family_name: labels.get("__name__").unwrap_or_default().to_string(),
        metric_type: metadata_type(metadata.r#type),
        help: symbol_ref(table, metadata.help_ref)?,
        unit: symbol_ref(table, metadata.unit_ref)?,
    })
}

fn symbol_ref(table: &SymbolTable, index: u32) -> Result<String, WireError> {
    table
        .resolve(index)
        .map(str::to_string)
        .ok_or_else(|| WireError::Invalid(format!("symbol ref {index} out of range")))
}

fn metadata_type(value: i32) -> String {
    match pb::v2::metadata::MetricType::try_from(value) {
        Ok(pb::v2::metadata::MetricType::Counter) => "counter",
        Ok(pb::v2::metadata::MetricType::Gauge) => "gauge",
        Ok(pb::v2::metadata::MetricType::Histogram) => "histogram",
        Ok(pb::v2::metadata::MetricType::Gaugehistogram) => "gaugehistogram",
        Ok(pb::v2::metadata::MetricType::Summary) => "summary",
        Ok(pb::v2::metadata::MetricType::Info) => "info",
        Ok(pb::v2::metadata::MetricType::Stateset) => "stateset",
        Ok(pb::v2::metadata::MetricType::Unspecified) | Err(_) => "unknown",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use prost::Message;

    use super::*;

    fn snappy(body: &[u8]) -> Vec<u8> {
        snap::raw::Encoder::new().compress_vec(body).unwrap()
    }

    #[test]
    fn decodes_v2_symbols_samples_exemplars_and_counts() {
        let req = pb::v2::Request {
            symbols: vec![
                String::new(),
                "__name__".into(),
                "up".into(),
                "trace_id".into(),
                "abc".into(),
            ],
            timeseries: vec![pb::v2::TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![pb::v2::Sample {
                    value: 1.0,
                    timestamp: 1000,
                    start_timestamp: 0,
                }],
                exemplars: vec![pb::v2::Exemplar {
                    labels_refs: vec![3, 4],
                    value: 2.0,
                    timestamp: 1100,
                }],
                ..Default::default()
            }],
        };

        let (decoded, counts) = decode_v2(&snappy(&req.encode_to_vec()), 1 << 20).unwrap();

        assert_eq!(
            decoded,
            vec![DecodedSeries {
                labels: Labels::from_iter([("__name__".to_string(), "up".to_string())]),
                samples: vec![DecodedSample::new(1000, 1.0)],
                histograms: vec![],
                exemplars: vec![DecodedExemplar {
                    labels: Labels::from_iter([("trace_id".to_string(), "abc".to_string())]),
                    timestamp_ms: 1100,
                    value: 2.0,
                }],
                metadata: None,
            }]
        );
        assert_eq!(
            counts,
            WrittenCounts {
                samples: 1,
                histograms: 0,
                exemplars: 1,
            }
        );
    }

    #[test]
    fn decodes_v2_histograms_and_counts_them() {
        let req = pb::v2::Request {
            symbols: vec![String::new()],
            timeseries: vec![pb::v2::TimeSeries {
                histograms: vec![pb::v2::Histogram {
                    timestamp: 10,
                    positive_spans: vec![pb::v2::BucketSpan {
                        offset: 0,
                        length: 2,
                    }],
                    positive_deltas: vec![1, 2],
                    count: Some(pb::v2::histogram::Count::CountInt(3)),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let (decoded, counts) = decode_v2(&snappy(&req.encode_to_vec()), 1 << 20).unwrap();

        assert!(counts.histograms == 1);
        assert!(decoded[0].histograms[0].1.positive_counts == vec![1.0, 3.0]);
    }

    #[test]
    fn decode_v2_rejects_non_empty_first_symbol() {
        let req = pb::v2::Request {
            symbols: vec!["not-empty".into()],
            timeseries: Vec::new(),
        };

        let err = decode_v2(&snappy(&req.encode_to_vec()), 1 << 20).unwrap_err();

        assert!(matches!(err, WireError::Invalid(_)));
    }

    #[test]
    fn decode_v2_rejects_duplicate_label_names() {
        let req = pb::v2::Request {
            symbols: vec![
                String::new(),
                "__name__".into(),
                "up".into(),
                "job".into(),
                "api".into(),
                "worker".into(),
            ],
            timeseries: vec![pb::v2::TimeSeries {
                labels_refs: vec![1, 2, 3, 4, 3, 5],
                samples: vec![pb::v2::Sample {
                    value: 1.0,
                    timestamp: 1000,
                    start_timestamp: 0,
                }],
                ..Default::default()
            }],
        };

        let err = decode_v2(&snappy(&req.encode_to_vec()), 1 << 20).unwrap_err();

        assert!(matches!(err, WireError::Invalid(_)));
        assert!(format!("{err}").contains("duplicate label `job`"));
    }

    #[test]
    fn decodes_v2_sample_start_timestamp() {
        let req = pb::v2::Request {
            symbols: vec![String::new(), "__name__".into(), "up".into()],
            timeseries: vec![pb::v2::TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![pb::v2::Sample {
                    value: 1.0,
                    timestamp: 1000,
                    start_timestamp: 500,
                }],
                ..Default::default()
            }],
        };

        let (decoded, _) = decode_v2(&snappy(&req.encode_to_vec()), 1 << 20).unwrap();

        assert!(
            decoded[0].samples == vec![DecodedSample::with_start_timestamp(1000, 1.0, Some(500))]
        );
    }
}
