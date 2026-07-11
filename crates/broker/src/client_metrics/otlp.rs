//! Decode KIP-714 `PushTelemetry` payloads (OTLP `MetricsData` v1 protobuf).

use opentelemetry_proto::tonic::metrics::v1::MetricsData;
use prost::Message;

#[derive(Debug, thiserror::Error)]
pub(crate) enum OtlpDecodeError {
    #[error("OTLP MetricsData protobuf decode failed: {0}")]
    Decode(#[from] prost::DecodeError),
}

/// Decode a (decompressed) OTLP `MetricsData` protobuf payload.
pub(crate) fn decode_metrics(bytes: &[u8]) -> Result<MetricsData, OtlpDecodeError> {
    Ok(MetricsData::decode(bytes)?)
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric::Data,
        number_data_point::Value,
    };
    use prost::Message;

    use super::*;

    fn sample_metrics_data() -> Vec<u8> {
        let dp = NumberDataPoint {
            value: Some(Value::AsInt(42)),
            ..Default::default()
        };
        let metric = Metric {
            name: "org.apache.kafka.consumer.fetch.size".into(),
            data: Some(Data::Gauge(Gauge {
                data_points: vec![dp],
            })),
            ..Default::default()
        };
        let md = MetricsData {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![metric],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        md.encode_to_vec()
    }

    #[test]
    fn decodes_valid_metrics_data() {
        let bytes = sample_metrics_data();
        let md = decode_metrics(&bytes).expect("decode");
        assert_eq!(md.resource_metrics.len(), 1);
    }

    #[test]
    fn rejects_garbage() {
        // A truncated varint (high-bit set, no continuation byte) reliably
        // causes prost to return a DecodeError.
        let bad = vec![0x82u8, 0x82, 0x82, 0x82, 0x82, 0x82, 0x82, 0x82, 0x82, 0x82];
        assert!(decode_metrics(&bad).is_err());
    }
}
