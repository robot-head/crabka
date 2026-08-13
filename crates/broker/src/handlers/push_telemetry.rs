//! `PushTelemetry` (`api_key=72`, KIP-714).
//!
//! The handler validates the push against the client's subscription and
//! throttle state, decompresses and decodes the OTLP payload, and fans it out
//! to the Prometheus and OTLP sinks.

use bytes::Bytes;
use crabka_compression::CompressionType;
use crabka_protocol::{
    Decode,
    owned::{
        push_telemetry_request::PushTelemetryRequest,
        push_telemetry_response::PushTelemetryResponse,
    },
};
use crabka_units::{
    ByteSize, Ratio,
    convert::{ByteSizeExt as _, RatioExt as _},
};
use uuid::Uuid;

use crate::{
    broker::Broker,
    client_metrics::{
        manager::PushDecision,
        otlp,
        prometheus_sink::{DataPoint, PointValue},
    },
    codes,
    error::BrokerError,
    handlers::context::TelemetryContext,
};

fn decompressed_output_bound(
    compressed_len: ByteSize,
    ratio: Ratio,
    floor: ByteSize,
    ceiling: ByteSize,
) -> ByteSize {
    ByteSize::from_bytes_f64(compressed_len.bytes_f64() * ratio.as_f64())
        .max(floor)
        .min(ceiling)
}

#[tracing::instrument(
    name = "handle_push_telemetry",
    level = "info",
    skip_all,
    fields(api = "PushTelemetry", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &TelemetryContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = PushTelemetryRequest::decode(&mut cur, version)?;
    let instance = Uuid::from_bytes(req.client_instance_id.0);

    let mut error_code = codes::NONE;
    let mut throttle_time_ms = 0i32;

    let codec =
        CompressionType::from_attribute_bits(u8::try_from(req.compression_type).unwrap_or(0xff));

    match broker.client_metrics.manager.authorize_push(
        instance,
        req.subscription_id,
        req.terminating,
        codec.is_some(),
        req.metrics.len(),
    ) {
        PushDecision::Reject {
            error_code: ec,
            throttle_ms,
        } => {
            error_code = ec;
            throttle_time_ms = throttle_ms;
        }
        PushDecision::Accept => {
            // authorize_push guarantees compression is supported on Accept.
            // A terminating push that later fails to decode still fences the
            // instance and drops those metrics (best-effort, matches Kafka).
            let ct = codec.expect("authorize_push guarantees a supported codec on Accept");
            if !req.metrics.is_empty() {
                // Bound decompressed output to guard against a decompression
                // bomb in the client-metrics payload.
                let max_output = decompressed_output_bound(
                    ByteSize::from_bytes(u64::try_from(req.metrics.len()).unwrap_or(u64::MAX)),
                    broker.config.telemetry_max_decompression_ratio,
                    broker.config.telemetry_decompressed_output_floor,
                    broker.config.telemetry_decompressed_output_ceiling,
                );
                let decoded = match crabka_compression::decompress(ct, &req.metrics, max_output) {
                    Ok(raw) => match otlp::decode_metrics(&raw) {
                        Ok(md) => Some(md),
                        Err(e) => {
                            tracing::debug!(error = %e, "client-metrics OTLP decode failed");
                            None
                        }
                    },
                    Err(e) => {
                        tracing::debug!(error = %e, "client-metrics decompress failed");
                        None
                    }
                };
                if let Some(md) = decoded {
                    let instance_str = instance.to_string();
                    let points = flatten_for_prometheus(&md, &instance_str, ctx.client_id);
                    broker.client_metrics.prometheus.ingest(&points);
                    broker.client_metrics.otlp.forward(md, &instance_str);
                } else {
                    error_code = codes::INVALID_RECORD;
                }
            }
        }
    }

    let resp = PushTelemetryResponse {
        throttle_time_ms,
        error_code,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

/// Flattens an OTLP `MetricsData` into Prometheus data points. Sum and Gauge
/// become numbers, and a Histogram becomes count and sum gauges. The function
/// is best-effort and skips unknown shapes.
fn flatten_for_prometheus(
    md: &opentelemetry_proto::tonic::metrics::v1::MetricsData,
    instance: &str,
    client_id: &str,
) -> Vec<DataPoint> {
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, KeyValue, any_value::Value as AnyValueKind},
        metrics::v1::{metric::Data, number_data_point::Value},
    };
    let mut out = Vec::new();
    let num = |v: &Value| -> f64 {
        match v {
            Value::AsDouble(d) => *d,
            Value::AsInt(i) => i
                .to_string()
                .parse()
                .expect("every i64 has a finite f64 representation"),
        }
    };
    let attribute_value = |value: &AnyValue| -> Option<String> {
        match value.value.as_ref()? {
            AnyValueKind::StringValue(value) => Some(value.clone()),
            AnyValueKind::BoolValue(value) => Some(value.to_string()),
            AnyValueKind::IntValue(value) => Some(value.to_string()),
            AnyValueKind::DoubleValue(value) => Some(value.to_string()),
            AnyValueKind::BytesValue(value) => Some(hex::encode(value)),
            AnyValueKind::ArrayValue(_)
            | AnyValueKind::KvlistValue(_)
            | AnyValueKind::StringValueStrindex(_) => None,
        }
    };
    let attributes = |sets: &[&[KeyValue]]| {
        let mut labels = sets
            .iter()
            .flat_map(|set| set.iter())
            .filter_map(|attribute| {
                Some((
                    sanitize_prometheus_label(&attribute.key),
                    attribute_value(attribute.value.as_ref()?)?,
                ))
            })
            .collect::<Vec<_>>();
        labels.sort();
        labels.dedup_by(|left, right| left.0 == right.0);
        labels
    };
    for rm in &md.resource_metrics {
        for sm in &rm.scope_metrics {
            for m in &sm.metrics {
                match &m.data {
                    Some(Data::Gauge(g)) => {
                        for dp in &g.data_points {
                            if let Some(v) = &dp.value {
                                out.push(DataPoint {
                                    metric: m.name.clone(),
                                    client_instance_id: instance.to_string(),
                                    client_id: client_id.to_string(),
                                    attributes: attributes(&[
                                        rm.resource
                                            .as_ref()
                                            .map_or(&[], |r| r.attributes.as_slice()),
                                        sm.scope.as_ref().map_or(&[], |s| s.attributes.as_slice()),
                                        dp.attributes.as_slice(),
                                    ]),
                                    value: PointValue::Gauge(num(v)),
                                });
                            }
                        }
                    }
                    Some(Data::Sum(s)) => {
                        for dp in &s.data_points {
                            if let Some(v) = &dp.value {
                                out.push(DataPoint {
                                    metric: m.name.clone(),
                                    client_instance_id: instance.to_string(),
                                    client_id: client_id.to_string(),
                                    attributes: attributes(&[
                                        rm.resource
                                            .as_ref()
                                            .map_or(&[], |r| r.attributes.as_slice()),
                                        sm.scope.as_ref().map_or(&[], |s| s.attributes.as_slice()),
                                        dp.attributes.as_slice(),
                                    ]),
                                    value: if s.is_monotonic {
                                        PointValue::Counter(num(v))
                                    } else {
                                        PointValue::Gauge(num(v))
                                    },
                                });
                            }
                        }
                    }
                    Some(Data::Histogram(h)) => {
                        for dp in &h.data_points {
                            let mut buckets = dp
                                .explicit_bounds
                                .iter()
                                .copied()
                                .zip(dp.bucket_counts.iter().copied())
                                .collect::<Vec<_>>();
                            if let Some(infinite) = dp.bucket_counts.get(dp.explicit_bounds.len()) {
                                buckets.push((f64::MAX, *infinite));
                            }
                            out.push(DataPoint {
                                metric: m.name.clone(),
                                client_instance_id: instance.to_string(),
                                client_id: client_id.to_string(),
                                attributes: attributes(&[
                                    rm.resource
                                        .as_ref()
                                        .map_or(&[], |r| r.attributes.as_slice()),
                                    sm.scope.as_ref().map_or(&[], |s| s.attributes.as_slice()),
                                    dp.attributes.as_slice(),
                                ]),
                                value: PointValue::Histogram {
                                    count: dp.count,
                                    sum: dp.sum.unwrap_or_default(),
                                    buckets,
                                },
                            });
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    out
}

fn sanitize_prometheus_label(label: &str) -> String {
    let mut sanitized = label
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.starts_with(|character: char| character.is_ascii_digit()) {
        sanitized.insert(0, '_');
    }
    sanitized
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use bytes::Bytes;
    use crabka_compression::CompressionType;
    use crabka_protocol::{owned::push_telemetry_response, primitives::uuid::Uuid as ProtoUuid};
    use crabka_units::{bytes, fraction, gibibytes};
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
        metrics::v1::{
            Gauge, Histogram, HistogramDataPoint, Metric, MetricsData, NumberDataPoint,
            ResourceMetrics, ScopeMetrics, Sum, metric, number_data_point,
        },
        resource::v1::Resource,
    };
    use prost::Message as _;
    use uuid::Uuid;

    use super::*;
    use crate::client_metrics::manager::SubscriptionDecision;

    fn number_point(value: number_data_point::Value) -> NumberDataPoint {
        NumberDataPoint {
            value: Some(value),
            ..Default::default()
        }
    }

    fn metrics_data(metrics: Vec<Metric>) -> MetricsData {
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    crate::test_support::codec_helpers!(
        PushTelemetryRequest,
        PushTelemetryResponse,
        version = push_telemetry_response::MAX_VERSION
    );

    async fn start_broker() -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|_cfg| {}).await
    }

    #[test]
    fn decompressed_output_bound_uses_runtime_policy() {
        let cases = [
            ("ratio", bytes(10), 7, bytes(1), bytes(1_000), bytes(70)),
            ("floor", bytes(10), 2, bytes(50), bytes(1_000), bytes(50)),
            ("ceiling", bytes(10), 100, bytes(1), bytes(500), bytes(500)),
            (
                "ceiling clamps a very large product",
                gibibytes(4),
                1_000_000,
                bytes(1),
                gibibytes(1),
                gibibytes(1),
            ),
        ];

        for (name, compressed_len, ratio, floor, ceiling, expected) in cases {
            assert!(
                decompressed_output_bound(
                    compressed_len,
                    fraction(f64::from(ratio)),
                    floor,
                    ceiling
                ) == expected,
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn handle_preserves_reject_response_fields_for_unknown_instance() {
        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let ctx = TelemetryContext {
            client_id: "client-a",
            peer: &"127.0.0.1:9092".parse().unwrap(),
            software_name: "test-client",
            software_version: "1.0.0",
        };
        let req = PushTelemetryRequest {
            client_instance_id: ProtoUuid([9; 16]),
            subscription_id: 12,
            terminating: false,
            compression_type: i8::try_from(CompressionType::Gzip.as_attribute_bits()).unwrap(),
            metrics: Bytes::from_static(b"payload"),
            ..Default::default()
        };

        let resp = handle(
            &broker,
            push_telemetry_response::MAX_VERSION,
            7,
            &encode_request(&req),
            &ctx,
        )
        .expect("handle");
        let resp = decode_response(&resp);

        assert!(resp.throttle_time_ms == 0);
        assert!(resp.error_code == codes::INVALID_REQUEST);
        broker_handle.shutdown().await;
    }

    async fn push_payload(payload: Bytes) -> i16 {
        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let instance = Uuid::from_u128(0x1234);
        let peer = "127.0.0.1:9092".parse().unwrap();
        let ctx = TelemetryContext {
            client_id: "client-a",
            peer: &peer,
            software_name: "test-client",
            software_version: "1.0.0",
        };
        let SubscriptionDecision::Assign(assignment) = broker.client_metrics.manager.assign(
            &crabka_metadata::MetadataImage::new(Uuid::nil()),
            &crate::client_metrics::manager::ClientAttributes {
                client_instance_id: instance,
                client_id: ctx.client_id.to_string(),
                software_name: ctx.software_name.to_string(),
                software_version: ctx.software_version.to_string(),
                source_address: ctx.peer.ip().to_string(),
                source_port: ctx.peer.port(),
            },
        ) else {
            panic!("fresh client must receive a subscription");
        };
        let req = PushTelemetryRequest {
            client_instance_id: ProtoUuid(*instance.as_bytes()),
            subscription_id: assignment.subscription_id,
            terminating: false,
            compression_type: i8::try_from(CompressionType::Gzip.as_attribute_bits()).unwrap(),
            metrics: payload,
            ..Default::default()
        };

        let resp = handle(
            &broker,
            push_telemetry_response::MAX_VERSION,
            7,
            &encode_request(&req),
            &ctx,
        )
        .expect("handle");
        let resp = decode_response(&resp);

        broker_handle.shutdown().await;
        resp.error_code
    }

    #[tokio::test]
    async fn valid_payload_is_accepted() {
        let raw = metrics_data(vec![Metric {
            name: "cpu.utilization".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![number_point(number_data_point::Value::AsDouble(0.75))],
            })),
            ..Default::default()
        }])
        .encode_to_vec();
        let payload = crabka_compression::compress(CompressionType::Gzip, &raw)
            .expect("compress telemetry payload");

        assert!(push_payload(payload).await == codes::NONE);
    }

    #[tokio::test]
    async fn malformed_otlp_payload_returns_invalid_record() {
        let payload = crabka_compression::compress(CompressionType::Gzip, b"not-otlp")
            .expect("compress telemetry payload");

        assert!(push_payload(payload).await == codes::INVALID_RECORD);
    }

    #[tokio::test]
    async fn malformed_compressed_payload_returns_invalid_record() {
        assert!(push_payload(Bytes::from_static(b"not-gzip")).await == codes::INVALID_RECORD);
    }

    #[tokio::test]
    async fn empty_payload_is_accepted_without_decode() {
        assert!(push_payload(Bytes::new()).await == codes::NONE);
    }

    #[test]
    fn flatten_for_prometheus_preserves_gauge_sum_and_histogram_points() {
        let md = metrics_data(vec![
            Metric {
                name: "cpu.utilization".into(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![number_point(number_data_point::Value::AsDouble(0.75))],
                })),
                ..Default::default()
            },
            Metric {
                name: "requests.total".into(),
                data: Some(metric::Data::Sum(Sum {
                    data_points: vec![number_point(number_data_point::Value::AsInt(42))],
                    is_monotonic: true,
                    ..Default::default()
                })),
                ..Default::default()
            },
            Metric {
                name: "latency.ms".into(),
                data: Some(metric::Data::Histogram(Histogram {
                    data_points: vec![HistogramDataPoint {
                        count: 3,
                        sum: Some(9.5),
                        ..Default::default()
                    }],
                    ..Default::default()
                })),
                ..Default::default()
            },
        ]);

        let points = flatten_for_prometheus(&md, "instance-1", "client-a");

        assert!(points.len() == 3, "{points:?}");
        check!(
            points[0].client_instance_id.as_str() == "instance-1",
            "{points:?}"
        );
        check!(points[0].client_id.as_str() == "client-a", "{points:?}");
        assert!(points[0].metric == "cpu.utilization", "{points:?}");
        assert!(
            matches!(points[0].value, PointValue::Gauge(value) if (value - 0.75).abs() < f64::EPSILON)
        );
        assert!(points[1].metric == "requests.total", "{points:?}");
        assert!(
            matches!(points[1].value, PointValue::Counter(value) if (value - 42.0).abs() < f64::EPSILON)
        );
        assert!(points[2].metric == "latency.ms", "{points:?}");
        assert!(
            matches!(points[2].value, PointValue::Histogram { count: 3, sum, .. } if (sum - 9.5).abs() < f64::EPSILON)
        );
    }

    fn string_attribute(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.into())),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn flatten_for_prometheus_sanitizes_and_deduplicates_attribute_labels() {
        let mut point = number_point(number_data_point::Value::AsInt(1));
        point.attributes = vec![string_attribute("dup.key", "point")];
        let md = MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![
                        string_attribute("9bad-key", "resource"),
                        string_attribute("dup.key", "resource"),
                    ],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        attributes: vec![string_attribute("dup.key", "scope")],
                        ..Default::default()
                    }),
                    metrics: vec![Metric {
                        name: "requests".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![point],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let points = flatten_for_prometheus(&md, "instance", "client");

        assert!(
            points[0].attributes
                == vec![
                    ("_9bad_key".into(), "resource".into()),
                    ("dup_key".into(), "point".into()),
                ]
        );
    }
}
