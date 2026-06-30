//! `PushTelemetry` (`api_key=72`, KIP-714). Validates the push against the
//! client's subscription + throttle state, decompresses + decodes the OTLP
//! payload, and fans it out to the Prometheus + OTLP sinks.

use bytes::{Bytes, BytesMut};
use uuid::Uuid;

use crabka_compression::CompressionType;
use crabka_protocol::owned::push_telemetry_request::PushTelemetryRequest;
use crabka_protocol::owned::push_telemetry_response::PushTelemetryResponse;
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::client_metrics::manager::PushDecision;
use crate::client_metrics::otlp;
use crate::client_metrics::prometheus_sink::DataPoint;
use crate::codes;
use crate::error::BrokerError;
use crate::handlers::context::TelemetryContext;

#[allow(clippy::unused_async)] // signature symmetry with other inline-intercept handlers
#[tracing::instrument(
    name = "handle_push_telemetry",
    level = "info",
    skip_all,
    fields(api = "PushTelemetry", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
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
        PushDecision::Accept { .. } => {
            // authorize_push guarantees compression is supported on Accept.
            // A terminating push that later fails to decode still fences the
            // instance and drops those metrics (best-effort, matches Kafka).
            let ct = codec.expect("authorize_push guarantees a supported codec on Accept");
            // Bound decompressed output to guard against a decompression bomb
            // in the client-metrics payload: ≤100x the compressed size, with a
            // 16 MiB floor and a 1 GiB ceiling.
            let max_output = req
                .metrics
                .len()
                .saturating_mul(100)
                .clamp(16 * 1024 * 1024, 1024 * 1024 * 1024);
            match crabka_compression::decompress(ct, &req.metrics, max_output) {
                Ok(raw) => match otlp::decode_metrics(&raw) {
                    Ok(md) => {
                        let instance_str = instance.to_string();
                        let points = flatten_for_prometheus(&md, &instance_str, ctx.client_id);
                        broker.client_metrics.prometheus.ingest(&points);
                        broker.client_metrics.otlp.forward(md, &instance_str);
                    }
                    Err(e) => tracing::debug!(error = %e, "client-metrics OTLP decode failed"),
                },
                Err(e) => tracing::debug!(error = %e, "client-metrics decompress failed"),
            }
        }
    }

    let resp = PushTelemetryResponse {
        throttle_time_ms,
        error_code,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

/// Flatten an OTLP `MetricsData` into Prometheus data points (Sum/Gauge
/// numbers; Histogram → count/sum gauges). Best-effort — unknown shapes skipped.
fn flatten_for_prometheus(
    md: &opentelemetry_proto::tonic::metrics::v1::MetricsData,
    instance: &str,
    client_id: &str,
) -> Vec<DataPoint> {
    use opentelemetry_proto::tonic::metrics::v1::{metric::Data, number_data_point::Value};
    let mut out = Vec::new();
    let num = |v: &Value| -> f64 {
        match v {
            Value::AsDouble(d) => *d,
            Value::AsInt(i) => {
                #[allow(clippy::cast_precision_loss)]
                // i64→f64 for telemetry display; sub-ms precision loss is acceptable
                let f = *i as f64;
                f
            }
        }
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
                                    value: num(v),
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
                                    value: num(v),
                                });
                            }
                        }
                    }
                    Some(Data::Histogram(h)) => {
                        for dp in &h.data_points {
                            #[allow(clippy::cast_precision_loss)]
                            // u64→f64 for telemetry display; large counts lose sub-ms precision
                            out.push(DataPoint {
                                metric: format!("{}_count", m.name),
                                client_instance_id: instance.to_string(),
                                client_id: client_id.to_string(),
                                value: dp.count as f64,
                            });
                            if let Some(sum) = dp.sum {
                                out.push(DataPoint {
                                    metric: format!("{}_sum", m.name),
                                    client_instance_id: instance.to_string(),
                                    client_id: client_id.to_string(),
                                    value: sum,
                                });
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use bytes::{Bytes, BytesMut};
    use crabka_compression::CompressionType;
    use crabka_protocol::owned::push_telemetry_response;
    use crabka_protocol::primitives::uuid::Uuid as ProtoUuid;
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Histogram, HistogramDataPoint, Metric, MetricsData, NumberDataPoint,
        ResourceMetrics, ScopeMetrics, Sum, metric, number_data_point,
    };
    use uuid::Uuid;

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

    fn assert_close(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < f64::EPSILON);
    }

    fn encode_request(req: &PushTelemetryRequest) -> Bytes {
        let version = push_telemetry_response::MAX_VERSION;
        let mut buf = BytesMut::with_capacity(req.encoded_len(version));
        req.encode(&mut buf, version).expect("encode request");
        buf.freeze()
    }

    fn decode_response(bytes: &Bytes) -> PushTelemetryResponse {
        let version = push_telemetry_response::MAX_VERSION;
        let mut cur: &[u8] = bytes.as_ref();
        let resp = PushTelemetryResponse::decode(&mut cur, version).expect("decode response");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    async fn start_broker() -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let cfg = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
        let handle = crate::broker::Broker::start(cfg)
            .await
            .expect("start broker");
        (handle, dir)
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
        .await
        .expect("handle");
        let resp = decode_response(&resp);

        assert!(resp.throttle_time_ms == 0);
        assert!(resp.error_code == codes::INVALID_REQUEST);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_accept_path_preserves_success_response_fields() {
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
        let assignment = broker.client_metrics.manager.assign(
            &crabka_metadata::MetadataImage::new(Uuid::nil()),
            &crate::client_metrics::manager::ClientAttributes {
                client_instance_id: instance,
                client_id: ctx.client_id.to_string(),
                software_name: ctx.software_name.to_string(),
                software_version: ctx.software_version.to_string(),
                source_address: ctx.peer.ip().to_string(),
                source_port: ctx.peer.port(),
            },
        );
        let compressed = crabka_compression::compress(CompressionType::Gzip, b"not-otlp")
            .expect("compress telemetry payload");
        let req = PushTelemetryRequest {
            client_instance_id: ProtoUuid(*instance.as_bytes()),
            subscription_id: assignment.subscription_id,
            terminating: false,
            compression_type: i8::try_from(CompressionType::Gzip.as_attribute_bits()).unwrap(),
            metrics: compressed,
            ..Default::default()
        };

        let resp = handle(
            &broker,
            push_telemetry_response::MAX_VERSION,
            7,
            &encode_request(&req),
            &ctx,
        )
        .await
        .expect("handle");
        let resp = decode_response(&resp);

        assert!(resp.throttle_time_ms == 0);
        assert!(resp.error_code == codes::NONE);
        broker_handle.shutdown().await;
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

        assert!(points.len() == 4, "{points:?}");
        assert!(points[0].metric == "cpu.utilization");
        assert!(points[0].client_instance_id == "instance-1");
        assert!(points[0].client_id == "client-a");
        assert_close(points[0].value, 0.75);
        assert!(points[1].metric == "requests.total");
        assert_close(points[1].value, 42.0);
        assert!(points[2].metric == "latency.ms_count");
        assert_close(points[2].value, 3.0);
        assert!(points[3].metric == "latency.ms_sum");
        assert_close(points[3].value, 9.5);
    }
}
