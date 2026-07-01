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
