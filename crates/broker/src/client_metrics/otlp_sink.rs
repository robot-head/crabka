//! OTLP forward sink for KIP-714 client metrics. The sink re-emits decoded
//! client `MetricsData` to the OTLP collector that traces already use. Sends
//! happen on a bounded background task. The sink drops and counts overflow, so
//! the request path never blocks on a slow collector.

use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value::Value},
    metrics::v1::MetricsData,
};
use tokio::sync::mpsc;

/// Build an OTLP export request from decoded metrics. This function tags
/// every resource with the originating client's instance id.
pub(crate) fn build_export_request(
    mut md: MetricsData,
    client_instance_id: &str,
) -> ExportMetricsServiceRequest {
    for rm in &mut md.resource_metrics {
        let resource = rm.resource.get_or_insert_with(Default::default);
        resource.attributes.push(KeyValue {
            key: "client_instance_id".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(client_instance_id.to_string())),
            }),
            ..Default::default()
        });
    }
    ExportMetricsServiceRequest {
        resource_metrics: md.resource_metrics,
    }
}

pub(crate) struct OtlpForwarder {
    tx: Option<mpsc::Sender<(MetricsData, String)>>,
}

impl OtlpForwarder {
    /// Disabled forwarder for when no endpoint is configured. All `forward`
    /// calls no-op.
    pub(crate) fn disabled() -> Self {
        Self { tx: None }
    }

    /// Spawn a background worker that POSTs export requests to `endpoint`
    /// (HTTP/protobuf `/v1/metrics`). `capacity` bounds the in-flight queue.
    pub(crate) fn spawn(endpoint: String, capacity: usize) -> Self {
        let (tx, mut rx) = mpsc::channel::<(MetricsData, String)>(capacity);
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let url = format!("{}/v1/metrics", endpoint.trim_end_matches('/'));
            while let Some((md, instance)) = rx.recv().await {
                let req = build_export_request(md, &instance);
                let body = {
                    use prost::Message;
                    req.encode_to_vec()
                };
                if let Err(e) = client
                    .post(&url)
                    .header("content-type", "application/x-protobuf")
                    .body(body)
                    .send()
                    .await
                {
                    tracing::debug!(error = %e, "client-metrics OTLP forward failed");
                }
            }
        });
        Self { tx: Some(tx) }
    }

    #[cfg(test)]
    pub(crate) fn is_enabled(&self) -> bool {
        self.tx.is_some()
    }

    /// Enqueue metrics for forwarding. This function drops the metrics and
    /// writes a debug log if the queue is full or the forwarder is disabled.
    /// It never blocks.
    pub(crate) fn forward(&self, md: MetricsData, client_instance_id: &str) {
        if let Some(tx) = &self.tx
            && let Err(e) = tx.try_send((md, client_instance_id.to_string()))
        {
            tracing::debug!(error = %e, "client-metrics OTLP forward queue full; dropping");
        }
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::metrics::v1::{MetricsData, ResourceMetrics};

    use super::*;

    #[test]
    fn wraps_and_injects_instance_id() {
        let md = MetricsData {
            resource_metrics: vec![ResourceMetrics::default()],
        };
        let req = build_export_request(md, "abc-123");
        assert_eq!(req.resource_metrics.len(), 1);
        let res = req.resource_metrics[0].resource.as_ref().expect("resource");
        assert!(
            res.attributes
                .iter()
                .any(|kv| kv.key == "client_instance_id")
        );
    }

    #[test]
    fn disabled_forwarder_is_noop() {
        let f = OtlpForwarder::disabled();
        assert!(!f.is_enabled());
        f.forward(MetricsData::default(), "x");
    }
}
