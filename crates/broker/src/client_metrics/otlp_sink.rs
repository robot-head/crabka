//! OTLP forward sink for KIP-714 client metrics. The sink re-emits decoded
//! client `MetricsData` to the OTLP collector that traces already use. Sends
//! happen on a bounded background task. The sink drops and counts overflow, so
//! the request path never blocks on a slow collector.

use opentelemetry_proto::tonic::{
    collector::metrics::v1::{
        ExportMetricsServiceRequest, metrics_service_client::MetricsServiceClient,
    },
    common::v1::{AnyValue, KeyValue, any_value::Value},
    metrics::v1::MetricsData,
};
use prometheus_client::metrics::counter::Counter;
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
    tx: std::sync::Mutex<Option<mpsc::Sender<(MetricsData, String)>>>,
    task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    dropped: Counter,
}

impl OtlpForwarder {
    /// Disabled forwarder for when no endpoint is configured. All `forward`
    /// calls no-op.
    pub(crate) fn disabled() -> Self {
        Self {
            tx: std::sync::Mutex::new(None),
            task: tokio::sync::Mutex::new(None),
            dropped: Counter::default(),
        }
    }

    /// Spawn a background worker that POSTs export requests to `endpoint`
    /// (HTTP/protobuf `/v1/metrics`). `capacity` bounds the in-flight queue.
    pub(crate) fn spawn(
        endpoint: String,
        protocol: crabka_telemetry::OtlpProtocol,
        capacity: usize,
        dropped: Counter,
        failed: Counter,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<(MetricsData, String)>(capacity);
        let task = tokio::spawn(async move {
            let http_client = reqwest::Client::new();
            let http_url = format!("{}/v1/metrics", endpoint.trim_end_matches('/'));
            while let Some((md, instance)) = rx.recv().await {
                let req = build_export_request(md, &instance);
                let result = match protocol {
                    crabka_telemetry::OtlpProtocol::HttpProtobuf => {
                        let body = {
                            use prost::Message;
                            req.encode_to_vec()
                        };
                        http_client
                            .post(&http_url)
                            .header("content-type", "application/x-protobuf")
                            .body(body)
                            .send()
                            .await
                            .and_then(reqwest::Response::error_for_status)
                            .map(|_| ())
                            .map_err(|error| error.to_string())
                    }
                    crabka_telemetry::OtlpProtocol::Grpc => {
                        match MetricsServiceClient::connect(endpoint.clone()).await {
                            Ok(mut client) => client
                                .export(tonic::Request::new(req))
                                .await
                                .map(|_| ())
                                .map_err(|error| error.to_string()),
                            Err(error) => Err(error.to_string()),
                        }
                    }
                };
                if let Err(error) = result {
                    failed.inc();
                    tracing::warn!(%error, "client-metrics OTLP forward failed");
                }
            }
        });
        Self {
            tx: std::sync::Mutex::new(Some(tx)),
            task: tokio::sync::Mutex::new(Some(task)),
            dropped,
        }
    }

    #[cfg(test)]
    pub(crate) fn is_enabled(&self) -> bool {
        self.tx
            .lock()
            .expect("OTLP forwarder mutex poisoned")
            .is_some()
    }

    /// Enqueue metrics for forwarding. This function drops the metrics and
    /// writes a debug log if the queue is full or the forwarder is disabled.
    /// It never blocks.
    pub(crate) fn forward(&self, md: MetricsData, client_instance_id: &str) {
        if let Some(tx) = self
            .tx
            .lock()
            .expect("OTLP forwarder mutex poisoned")
            .as_ref()
            && let Err(error) = tx.try_send((md, client_instance_id.to_string()))
        {
            self.dropped.inc();
            tracing::warn!(%error, "client-metrics OTLP forward queue unavailable; dropping");
        }
    }

    /// Closes the queue and waits until every accepted batch has been sent.
    pub(crate) async fn shutdown(&self) {
        self.tx
            .lock()
            .expect("OTLP forwarder mutex poisoned")
            .take();
        if let Some(task) = self.task.lock().await.take()
            && let Err(error) = task.await
        {
            tracing::warn!(%error, "client-metrics OTLP forward worker join failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::metrics::v1::{MetricsData, ResourceMetrics};
    use prometheus_client::metrics::counter::Counter;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::path};

    use super::*;

    #[test]
    fn wraps_and_injects_instance_id() {
        let md = MetricsData {
            resource_metrics: vec![ResourceMetrics::default()],
        };
        let req = build_export_request(md, "abc-123");
        assert_eq!(req.resource_metrics.len(), 1);
        let res = req.resource_metrics[0].resource.as_ref().expect("resource");
        let attribute = res
            .attributes
            .iter()
            .find(|kv| kv.key == "client_instance_id")
            .expect("client instance attribute");
        assert_eq!(
            attribute
                .value
                .as_ref()
                .and_then(|value| value.value.as_ref()),
            Some(&Value::StringValue("abc-123".to_string()))
        );
    }

    #[test]
    fn disabled_forwarder_is_noop() {
        let f = OtlpForwarder::disabled();
        assert!(!f.is_enabled());
        f.forward(MetricsData::default(), "x");
    }

    #[tokio::test]
    async fn http_status_failures_are_counted_and_shutdown_drains() {
        let server = MockServer::start().await;
        Mock::given(path("/v1/metrics"))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&server)
            .await;
        let dropped = Counter::default();
        let failed = Counter::default();
        let forwarder = OtlpForwarder::spawn(
            server.uri(),
            crabka_telemetry::OtlpProtocol::HttpProtobuf,
            1,
            dropped.clone(),
            failed.clone(),
        );

        forwarder.forward(MetricsData::default(), "client-a");
        forwarder.shutdown().await;

        assert_eq!(dropped.get(), 0);
        assert_eq!(failed.get(), 1);
    }
}
