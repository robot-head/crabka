//! Metrics-generator service loop.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::metricsgen::clock::Clock;
use crate::metricsgen::config::MetricsGenConfig;
use crate::metricsgen::processor::MetricsGenerator;
use crate::metricsgen::sink::{RemoteWriteSink, SinkError, SpanSource};

/// Wires the source, processors, sink, and clock for the metrics-generator role.
pub struct MetricsGenService<Src, Snk>
where
    Src: SpanSource,
    Snk: RemoteWriteSink,
{
    pub(crate) source: Arc<Src>,
    pub(crate) sink: Arc<Snk>,
    generator: Mutex<MetricsGenerator>,
    clock: Arc<dyn Clock>,
    cfg: MetricsGenConfig,
}

impl<Src, Snk> MetricsGenService<Src, Snk>
where
    Src: SpanSource + 'static,
    Snk: RemoteWriteSink + 'static,
{
    #[must_use]
    pub fn new(
        cfg: MetricsGenConfig,
        clock: Arc<dyn Clock>,
        source: Arc<Src>,
        sink: Arc<Snk>,
    ) -> Self {
        Self {
            generator: Mutex::new(MetricsGenerator::new(cfg.clone(), clock.clone())),
            cfg,
            clock,
            source,
            sink,
        }
    }

    pub async fn poll_once(&self, max: usize) -> Result<usize, SinkError> {
        let spans = self.source.poll(max).await?;
        let count = spans.len();
        if count > 0 {
            let mut generator = self
                .generator
                .lock()
                .expect("metrics generator mutex poisoned");
            for span in &spans {
                generator.process(span);
            }
        }
        Ok(count)
    }

    pub async fn collect_once(&self) -> Result<usize, SinkError> {
        let timestamp_ms = self.clock.now_ns() / 1_000_000;
        let payloads = {
            let mut generator = self
                .generator
                .lock()
                .expect("metrics generator mutex poisoned");
            generator.collect(timestamp_ms)
        };

        if payloads.is_empty() {
            return Ok(0);
        }

        for payload in &payloads {
            self.sink.write(payload).await?;
        }
        self.source.commit().await?;
        Ok(payloads.len())
    }

    pub async fn run(self, shutdown: CancellationToken) {
        let interval = self.cfg.collection_interval.max(Duration::from_secs(1));
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    if let Err(err) = self.collect_once().await {
                        tracing::warn!(error = %err, "metrics-generator final flush failed");
                    }
                    return;
                }
                _ = ticker.tick() => {
                    if let Err(err) = self.collect_once().await {
                        tracing::warn!(error = %err, "metrics-generator flush failed");
                    }
                }
                poll = self.poll_once(1_000) => {
                    if let Err(err) = poll {
                        tracing::warn!(error = %err, "metrics-generator poll failed");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;

    use super::*;
    use crate::metricsgen::clock::MockClock;
    use crate::metricsgen::config::MetricsGenConfig;
    use crate::metricsgen::contract::{SpanKind, SpanRecord, StatusCode};
    use crate::metricsgen::sink::{MockRemoteWriteSink, MockSpanSource};

    fn span(tenant: &str, kind: SpanKind, span_id: [u8; 8], parent: [u8; 8]) -> SpanRecord {
        SpanRecord {
            tenant: tenant.into(),
            trace_id: [0x44; 16],
            span_id,
            parent_span_id: parent,
            name: "op".into(),
            kind,
            start_ns: 0,
            duration_ns: 5_000_000,
            status: StatusCode::Ok,
            service_name: "svc".into(),
            attributes: vec![],
            size_bytes: 10,
        }
    }

    fn service() -> MetricsGenService<MockSpanSource, MockRemoteWriteSink> {
        let source = Arc::new(MockSpanSource::default());
        let sink = Arc::new(MockRemoteWriteSink::default());
        MetricsGenService::new(
            MetricsGenConfig::default(),
            Arc::new(MockClock::new(0)),
            source,
            sink,
        )
    }

    #[tokio::test]
    async fn poll_then_collect_writes_then_commits() {
        let svc = service();
        svc.source.push_batch(vec![
            span("A", SpanKind::Client, [0xA; 8], [0; 8]),
            span("A", SpanKind::Server, [0xB; 8], [0xA; 8]),
        ]);

        let processed = svc.poll_once(100).await.unwrap();
        assert!(processed == 2);

        let flushed = svc.collect_once().await.unwrap();
        assert!(flushed == 1);
        assert!(svc.sink.writes().len() == 1);
        let payload = &svc.sink.writes()[0];
        assert!(payload.tenant == "A");
        assert!(
            payload
                .series
                .iter()
                .any(|s| s.name == "traces_service_graph_request_total")
        );
        assert!(svc.source.commits() == 1);
    }

    #[tokio::test]
    async fn collect_does_not_commit_when_write_fails() {
        let svc = service();
        svc.source
            .push_batch(vec![span("A", SpanKind::Server, [0xB; 8], [0; 8])]);
        svc.poll_once(100).await.unwrap();
        svc.sink.fail_next();

        let result = svc.collect_once().await;

        assert!(result.is_err());
        assert!(svc.source.commits() == 0);
    }

    #[tokio::test]
    async fn empty_poll_is_a_noop() {
        let svc = service();
        assert!(svc.poll_once(100).await.unwrap() == 0);
        assert!(svc.collect_once().await.unwrap() == 0);
        assert!(svc.sink.writes().is_empty());
    }
}
