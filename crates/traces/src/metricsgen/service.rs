//! Metrics-generator service loop.

use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio_util::sync::CancellationToken;

use crate::metricsgen::{
    checkpoint::EdgeCheckpointStore,
    clock::Clock,
    config::MetricsGenConfig,
    processor::MetricsGenerator,
    series::SeriesPayload,
    sink::{RemoteWriteSink, SinkError, SpanSource},
};

/// Wires the source, processors, sink, and clock for the metrics-generator role.
pub struct MetricsGenService<Src, Snk>
where
    Src: SpanSource,
    Snk: RemoteWriteSink,
{
    pub(crate) source: Arc<Src>,
    pub(crate) sink: Arc<Snk>,
    generator: Mutex<MetricsGenerator>,
    pending_payloads: Mutex<Vec<SeriesPayload>>,
    checkpoint_store: Option<Arc<dyn EdgeCheckpointStore>>,
    checkpoint_keys: Mutex<HashMap<String, BTreeSet<Vec<u8>>>>,
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
            pending_payloads: Mutex::new(Vec::new()),
            checkpoint_store: None,
            checkpoint_keys: Mutex::new(HashMap::new()),
            cfg,
            clock,
            source,
            sink,
        }
    }

    #[must_use]
    pub fn with_checkpoint_store(mut self, store: Arc<dyn EdgeCheckpointStore>) -> Self {
        self.checkpoint_store = Some(store);
        self
    }

    #[must_use]
    pub fn with_checkpoint_store_for_tenants<I, T>(
        self,
        store: &Arc<dyn EdgeCheckpointStore>,
        tenants: I,
    ) -> Self
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        let service = self.with_checkpoint_store(store.clone());
        {
            let mut generator = service
                .generator
                .lock()
                .expect("metrics generator mutex poisoned");
            let mut checkpoint_keys = service
                .checkpoint_keys
                .lock()
                .expect("metrics generator checkpoint key mutex poisoned");
            for tenant in tenants {
                let tenant = tenant.as_ref();
                let entries = store.load_all(tenant);
                let mut keys = BTreeSet::new();
                for (key, value) in entries {
                    if generator
                        .restore_edge_checkpoint(tenant, &key, &value)
                        .is_ok()
                    {
                        keys.insert(key);
                    }
                }
                checkpoint_keys.insert(tenant.to_string(), keys);
            }
        }
        service
    }

    #[must_use]
    pub fn with_checkpoint_store_restoring_all_tenants(
        self,
        store: &Arc<dyn EdgeCheckpointStore>,
    ) -> Self {
        let tenants = store.tenants();
        self.with_checkpoint_store_for_tenants(store, tenants)
    }

    pub async fn poll_once(&self, max: usize) -> Result<usize, SinkError> {
        // NOTE: do not gate polling on `pending_payloads`. Awaiting `source.poll`
        // is what keeps the Kafka WAL consumer alive and draining; an early
        // `return Ok(0)` here resolved the run loop's `select!` poll arm
        // synchronously (no `.await`), busy-spinning the CPU AND halting Kafka
        // consumption entirely while a flush was outstanding (sink down) — growing
        // consumer-group lag until the session timed out and the group rebalanced.
        // A pending payload was already snapshotted by `collect`/`drain`, so
        // processing newly polled spans only feeds the *next* collection.
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
            drop(generator);
            self.sync_edge_checkpoints();
        }
        Ok(count)
    }

    pub async fn collect_once(&self) -> Result<usize, SinkError> {
        let timestamp_ms = self.clock.now_ns() / 1_000_000;
        let payload_count = {
            let mut pending = self
                .pending_payloads
                .lock()
                .expect("metrics generator pending payload mutex poisoned");
            if pending.is_empty() {
                let mut generator = self
                    .generator
                    .lock()
                    .expect("metrics generator mutex poisoned");
                *pending = generator.collect(timestamp_ms);
            }
            pending.len()
        };

        if payload_count == 0 {
            // No payload to flush; still persist live half-edge checkpoints. This
            // path only *saves* current edges (no expiry accounting is pending),
            // so it cannot lose unpaired counts.
            self.sync_edge_checkpoints();
            return Ok(0);
        }

        let mut written = 0;
        while let Some(payload) = self.pending_payload() {
            self.sink.write(&payload).await?;
            self.mark_pending_payload_written();
            written += 1;
        }
        self.source.commit().await?;

        // Only now that the payload carrying any expired-edge unpaired-span
        // accounting is durably written AND offsets are committed do we sync
        // (and tombstone) edge checkpoints. A write/commit failure above returns
        // early via `?`, leaving the expired-edge checkpoints intact for retry —
        // matching the write-then-commit crash-safety already used for payloads.
        self.sync_edge_checkpoints();
        Ok(written)
    }

    fn pending_payload(&self) -> Option<SeriesPayload> {
        self.pending_payloads
            .lock()
            .expect("metrics generator pending payload mutex poisoned")
            .first()
            .cloned()
    }

    fn mark_pending_payload_written(&self) {
        self.pending_payloads
            .lock()
            .expect("metrics generator pending payload mutex poisoned")
            .remove(0);
    }

    fn sync_edge_checkpoints(&self) {
        let Some(store) = &self.checkpoint_store else {
            return;
        };

        let checkpoints = self
            .generator
            .lock()
            .expect("metrics generator mutex poisoned")
            .edge_checkpoints();
        let mut previous = self
            .checkpoint_keys
            .lock()
            .expect("metrics generator checkpoint key mutex poisoned");

        for (tenant, entries) in checkpoints {
            let current: BTreeSet<Vec<u8>> = entries.iter().map(|(key, _)| key.clone()).collect();
            for (key, value) in entries {
                store.save(&tenant, &key, &value);
            }
            if let Some(old_keys) = previous.get(&tenant) {
                for old_key in old_keys.difference(&current) {
                    store.save(&tenant, old_key, b"");
                }
            }
            previous.insert(tenant, current);
        }
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

    use assert2::check;

    use super::*;
    use crate::metricsgen::{
        checkpoint::{EdgeCheckpointStore, InMemoryCheckpointStore},
        clock::MockClock,
        config::MetricsGenConfig,
        contract::{SpanKind, SpanRecord, StatusCode},
        sink::{MockRemoteWriteSink, MockSpanSource},
    };

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
            status_message: String::new(),
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
        assert2::assert!(processed == 2);

        let flushed = svc.collect_once().await.unwrap();
        assert2::assert!(flushed == 1);
        assert2::assert!(svc.sink.writes().len() == 1);
        let payload = &svc.sink.writes()[0];
        check!(payload.tenant == "A");
        check!(
            payload
                .series
                .iter()
                .any(|s| s.name == "traces_service_graph_request_total")
        );
        check!(svc.source.commits() == 1);
    }

    #[tokio::test]
    async fn poll_keeps_consuming_while_a_flush_is_pending() {
        let svc = service();
        svc.source
            .push_batch(vec![span("A", SpanKind::Server, [0xB; 8], [0; 8])]);
        svc.poll_once(100).await.unwrap();
        // A failed write leaves a payload pending (sink down).
        svc.sink.fail_next();
        assert2::assert!(svc.collect_once().await.is_err());

        // With a payload pending, polling MUST still consume spans. Previously
        // poll_once short-circuited to Ok(0) without awaiting, busy-spinning the
        // run loop and halting Kafka consumption until the sink recovered.
        svc.source
            .push_batch(vec![span("B", SpanKind::Server, [0xC; 8], [0; 8])]);
        let processed = svc.poll_once(100).await.unwrap();
        assert2::assert!(processed == 1);
    }

    #[tokio::test]
    async fn collect_does_not_commit_when_write_fails() {
        let svc = service();
        svc.source
            .push_batch(vec![span("A", SpanKind::Server, [0xB; 8], [0; 8])]);
        svc.poll_once(100).await.unwrap();
        svc.sink.fail_next();

        let result = svc.collect_once().await;

        assert2::assert!(result.is_err());
        assert2::assert!(svc.source.commits() == 0);
    }

    #[tokio::test]
    async fn collect_retries_pending_payload_after_write_failure() {
        let svc = service();
        svc.source
            .push_batch(vec![span("A", SpanKind::Server, [0xB; 8], [0; 8])]);
        svc.poll_once(100).await.unwrap();
        svc.sink.fail_next();

        check!(svc.collect_once().await.is_err());
        check!(svc.source.commits() == 0);
        check!(svc.sink.writes().is_empty());

        let retried = svc.collect_once().await.unwrap();

        check!(retried == 1);
        check!(svc.sink.writes().len() == 1);
        check!(svc.source.commits() == 1);
    }

    #[tokio::test]
    async fn collect_retries_only_unwritten_payloads_after_partial_write_failure() {
        let svc = service();
        svc.source.push_batch(vec![
            span("A", SpanKind::Server, [0xA; 8], [0; 8]),
            span("B", SpanKind::Server, [0xB; 8], [0; 8]),
        ]);
        svc.poll_once(100).await.unwrap();
        svc.sink.fail_after_successes(1);

        check!(svc.collect_once().await.is_err());
        check!(svc.source.commits() == 0);
        check!(svc.sink.writes().len() == 1);

        let retried = svc.collect_once().await.unwrap();
        let writes = svc.sink.writes();

        check!(retried == 1);
        assert2::assert!(writes.len() == 2);
        check!(writes[0].tenant != writes[1].tenant);
        check!(svc.source.commits() == 1);
    }

    #[tokio::test]
    async fn empty_poll_is_a_noop() {
        let svc = service();
        check!(svc.poll_once(100).await.unwrap() == 0);
        check!(svc.collect_once().await.unwrap() == 0);
        check!(svc.sink.writes().is_empty());
    }

    #[tokio::test]
    async fn poll_updates_edge_checkpoints_for_pending_and_completed_edges() {
        let store = Arc::new(InMemoryCheckpointStore::default());
        let svc = service().with_checkpoint_store(store.clone());

        svc.source
            .push_batch(vec![span("A", SpanKind::Client, [0xA; 8], [0; 8])]);
        assert2::assert!(svc.poll_once(100).await.unwrap() == 1);
        assert2::assert!(store.load_all("A").len() == 1);

        svc.source
            .push_batch(vec![span("A", SpanKind::Server, [0xB; 8], [0xA; 8])]);
        assert2::assert!(svc.poll_once(100).await.unwrap() == 1);
        assert2::assert!(store.load_all("A").is_empty());
    }

    #[tokio::test]
    async fn collect_tombstones_checkpoints_for_expired_edges() {
        let store = Arc::new(InMemoryCheckpointStore::default());
        let clock = MockClock::new(0);
        let source = Arc::new(MockSpanSource::default());
        let sink = Arc::new(MockRemoteWriteSink::default());
        let svc = MetricsGenService::new(
            MetricsGenConfig::default(),
            Arc::new(clock.clone()),
            source.clone(),
            sink,
        )
        .with_checkpoint_store(store.clone());

        source.push_batch(vec![span("A", SpanKind::Client, [0xA; 8], [0; 8])]);
        assert2::assert!(svc.poll_once(100).await.unwrap() == 1);
        assert2::assert!(store.load_all("A").len() == 1);

        clock.set(11_000_000_000);
        assert2::assert!(svc.collect_once().await.unwrap() == 1);

        assert2::assert!(store.load_all("A").is_empty());
    }

    #[tokio::test]
    async fn collect_keeps_expired_edge_checkpoint_when_write_fails() {
        let store = Arc::new(InMemoryCheckpointStore::default());
        let clock = MockClock::new(0);
        let source = Arc::new(MockSpanSource::default());
        let sink = Arc::new(MockRemoteWriteSink::default());
        let svc = MetricsGenService::new(
            MetricsGenConfig::default(),
            Arc::new(clock.clone()),
            source.clone(),
            sink.clone(),
        )
        .with_checkpoint_store(store.clone());

        source.push_batch(vec![span("A", SpanKind::Client, [0xA; 8], [0; 8])]);
        assert2::assert!(svc.poll_once(100).await.unwrap() == 1);
        assert2::assert!(store.load_all("A").len() == 1);

        // Advance past the edge TTL so the collect expires it, but force the sink
        // write to fail. The unpaired-span accounting is carried in the (pending)
        // payload, so the expired-edge checkpoint MUST survive for retry — never
        // tombstone before the payload is durably written and committed.
        clock.set(11_000_000_000);
        svc.sink.fail_next();
        check!(svc.collect_once().await.is_err());
        check!(svc.source.commits() == 0);
        check!(store.load_all("A").len() == 1);

        // A subsequent successful collect still emits/accounts the unpaired count
        // and only then tombstones the checkpoint.
        let written = svc.collect_once().await.unwrap();
        assert2::assert!(written == 1);
        let payload = svc.sink.writes().pop().unwrap();
        check!(
            payload
                .series
                .iter()
                .any(|s| s.name == "traces_service_graph_unpaired_spans_total")
        );
        check!(svc.source.commits() == 1);
        check!(store.load_all("A").is_empty());
    }

    #[tokio::test]
    async fn checkpointed_edges_are_restored_on_restart() {
        let store = Arc::new(InMemoryCheckpointStore::default());
        let svc = service().with_checkpoint_store(store.clone());
        svc.source
            .push_batch(vec![span("A", SpanKind::Client, [0xA; 8], [0; 8])]);
        assert2::assert!(svc.poll_once(100).await.unwrap() == 1);

        let store_for_restore: Arc<dyn EdgeCheckpointStore> = store.clone();
        let restarted = service().with_checkpoint_store_for_tenants(&store_for_restore, ["A"]);
        restarted
            .source
            .push_batch(vec![span("A", SpanKind::Server, [0xB; 8], [0xA; 8])]);
        assert2::assert!(restarted.poll_once(100).await.unwrap() == 1);
        assert2::assert!(restarted.collect_once().await.unwrap() == 1);

        let payload = restarted.sink.writes().pop().unwrap();
        assert2::assert!(
            payload
                .series
                .iter()
                .any(|s| s.name == "traces_service_graph_request_total")
        );
        assert2::assert!(store.load_all("A").is_empty());
    }

    #[tokio::test]
    async fn checkpointed_edges_restore_all_store_tenants_on_restart() {
        let store = Arc::new(InMemoryCheckpointStore::default());
        let svc = service().with_checkpoint_store(store.clone());
        svc.source.push_batch(vec![
            span("A", SpanKind::Client, [0xA; 8], [0; 8]),
            span("B", SpanKind::Client, [0xC; 8], [0; 8]),
        ]);
        assert2::assert!(svc.poll_once(100).await.unwrap() == 2);

        let store_for_restore: Arc<dyn EdgeCheckpointStore> = store.clone();
        let restarted = service().with_checkpoint_store_restoring_all_tenants(&store_for_restore);
        restarted.source.push_batch(vec![
            span("A", SpanKind::Server, [0xB; 8], [0xA; 8]),
            span("B", SpanKind::Server, [0xD; 8], [0xC; 8]),
        ]);
        assert2::assert!(restarted.poll_once(100).await.unwrap() == 2);
        assert2::assert!(restarted.collect_once().await.unwrap() == 2);

        let writes = restarted.sink.writes();
        check!(writes.len() == 2);
        check!(writes.iter().any(|payload| payload.tenant == "A"));
        check!(writes.iter().any(|payload| payload.tenant == "B"));
        check!(store.load_all("A").is_empty());
        check!(store.load_all("B").is_empty());
    }
}
