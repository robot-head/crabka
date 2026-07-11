//! Span source and `remote_write` sink traits.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use crabka_client_consumer::{Consumer, ConsumerRecord};
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    metricsgen::{contract::SpanRecord, series::SeriesPayload},
    span::{AttrValue, KeyValue},
    wal,
};

/// Errors crossing the metrics-generator source/sink boundaries.
#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("transport error: {0}")]
    Transport(String),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("source error: {0}")]
    Source(String),
}

/// Output edge for Prometheus `remote_write` payloads.
#[async_trait]
pub trait RemoteWriteSink: Send + Sync {
    async fn write(&self, payload: &SeriesPayload) -> Result<(), SinkError>;
}

/// Input edge for decoded traces WAL records.
#[async_trait]
pub trait SpanSource: Send + Sync {
    async fn poll(&self, max: usize) -> Result<Vec<SpanRecord>, SinkError>;
    async fn commit(&self) -> Result<(), SinkError>;
}

/// Kafka-backed source for the traces WAL consumer group.
pub struct KafkaSpanSource {
    consumer: AsyncMutex<Consumer>,
    poll_timeout: Duration,
}

impl KafkaSpanSource {
    #[must_use]
    pub fn new(consumer: Consumer) -> Self {
        Self {
            consumer: AsyncMutex::new(consumer),
            poll_timeout: Duration::from_millis(500),
        }
    }

    #[must_use]
    pub fn with_poll_timeout(mut self, poll_timeout: Duration) -> Self {
        self.poll_timeout = poll_timeout;
        self
    }
}

#[async_trait]
impl SpanSource for KafkaSpanSource {
    async fn poll(&self, _max: usize) -> Result<Vec<SpanRecord>, SinkError> {
        let mut consumer = self.consumer.lock().await;
        let records = consumer
            .poll(self.poll_timeout)
            .await
            .map_err(|err| SinkError::Source(err.to_string()))?;
        decode_consumer_records(records)
    }

    async fn commit(&self) -> Result<(), SinkError> {
        self.consumer
            .lock()
            .await
            .commit_sync()
            .await
            .map_err(|err| SinkError::Source(err.to_string()))
    }
}

pub fn decode_consumer_records(records: Vec<ConsumerRecord>) -> Result<Vec<SpanRecord>, SinkError> {
    records
        .into_iter()
        .filter_map(|record| {
            record.value.map(|value| {
                let size_bytes = u64::try_from(value.len()).unwrap_or(u64::MAX);
                wal::SpanRecord::decode(&value)
                    .map(|wal| project_wal_record(wal, size_bytes))
                    .map_err(|err| SinkError::Decode(err.to_string()))
            })
        })
        .collect()
}

#[must_use]
pub fn project_wal_record(record: wal::SpanRecord, size_bytes: u64) -> SpanRecord {
    let service_name = service_name(&record.span.resource_attrs);
    let attributes = record
        .span
        .span_attrs
        .iter()
        .chain(record.span.resource_attrs.iter())
        .filter(|kv| kv.key != "service.name")
        .map(|kv| (kv.key.clone(), attr_value_to_string(&kv.value)))
        .collect();

    SpanRecord {
        tenant: record.tenant,
        trace_id: record.span.trace_id,
        span_id: record.span.span_id,
        parent_span_id: record.span.parent_span_id.unwrap_or([0; 8]),
        name: record.span.name,
        kind: record.span.kind,
        start_ns: record.span.start_ns,
        duration_ns: record.span.duration_ns,
        status: record.span.status,
        status_message: record.span.status_message,
        service_name,
        attributes,
        size_bytes,
    }
}

fn service_name(attrs: &[KeyValue]) -> String {
    attrs
        .iter()
        .find_map(|kv| match (&*kv.key, &kv.value) {
            ("service.name", AttrValue::Str(value)) if !value.is_empty() => Some(value.clone()),
            _ => None,
        })
        .unwrap_or_else(|| "unknown_service".to_string())
}

fn attr_value_to_string(value: &AttrValue) -> String {
    match value {
        AttrValue::Str(value) => value.clone(),
        AttrValue::Int(value) => value.to_string(),
        AttrValue::Double(value) => value.to_string(),
        AttrValue::Bool(value) => value.to_string(),
        AttrValue::Bytes(value) => hex::encode(value),
    }
}

/// Deterministic sink mock that records successful writes.
#[derive(Clone, Default)]
pub struct MockRemoteWriteSink {
    writes: Arc<Mutex<Vec<SeriesPayload>>>,
    fail_next: Arc<Mutex<bool>>,
    fail_after_successes: Arc<Mutex<Option<usize>>>,
}

impl MockRemoteWriteSink {
    pub fn fail_next(&self) {
        *self.fail_next.lock().expect("mock sink mutex poisoned") = true;
    }

    pub fn fail_after_successes(&self, successes: usize) {
        *self
            .fail_after_successes
            .lock()
            .expect("mock sink mutex poisoned") = Some(successes);
    }

    #[must_use]
    pub fn writes(&self) -> Vec<SeriesPayload> {
        self.writes
            .lock()
            .expect("mock sink mutex poisoned")
            .clone()
    }
}

#[async_trait]
impl RemoteWriteSink for MockRemoteWriteSink {
    async fn write(&self, payload: &SeriesPayload) -> Result<(), SinkError> {
        {
            let mut fail_next = self.fail_next.lock().expect("mock sink mutex poisoned");
            if *fail_next {
                *fail_next = false;
                return Err(SinkError::Transport("forced mock failure".into()));
            }
        }
        {
            let successful_writes = self.writes.lock().expect("mock sink mutex poisoned").len();
            let mut fail_after = self
                .fail_after_successes
                .lock()
                .expect("mock sink mutex poisoned");
            if fail_after.is_some_and(|limit| successful_writes >= limit) {
                *fail_after = None;
                return Err(SinkError::Transport("forced mock failure".into()));
            }
        }

        self.writes
            .lock()
            .expect("mock sink mutex poisoned")
            .push(payload.clone());
        Ok(())
    }
}

/// Deterministic source mock that returns scripted batches.
#[derive(Clone, Default)]
pub struct MockSpanSource {
    batches: Arc<Mutex<VecDeque<Vec<SpanRecord>>>>,
    commits: Arc<Mutex<usize>>,
}

impl MockSpanSource {
    pub fn push_batch(&self, batch: Vec<SpanRecord>) {
        self.batches
            .lock()
            .expect("mock source mutex poisoned")
            .push_back(batch);
    }

    #[must_use]
    pub fn commits(&self) -> usize {
        *self.commits.lock().expect("mock source mutex poisoned")
    }
}

#[async_trait]
impl SpanSource for MockSpanSource {
    async fn poll(&self, _max: usize) -> Result<Vec<SpanRecord>, SinkError> {
        Ok(self
            .batches
            .lock()
            .expect("mock source mutex poisoned")
            .pop_front()
            .unwrap_or_default())
    }

    async fn commit(&self) -> Result<(), SinkError> {
        *self.commits.lock().expect("mock source mutex poisoned") += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::{
        metricsgen::{
            contract::{SpanKind, SpanRecord, StatusCode},
            series::{Series, SeriesPayload, SeriesSample},
        },
        span::{AttrValue, KeyValue, Span},
        wal,
    };

    fn payload() -> SeriesPayload {
        SeriesPayload {
            tenant: "t".into(),
            series: vec![Series {
                name: "traces_spanmetrics_calls_total".into(),
                labels: vec![("service".into(), "api".into())],
                sample: SeriesSample::Counter(1.0),
                exemplars: vec![],
                timestamp_ms: 1_000,
            }],
        }
    }

    fn span() -> SpanRecord {
        SpanRecord {
            tenant: "t".into(),
            trace_id: [0; 16],
            span_id: [0; 8],
            parent_span_id: [0; 8],
            name: "op".into(),
            kind: SpanKind::Server,
            start_ns: 0,
            duration_ns: 1,
            status: StatusCode::Ok,
            status_message: String::new(),
            service_name: "api".into(),
            attributes: vec![],
            size_bytes: 0,
        }
    }

    fn wal_span() -> Span {
        Span {
            trace_id: [0xAB; 16],
            span_id: [0xCD; 8],
            parent_span_id: Some([0xEF; 8]),
            name: "GET /checkout".into(),
            kind: SpanKind::Server,
            start_ns: 10,
            duration_ns: 5_000_000,
            status: StatusCode::Ok,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str("checkout".into()),
            }],
            span_attrs: vec![
                KeyValue {
                    key: "db.system".into(),
                    value: AttrValue::Str("postgresql".into()),
                },
                KeyValue {
                    key: "http.status_code".into(),
                    value: AttrValue::Int(200),
                },
            ],
            events: Vec::new(),
            links: Vec::new(),
            instrumentation_scope: "tracer".into(),
            instrumentation_version: "1.2.3".into(),
        }
    }

    #[tokio::test]
    async fn mock_sink_records_writes_and_can_fail_once() {
        let sink = MockRemoteWriteSink::default();
        sink.fail_next();
        check!(sink.write(&payload()).await.is_err());
        check!(sink.write(&payload()).await.is_ok());
        check!(sink.writes().len() == 1);
    }

    #[tokio::test]
    async fn mock_source_returns_scripted_batches_and_tracks_commits() {
        let src = MockSpanSource::default();
        src.push_batch(vec![span(), span()]);
        let batch = src.poll(10).await.unwrap();
        assert2::assert!(batch.len() == 2);
        assert2::assert!(src.poll(10).await.unwrap().is_empty());
        src.commit().await.unwrap();
        assert2::assert!(src.commits() == 1);
    }

    #[test]
    fn wal_record_projects_to_metricsgen_contract() {
        let record = wal::SpanRecord {
            tenant: "tenant-a".into(),
            span: wal_span(),
        };

        let projected = project_wal_record(record, 123);

        assert2::assert!(
            projected
                == SpanRecord {
                    tenant: "tenant-a".into(),
                    trace_id: [0xAB; 16],
                    span_id: [0xCD; 8],
                    parent_span_id: [0xEF; 8],
                    name: "GET /checkout".into(),
                    kind: SpanKind::Server,
                    start_ns: 10,
                    duration_ns: 5_000_000,
                    status: StatusCode::Ok,
                    status_message: String::new(),
                    service_name: "checkout".into(),
                    attributes: vec![
                        ("db.system".into(), "postgresql".into()),
                        ("http.status_code".into(), "200".into()),
                    ],
                    size_bytes: 123,
                }
        );
    }

    #[test]
    fn consumer_records_decode_wal_values_and_skip_tombstones() {
        let record = wal::SpanRecord {
            tenant: "tenant-a".into(),
            span: wal_span(),
        };
        let bytes = record.encode().unwrap();
        let records = vec![
            crabka_client_consumer::ConsumerRecord {
                topic: crate::TRACES_WAL_TOPIC.into(),
                partition: 0,
                offset: 1,
                leader_epoch: -1,
                timestamp: 0,
                key: None,
                value: Some(bytes::Bytes::from(bytes.clone())),
                headers: Vec::new(),
            },
            crabka_client_consumer::ConsumerRecord {
                topic: crate::TRACES_WAL_TOPIC.into(),
                partition: 0,
                offset: 2,
                leader_epoch: -1,
                timestamp: 0,
                key: None,
                value: None,
                headers: Vec::new(),
            },
        ];

        let projected = decode_consumer_records(records).unwrap();

        assert2::assert!(projected.len() == 1);
        check!(projected[0].tenant == "tenant-a");
        check!(projected[0].size_bytes == u64::try_from(bytes.len()).unwrap());
    }
}
