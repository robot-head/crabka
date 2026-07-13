//! Role-selectable metrics service wiring for the Prometheus/Mimir-compatible backend.

// Proving the async service futures `Send` traverses DataFusion's deep
// `sqlparser` AST type graph (reached through `SessionContext` held across
// awaits in the PromQL operator-path evaluation); the default limit is too low.
#![recursion_limit = "256"]

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::Path as StdPath,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

mod ids;

use axum::Router;
use bytes::Bytes;
use crabka_blockstore::{BlockStore, LabelMatcher, Labels};
use crabka_client_consumer::{Consumer, ConsumerRecord};
use crabka_client_producer::{Producer, ProducerRecord};
use crabka_metrics::{CompactionIndexManifest, WalRecord, partition_key};
use crabka_promql::{
    AlertmanagerSink, EngineOpts, ExemplarRecord, InMemoryMetricStore, LabelNameCardinality,
    LabelValueCardinality, MergedMetricStore, MetadataRecord, MetricBlockStore, MetricStore,
    PrometheusApiState, QueryFrontendOptions, RecordingRuleWalSink, RulerAlertState,
    RulerAlertStateRecord, RulerGroupEvaluation, RulerGroupState, RulerGroupStateRecord,
    RulerShard, RulerStateSink, RulerWalError, ScanResult, TsdbBlock, WalHead,
    evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval, prometheus_router,
};
use futures::TryStreamExt;
pub use ids::{Offset, PartitionIndex};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use tokio::{net::TcpListener, task::JoinHandle};
use url::Url;

pub const RULER_STATE_TOPIC: &str = "__crabka_metrics_ruler_state";

#[derive(Debug, thiserror::Error)]
pub enum MetricsServiceError {
    #[error("object store error: {0}")]
    ObjectStore(String),

    #[error("compaction manifest decode failed: {0}")]
    Manifest(String),
}

#[derive(Debug, thiserror::Error)]
pub enum WalHeadReplayError {
    #[error("metrics WAL record at partition {partition} offset {offset} has no value")]
    MissingValue {
        partition: PartitionIndex,
        offset: Offset,
    },

    #[error("metrics WAL record decode failed: {0}")]
    Decode(String),
}

#[derive(Debug, thiserror::Error)]
pub enum WalHeadConsumerError {
    #[error("metrics WAL consumer poll failed: {0}")]
    Poll(String),

    #[error(transparent)]
    Replay(#[from] WalHeadReplayError),

    #[error("metrics WAL consumer commit failed: {0}")]
    Commit(String),
}

#[derive(Debug, thiserror::Error)]
pub enum RulerStateWalRecordError {
    #[error("ruler state record encode failed: {0}")]
    Encode(String),

    #[error("ruler state record decode failed: {0}")]
    Decode(String),
}

#[derive(Debug, thiserror::Error)]
pub enum RulerStateReplayError {
    #[error("ruler state record at partition {partition} offset {offset} has no value")]
    MissingValue {
        partition: PartitionIndex,
        offset: Offset,
    },

    #[error("ruler state record decode failed: {0}")]
    Decode(String),
}

#[derive(Debug, thiserror::Error)]
pub enum RulerStateConsumerError {
    #[error("ruler state consumer poll failed: {0}")]
    Poll(String),

    #[error(transparent)]
    Replay(#[from] RulerStateReplayError),

    #[error("ruler state consumer commit failed: {0}")]
    Commit(String),
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum RulerStateWalRecord {
    Group(RulerGroupStateRecord),
    Alert(RulerAlertStateRecord),
}

impl RulerStateWalRecord {
    ///
    /// # Errors
    /// Returns an error if the operation cannot be completed.
    pub fn encode(&self) -> Result<Vec<u8>, RulerStateWalRecordError> {
        serde_json::to_vec(self)
            .map_err(|error| RulerStateWalRecordError::Encode(error.to_string()))
    }

    ///
    /// # Errors
    /// Returns an error if the operation cannot be completed.
    pub fn decode(bytes: &[u8]) -> Result<Self, RulerStateWalRecordError> {
        serde_json::from_slice(bytes)
            .map_err(|error| RulerStateWalRecordError::Decode(error.to_string()))
    }
}

#[must_use]
pub fn ruler_state_compaction_key(record: &RulerStateWalRecord) -> Bytes {
    match record {
        RulerStateWalRecord::Group(record) => Bytes::from(format!(
            "group\0{}\0{}\0{}",
            record.tenant, record.namespace, record.group
        )),
        RulerStateWalRecord::Alert(record) => {
            let mut key = format!("alert\0{}\0{}", record.tenant, record.rule_id);
            for (name, value) in &record.labels {
                key.push('\0');
                key.push_str(name);
                key.push('=');
                key.push_str(value);
            }
            Bytes::from(key)
        }
    }
}

pub fn apply_ruler_state_record<S: MetricStore>(
    state: &PrometheusApiState<S>,
    record: RulerStateWalRecord,
) {
    match record {
        RulerStateWalRecord::Group(record) => state.apply_ruler_group_state(record),
        RulerStateWalRecord::Alert(record) => state.apply_ruler_alert_state(record),
    }
}

#[tracing::instrument(
    level = "debug",
    name = "metrics.ruler_state.replay",
    skip_all,
    fields(state_topic = %state_topic, records = records.len()),
    err
)]
///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub fn replay_ruler_state_records<S: MetricStore>(
    state: &PrometheusApiState<S>,
    state_topic: &str,
    records: &[WalHeadConsumerRecord],
) -> Result<WalHeadReplayResult, RulerStateReplayError> {
    let mut committed_offsets = BTreeMap::<PartitionIndex, Offset>::new();
    let mut replayed_records = 0;
    for record in records {
        if record.topic != state_topic {
            continue;
        }
        let value = record
            .value
            .as_deref()
            .ok_or(RulerStateReplayError::MissingValue {
                partition: record.partition,
                offset: record.offset,
            })?;
        let state_record = RulerStateWalRecord::decode(value)
            .map_err(|error| RulerStateReplayError::Decode(error.to_string()))?;
        apply_ruler_state_record(state, state_record);
        replayed_records += 1;
        committed_offsets
            .entry(record.partition)
            .and_modify(|offset| *offset = (*offset).max(record.offset + 1))
            .or_insert(record.offset + 1);
    }

    Ok(WalHeadReplayResult {
        polled_records: records.len(),
        replayed_records,
        committed_offsets: committed_offsets
            .into_iter()
            .map(|(partition, offset)| WalHeadPartitionOffset { partition, offset })
            .collect(),
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalHeadConsumerRecord {
    pub topic: String,
    pub partition: PartitionIndex,
    pub offset: Offset,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalHeadPartitionOffset {
    pub partition: PartitionIndex,
    /// Kafka commit offset: the next offset after the last replayed record.
    pub offset: Offset,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WalHeadReplayResult {
    pub polled_records: usize,
    pub replayed_records: usize,
    pub committed_offsets: Vec<WalHeadPartitionOffset>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WalHeadConsumerLoopSummary {
    pub polls: usize,
    pub polled_records: usize,
    pub replayed_records: usize,
    pub committed_offsets: Vec<WalHeadPartitionOffset>,
}

#[tracing::instrument(
    level = "debug",
    name = "metrics.wal_head.replay",
    skip_all,
    fields(wal_topic = %wal_topic, records = records.len()),
    err
)]
///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub fn replay_wal_head_records(
    head: &WalHead,
    wal_topic: &str,
    records: &[WalHeadConsumerRecord],
) -> Result<WalHeadReplayResult, WalHeadReplayError> {
    let mut committed_offsets = BTreeMap::<PartitionIndex, Offset>::new();
    let mut newest_timestamp_ms: Option<i64> = None;
    let mut replayed_records = 0;
    for record in records {
        if record.topic != wal_topic {
            continue;
        }
        let value = record
            .value
            .as_deref()
            .ok_or(WalHeadReplayError::MissingValue {
                partition: record.partition,
                offset: record.offset,
            })?;
        let wal_record = WalRecord::decode(value)
            .map_err(|error| WalHeadReplayError::Decode(error.to_string()))?;
        if let Some(timestamp_ms) = wal_record_max_timestamp_ms(&wal_record) {
            newest_timestamp_ms =
                Some(newest_timestamp_ms.map_or(timestamp_ms, |current| current.max(timestamp_ms)));
        }
        // partition/offset are now the shared crabka_ids types promql also uses,
        // so they pass straight through with no conversion at the seam.
        head.apply_wal_record_at(&wal_record, record.partition, record.offset);
        replayed_records += 1;
        committed_offsets
            .entry(record.partition)
            .and_modify(|offset| *offset = (*offset).max(record.offset + 1))
            .or_insert(record.offset + 1);
    }
    if let Some(timestamp_ms) = newest_timestamp_ms {
        let _ = head.prune(timestamp_ms);
    }

    Ok(WalHeadReplayResult {
        polled_records: records.len(),
        replayed_records,
        committed_offsets: committed_offsets
            .into_iter()
            .map(|(partition, offset)| WalHeadPartitionOffset { partition, offset })
            .collect(),
    })
}

fn wal_record_max_timestamp_ms(record: &WalRecord) -> Option<i64> {
    record
        .payload
        .timestamp_ms()
        .into_iter()
        .chain(
            record
                .exemplars
                .iter()
                .map(|exemplar| exemplar.timestamp_ms),
        )
        .max()
}

#[async_trait::async_trait]
pub trait WalHeadConsumerPoll: Send {
    async fn poll(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<ConsumerRecord>, WalHeadConsumerError>;
}

#[async_trait::async_trait]
pub trait WalHeadConsumerCommit: Send {
    async fn commit_sync(&mut self) -> Result<(), WalHeadConsumerError>;
}

#[async_trait::async_trait]
impl WalHeadConsumerPoll for Consumer {
    async fn poll(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<ConsumerRecord>, WalHeadConsumerError> {
        Consumer::poll(self, timeout)
            .await
            .map_err(|error| WalHeadConsumerError::Poll(error.to_string()))
    }
}

#[async_trait::async_trait]
impl WalHeadConsumerCommit for Consumer {
    async fn commit_sync(&mut self) -> Result<(), WalHeadConsumerError> {
        Consumer::commit_sync(self)
            .await
            .map_err(|error| WalHeadConsumerError::Commit(error.to_string()))
    }
}

#[tracing::instrument(
    level = "debug",
    name = "metrics.wal_head.poll_once",
    skip_all,
    fields(wal_topic = %wal_topic, polled = tracing::field::Empty, replayed = tracing::field::Empty),
    err
)]
///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn poll_wal_head_consumer_once<C>(
    consumer: &mut C,
    head: &WalHead,
    wal_topic: &str,
    timeout: Duration,
) -> Result<WalHeadReplayResult, WalHeadConsumerError>
where
    C: WalHeadConsumerPoll + WalHeadConsumerCommit + ?Sized,
{
    let records = consumer.poll(timeout).await?;
    let replay_records = records
        .into_iter()
        .map(|record| WalHeadConsumerRecord {
            topic: record.topic,
            partition: record.partition.into(),
            offset: record.offset.into(),
            value: record.value.map(|value| value.to_vec()),
        })
        .collect::<Vec<_>>();
    let result = replay_wal_head_records(head, wal_topic, &replay_records)?;
    let span = tracing::Span::current();
    span.record("polled", result.polled_records);
    span.record("replayed", result.replayed_records);
    if result.replayed_records > 0 {
        consumer.commit_sync().await?;
    }
    Ok(result)
}

#[tracing::instrument(
    level = "debug",
    name = "metrics.ruler_state.poll_once",
    skip_all,
    fields(state_topic = %state_topic, polled = tracing::field::Empty, replayed = tracing::field::Empty),
    err
)]
///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn poll_ruler_state_consumer_once<S, C>(
    consumer: &mut C,
    state: &PrometheusApiState<S>,
    state_topic: &str,
    timeout: Duration,
) -> Result<WalHeadReplayResult, RulerStateConsumerError>
where
    S: MetricStore,
    C: WalHeadConsumerPoll + WalHeadConsumerCommit + ?Sized,
{
    let records = consumer
        .poll(timeout)
        .await
        .map_err(|error| RulerStateConsumerError::Poll(error.to_string()))?;
    let replay_records = records
        .into_iter()
        .map(|record| WalHeadConsumerRecord {
            topic: record.topic,
            partition: record.partition.into(),
            offset: record.offset.into(),
            value: record.value.map(|value| value.to_vec()),
        })
        .collect::<Vec<_>>();
    let result = replay_ruler_state_records(state, state_topic, &replay_records)?;
    let span = tracing::Span::current();
    span.record("polled", result.polled_records);
    span.record("replayed", result.replayed_records);
    if result.replayed_records > 0 {
        consumer
            .commit_sync()
            .await
            .map_err(|error| RulerStateConsumerError::Commit(error.to_string()))?;
    }
    Ok(result)
}

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn run_ruler_state_consumer_loop<S, C, Stop>(
    consumer: &mut C,
    state: &PrometheusApiState<S>,
    state_topic: &str,
    timeout: Duration,
    mut should_stop: Stop,
) -> Result<WalHeadConsumerLoopSummary, RulerStateConsumerError>
where
    S: MetricStore,
    C: WalHeadConsumerPoll + WalHeadConsumerCommit + ?Sized,
    Stop: FnMut(&WalHeadConsumerLoopSummary) -> bool,
{
    let mut summary = WalHeadConsumerLoopSummary::default();
    loop {
        let result = poll_ruler_state_consumer_once(consumer, state, state_topic, timeout).await?;
        summary.polls += 1;
        summary.polled_records += result.polled_records;
        summary.replayed_records += result.replayed_records;
        summary.committed_offsets.extend(result.committed_offsets);

        if should_stop(&summary) {
            break;
        }
    }
    Ok(summary)
}

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn run_wal_head_consumer_loop<C, Stop>(
    consumer: &mut C,
    head: &WalHead,
    wal_topic: &str,
    timeout: Duration,
    mut should_stop: Stop,
) -> Result<WalHeadConsumerLoopSummary, WalHeadConsumerError>
where
    C: WalHeadConsumerPoll + WalHeadConsumerCommit + ?Sized,
    Stop: FnMut(&WalHeadConsumerLoopSummary) -> bool,
{
    let mut summary = WalHeadConsumerLoopSummary::default();
    loop {
        let result = poll_wal_head_consumer_once(consumer, head, wal_topic, timeout).await?;
        summary.polls += 1;
        summary.polled_records += result.polled_records;
        summary.replayed_records += result.replayed_records;
        summary.committed_offsets.extend(result.committed_offsets);

        if should_stop(&summary) {
            break;
        }
    }
    Ok(summary)
}

pub fn prometheus_router_for_store<S>(store: S) -> Router
where
    S: MetricStore + 'static,
{
    prometheus_router(prometheus_api_state_for_store(store))
}

pub fn prometheus_api_state_for_store<S>(store: S) -> Arc<PrometheusApiState<S>>
where
    S: MetricStore + 'static,
{
    Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ))
}

pub fn query_frontend_prometheus_router_for_store<S>(store: S, opts: QueryFrontendOptions) -> Router
where
    S: MetricStore + 'static,
{
    prometheus_router(Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default()).with_query_frontend(opts),
    ))
}

pub fn query_frontend_prometheus_router_for_store_with_cache<S, C>(
    store: S,
    opts: QueryFrontendOptions,
    cache: C,
) -> Router
where
    S: MetricStore + 'static,
    C: crabka_promql::RangeQueryCache + 'static,
{
    prometheus_router(Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default())
            .with_query_frontend_cache(opts, Arc::new(cache)),
    ))
}

pub struct PrometheusRulerStateSink<S: MetricStore> {
    state: Arc<PrometheusApiState<S>>,
}

impl<S: MetricStore> PrometheusRulerStateSink<S> {
    #[must_use]
    pub fn new(state: Arc<PrometheusApiState<S>>) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl<S> RulerStateSink for PrometheusRulerStateSink<S>
where
    S: MetricStore + 'static,
{
    async fn persist_ruler_group_state(
        &self,
        record: RulerGroupStateRecord,
    ) -> Result<(), RulerWalError> {
        self.state.apply_ruler_group_state(record);
        Ok(())
    }

    async fn persist_ruler_alert_state(
        &self,
        record: RulerAlertStateRecord,
    ) -> Result<(), RulerWalError> {
        self.state.apply_ruler_alert_state(record);
        Ok(())
    }
}

pub struct KafkaRulerStateSink {
    producer: Arc<Producer>,
    topic: String,
}

impl KafkaRulerStateSink {
    #[must_use]
    pub fn new(producer: Arc<Producer>, topic: impl Into<String>) -> Self {
        Self {
            producer,
            topic: topic.into(),
        }
    }

    async fn append_state_record(&self, record: RulerStateWalRecord) -> Result<(), RulerWalError> {
        let key = ruler_state_compaction_key(&record);
        let value = record
            .encode()
            .map_err(|error| RulerWalError::Append(error.to_string()))?;
        let ack = self
            .producer
            .send(ProducerRecord {
                topic: self.topic.clone(),
                partition: None,
                key: Some(key),
                value: Some(Bytes::from(value)),
                ..Default::default()
            })
            .await;
        ack.await
            .map_err(|error| RulerWalError::Append(error.to_string()))?
            .map_err(|error| RulerWalError::Append(error.to_string()))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl RulerStateSink for KafkaRulerStateSink {
    async fn persist_ruler_group_state(
        &self,
        record: RulerGroupStateRecord,
    ) -> Result<(), RulerWalError> {
        self.append_state_record(RulerStateWalRecord::Group(record))
            .await
    }

    async fn persist_ruler_alert_state(
        &self,
        record: RulerAlertStateRecord,
    ) -> Result<(), RulerWalError> {
        self.append_state_record(RulerStateWalRecord::Alert(record))
            .await
    }
}

pub struct RulerStateFanoutSink<A, B> {
    first: A,
    second: B,
}

impl<A, B> RulerStateFanoutSink<A, B> {
    #[must_use]
    pub fn new(first: A, second: B) -> Self {
        Self { first, second }
    }
}

#[async_trait::async_trait]
impl<A, B> RulerStateSink for RulerStateFanoutSink<A, B>
where
    A: RulerStateSink,
    B: RulerStateSink,
{
    async fn persist_ruler_group_state(
        &self,
        record: RulerGroupStateRecord,
    ) -> Result<(), RulerWalError> {
        self.first.persist_ruler_group_state(record.clone()).await?;
        self.second.persist_ruler_group_state(record).await
    }

    async fn persist_ruler_alert_state(
        &self,
        record: RulerAlertStateRecord,
    ) -> Result<(), RulerWalError> {
        self.first.persist_ruler_alert_state(record.clone()).await?;
        self.second.persist_ruler_alert_state(record).await
    }
}

pub struct KafkaRecordingRuleWalSink {
    producer: Arc<Producer>,
    topic: String,
}

impl KafkaRecordingRuleWalSink {
    #[must_use]
    pub fn new(producer: Arc<Producer>, topic: impl Into<String>) -> Self {
        Self {
            producer,
            topic: topic.into(),
        }
    }
}

#[async_trait::async_trait]
impl RecordingRuleWalSink for KafkaRecordingRuleWalSink {
    async fn append_recording_rule_record(&self, record: WalRecord) -> Result<(), RulerWalError> {
        let value = record
            .encode()
            .map_err(|error| RulerWalError::Append(error.to_string()))?;
        let key = partition_key(&record.tenant, record.series_fingerprint());
        let ack = self
            .producer
            .send(ProducerRecord {
                topic: self.topic.clone(),
                partition: None,
                key: Some(key),
                value: Some(Bytes::from(value)),
                ..Default::default()
            })
            .await;
        ack.await
            .map_err(|error| RulerWalError::Append(error.to_string()))?
            .map_err(|error| RulerWalError::Append(error.to_string()))?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopAlertmanagerSink;

#[async_trait::async_trait]
impl AlertmanagerSink for NoopAlertmanagerSink {
    async fn dispatch_alerts(
        &self,
        alerts: Vec<crabka_promql::AlertmanagerAlert>,
    ) -> Result<(), RulerWalError> {
        if !alerts.is_empty() {
            tracing::warn!(
                alert_count = alerts.len(),
                "ruler alertmanager sink is not configured; dropping alerts"
            );
        }
        Ok(())
    }
}

pub struct AlertmanagerHttpSink {
    client: reqwest::Client,
    endpoint: String,
}

impl AlertmanagerHttpSink {
    #[must_use]
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoint: endpoint.into(),
        }
    }
}

#[async_trait::async_trait]
impl AlertmanagerSink for AlertmanagerHttpSink {
    async fn dispatch_alerts(
        &self,
        alerts: Vec<crabka_promql::AlertmanagerAlert>,
    ) -> Result<(), RulerWalError> {
        if alerts.is_empty() {
            return Ok(());
        }
        let response = self
            .client
            .post(&self.endpoint)
            .json(&alertmanager_payload(alerts))
            .send()
            .await
            .map_err(|error| RulerWalError::Append(error.to_string()))?;
        if !response.status().is_success() {
            return Err(RulerWalError::Append(format!(
                "alertmanager dispatch returned HTTP {}",
                response.status()
            )));
        }
        Ok(())
    }
}

pub enum RulerAlertmanagerSink {
    Http(AlertmanagerHttpSink),
    Noop(NoopAlertmanagerSink),
}

impl RulerAlertmanagerSink {
    #[must_use]
    pub fn from_endpoint(endpoint: Option<String>) -> Self {
        endpoint.map_or(Self::Noop(NoopAlertmanagerSink), |endpoint| {
            Self::Http(AlertmanagerHttpSink::new(endpoint))
        })
    }
}

#[async_trait::async_trait]
impl AlertmanagerSink for RulerAlertmanagerSink {
    async fn dispatch_alerts(
        &self,
        alerts: Vec<crabka_promql::AlertmanagerAlert>,
    ) -> Result<(), RulerWalError> {
        match self {
            Self::Http(sink) => sink.dispatch_alerts(alerts).await,
            Self::Noop(sink) => sink.dispatch_alerts(alerts).await,
        }
    }
}

fn alertmanager_payload(alerts: Vec<crabka_promql::AlertmanagerAlert>) -> serde_json::Value {
    serde_json::Value::Array(
        alerts
            .into_iter()
            .map(|alert| {
                serde_json::json!({
                    "labels": alert.labels,
                    "annotations": alert.annotations,
                    "startsAt": unix_ms_to_rfc3339(alert.starts_at_ms),
                    "endsAt": alert.ends_at_ms.map(unix_ms_to_rfc3339),
                    "generatorURL": alert.generator_url,
                })
            })
            .collect(),
    )
}

fn unix_ms_to_rfc3339(timestamp_ms: i64) -> String {
    use time::format_description::well_known::Rfc3339;

    let Ok(time) =
        time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamp_ms) * 1_000_000)
    else {
        return "1970-01-01T00:00:00Z".to_string();
    };
    time.format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

#[tracing::instrument(
    level = "info",
    name = "metrics.ruler.evaluate_once",
    skip_all,
    fields(tenant = %tenant, eval_time_ms),
    err
)]
///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn evaluate_ruler_once<S, W, A, R>(
    state: &Arc<PrometheusApiState<S>>,
    sinks: (&W, &A, &R),
    alert_state: &mut RulerAlertState,
    group_state: &mut RulerGroupState,
    tenant: &str,
    shard: RulerShard,
    eval_time_ms: i64,
) -> Result<RulerGroupEvaluation, crabka_promql::PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    let (wal_sink, alert_sink, state_sink) = sinks;
    state.set_ruler_evaluation_time_ms(eval_time_ms);
    let rules = state.ruler_rule_set(tenant);
    let engine = state.engine_for_tenant(tenant);
    evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval(
        &engine,
        (wal_sink, alert_sink, state_sink),
        alert_state,
        tenant,
        &rules,
        (group_state, shard, eval_time_ms),
    )
    .await
}

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn run_ruler_evaluation_loop<S, W, A, R, Stop>(
    state: Arc<PrometheusApiState<S>>,
    sinks: (W, A, R),
    tenant: String,
    shard: RulerShard,
    interval: Duration,
    mut should_stop: Stop,
) -> Result<(), crabka_promql::PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
    Stop: FnMut() -> bool,
{
    let (wal_sink, alert_sink, state_sink) = sinks;
    let mut alert_state = RulerAlertState::default();
    let mut group_state = RulerGroupState::default();
    loop {
        let eval_time_ms = current_time_ms();
        evaluate_ruler_once(
            &state,
            (&wal_sink, &alert_sink, &state_sink),
            &mut alert_state,
            &mut group_state,
            &tenant,
            shard,
            eval_time_ms,
        )
        .await?;

        if should_stop() {
            break;
        }
        tokio::time::sleep(interval).await;
    }
    Ok(())
}

fn current_time_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_millis().min(i64::MAX as u128)).unwrap_or(i64::MAX)
        })
}

pub fn in_memory_prometheus_router() -> Router {
    prometheus_router_for_store(InMemoryMetricStore::new())
}

pub fn refreshing_blockstore_prometheus_router(
    store: Arc<dyn ObjectStore>,
    base: Url,
    manifest_prefix: impl Into<String>,
) -> Router {
    refreshing_blockstore_prometheus_router_with_hot_store(
        store,
        base,
        manifest_prefix,
        WalHead::new(),
    )
}

pub fn refreshing_blockstore_prometheus_router_with_hot_store(
    store: Arc<dyn ObjectStore>,
    base: Url,
    manifest_prefix: impl Into<String>,
    hot_store: WalHead,
) -> Router {
    prometheus_router_for_store(RefreshingMetricBlockStore::new(
        store,
        base,
        manifest_prefix,
        hot_store,
    ))
}

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn blockstore_prometheus_router(
    store: Arc<dyn ObjectStore>,
    base: Url,
    manifest_prefix: &str,
) -> Result<Router, MetricsServiceError> {
    let manifests = load_compaction_manifests(store.clone(), manifest_prefix).await?;
    let metric_store = MetricBlockStore::from_compaction_manifests(
        BlockStore::new(store.clone(), base.clone()),
        Some(BlockStore::new(store, base)),
        &manifests,
    );
    Ok(prometheus_router_for_store(metric_store))
}

pub struct RefreshingMetricBlockStore {
    store: Arc<dyn ObjectStore>,
    base: Url,
    manifest_prefix: String,
    hot_store: WalHead,
    manifest_cache: Arc<tokio::sync::RwLock<BTreeMap<String, CompactionIndexManifest>>>,
    cold_cache: Arc<tokio::sync::RwLock<Option<CachedMetricBlockStore>>>,
    cold_refresh: tokio::sync::Mutex<()>,
}

struct CachedMetricBlockStore {
    cached_at: Instant,
    start_ms: i64,
    end_ms: i64,
    cold: MetricBlockStore,
}

/// Lookback substituted for an unbounded (`i64::MIN..i64::MAX`) query range so
/// metadata-style requests don't force a full cold-manifest scan.
const UNBOUNDED_COMPATIBILITY_LOOKBACK: Duration = Duration::from_hours(1);

/// How long a cached cold-block store snapshot is served before manifests are
/// re-listed from the object store.
const COLD_CACHE_TTL: Duration = Duration::from_secs(30);

impl CachedMetricBlockStore {
    fn covers(&self, start_ms: i64, end_ms: i64, ttl: Duration) -> bool {
        self.cached_at.elapsed() < ttl && self.start_ms <= start_ms && self.end_ms >= end_ms
    }
}

impl RefreshingMetricBlockStore {
    #[must_use]
    pub fn new(
        store: Arc<dyn ObjectStore>,
        base: Url,
        manifest_prefix: impl Into<String>,
        hot_store: WalHead,
    ) -> Self {
        Self {
            store,
            base,
            manifest_prefix: manifest_prefix.into(),
            hot_store,
            manifest_cache: Arc::new(tokio::sync::RwLock::new(BTreeMap::new())),
            cold_cache: Arc::new(tokio::sync::RwLock::new(None)),
            cold_refresh: tokio::sync::Mutex::new(()),
        }
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.current_store",
        skip_all,
        fields(start_ms, end_ms, cold_refreshed = tracing::field::Empty),
        err
    )]
    async fn current_store(
        &self,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<MergedMetricStore<MetricBlockStore, WalHead>, MetricsServiceError> {
        let (start_ms, end_ms) = normalize_refresh_range(start_ms, end_ms);

        {
            let guard = self.cold_cache.read().await;
            if let Some(entry) = guard.as_ref()
                && entry.covers(start_ms, end_ms, COLD_CACHE_TTL)
            {
                return Ok(MergedMetricStore::new(
                    entry.cold.clone(),
                    self.hot_store.clone(),
                ));
            }
        }

        let _refresh_guard = self.cold_refresh.lock().await;
        {
            let guard = self.cold_cache.read().await;
            if let Some(entry) = guard.as_ref()
                && entry.covers(start_ms, end_ms, COLD_CACHE_TTL)
            {
                return Ok(MergedMetricStore::new(
                    entry.cold.clone(),
                    self.hot_store.clone(),
                ));
            }
        }

        let manifests = load_compaction_manifests_for_range_with_cache(
            self.store.clone(),
            &self.manifest_prefix,
            start_ms,
            end_ms,
            &self.manifest_cache,
        )
        .await?;
        let cold = MetricBlockStore::from_compaction_manifests(
            BlockStore::new(self.store.clone(), self.base.clone()),
            Some(BlockStore::new(self.store.clone(), self.base.clone())),
            &manifests,
        );
        let merged = MergedMetricStore::new(cold.clone(), self.hot_store.clone());
        *self.cold_cache.write().await = Some(CachedMetricBlockStore {
            cached_at: Instant::now(),
            start_ms,
            end_ms,
            cold,
        });
        tracing::Span::current().record("cold_refreshed", true);
        Ok(merged)
    }
}

fn normalize_refresh_range(start_ms: i64, end_ms: i64) -> (i64, i64) {
    if start_ms == i64::MIN && end_ms == i64::MAX {
        return (
            unix_time_ms().saturating_sub(duration_ms(UNBOUNDED_COMPATIBILITY_LOOKBACK)),
            i64::MAX,
        );
    }
    (start_ms, end_ms)
}

fn unix_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, duration_ms)
}

fn duration_ms(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

#[async_trait::async_trait]
impl MetricStore for RefreshingMetricBlockStore {
    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.scan",
        skip_all,
        fields(tenant = %tenant, matchers = matchers.len(), start_ms, end_ms),
        err
    )]
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ScanResult, crabka_promql::PromqlError> {
        self.current_store(start_ms, end_ms)
            .await?
            .scan(tenant, matchers, start_ms, end_ms)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.label_names",
        skip_all,
        fields(tenant = %tenant, matchers = matchers.len(), start_ms, end_ms),
        err
    )]
    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, crabka_promql::PromqlError> {
        self.current_store(start_ms, end_ms)
            .await?
            .label_names(tenant, matchers, start_ms, end_ms)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.label_values",
        skip_all,
        fields(tenant = %tenant, label = %name, matchers = matchers.len(), start_ms, end_ms),
        err
    )]
    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, crabka_promql::PromqlError> {
        self.current_store(start_ms, end_ms)
            .await?
            .label_values(tenant, name, matchers, start_ms, end_ms)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.series",
        skip_all,
        fields(tenant = %tenant, matchers = matchers.len(), start_ms, end_ms),
        err
    )]
    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Labels>, crabka_promql::PromqlError> {
        self.current_store(start_ms, end_ms)
            .await?
            .series(tenant, matchers, start_ms, end_ms)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.exemplars",
        skip_all,
        fields(tenant = %tenant, matchers = matchers.len(), start_ms, end_ms),
        err
    )]
    async fn exemplars(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<ExemplarRecord>, crabka_promql::PromqlError> {
        self.current_store(start_ms, end_ms)
            .await?
            .exemplars(tenant, matchers, start_ms, end_ms)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.metadata",
        skip_all,
        fields(tenant = %tenant, metric = metric.unwrap_or("")),
        err
    )]
    async fn metadata(
        &self,
        tenant: &str,
        metric: Option<&str>,
    ) -> Result<Vec<MetadataRecord>, crabka_promql::PromqlError> {
        self.current_store(i64::MIN, i64::MAX)
            .await?
            .metadata(tenant, metric)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.cardinality_label_names",
        skip_all,
        fields(tenant = %tenant),
        err
    )]
    async fn cardinality_label_names(
        &self,
        tenant: &str,
    ) -> Result<Vec<LabelNameCardinality>, crabka_promql::PromqlError> {
        self.current_store(i64::MIN, i64::MAX)
            .await?
            .cardinality_label_names(tenant)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.cardinality_label_values",
        skip_all,
        fields(tenant = %tenant),
        err
    )]
    async fn cardinality_label_values(
        &self,
        tenant: &str,
    ) -> Result<Vec<LabelValueCardinality>, crabka_promql::PromqlError> {
        self.current_store(i64::MIN, i64::MAX)
            .await?
            .cardinality_label_values(tenant)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.cardinality_active_series",
        skip_all,
        fields(tenant = %tenant),
        err
    )]
    async fn cardinality_active_series(
        &self,
        tenant: &str,
    ) -> Result<Vec<Labels>, crabka_promql::PromqlError> {
        self.current_store(i64::MIN, i64::MAX)
            .await?
            .cardinality_active_series(tenant)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.tsdb_stats",
        skip_all,
        fields(tenant = %tenant),
        err
    )]
    async fn tsdb_stats(
        &self,
        tenant: &str,
    ) -> Result<crabka_promql::TsdbStats, crabka_promql::PromqlError> {
        self.current_store(i64::MIN, i64::MAX)
            .await?
            .tsdb_stats(tenant)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.tsdb_blocks",
        skip_all,
        fields(tenant = %tenant),
        err
    )]
    async fn tsdb_blocks(
        &self,
        tenant: &str,
    ) -> Result<Vec<TsdbBlock>, crabka_promql::PromqlError> {
        self.current_store(i64::MIN, i64::MAX)
            .await?
            .tsdb_blocks(tenant)
            .await
    }
}

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn load_compaction_manifests(
    store: Arc<dyn ObjectStore>,
    manifest_prefix: &str,
) -> Result<Vec<CompactionIndexManifest>, MetricsServiceError> {
    load_compaction_manifests_filtered(store, manifest_prefix, None).await
}

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn load_compaction_manifests_for_range(
    store: Arc<dyn ObjectStore>,
    manifest_prefix: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<CompactionIndexManifest>, MetricsServiceError> {
    load_compaction_manifests_filtered(store, manifest_prefix, Some((start_ms, end_ms))).await
}

async fn load_compaction_manifests_for_range_with_cache(
    store: Arc<dyn ObjectStore>,
    manifest_prefix: &str,
    start_ms: i64,
    end_ms: i64,
    cache: &tokio::sync::RwLock<BTreeMap<String, CompactionIndexManifest>>,
) -> Result<Vec<CompactionIndexManifest>, MetricsServiceError> {
    load_compaction_manifests_filtered_with_cache(
        store,
        manifest_prefix,
        Some((start_ms, end_ms)),
        Some(cache),
    )
    .await
}

async fn load_compaction_manifests_filtered(
    store: Arc<dyn ObjectStore>,
    manifest_prefix: &str,
    time_range: Option<(i64, i64)>,
) -> Result<Vec<CompactionIndexManifest>, MetricsServiceError> {
    load_compaction_manifests_filtered_with_cache(store, manifest_prefix, time_range, None).await
}

#[tracing::instrument(
    level = "debug",
    name = "metrics.manifests.load",
    skip_all,
    fields(prefix = %manifest_prefix, manifests = tracing::field::Empty),
    err
)]
async fn load_compaction_manifests_filtered_with_cache(
    store: Arc<dyn ObjectStore>,
    manifest_prefix: &str,
    time_range: Option<(i64, i64)>,
    cache: Option<&tokio::sync::RwLock<BTreeMap<String, CompactionIndexManifest>>>,
) -> Result<Vec<CompactionIndexManifest>, MetricsServiceError> {
    let prefix = (!manifest_prefix.is_empty()).then(|| Path::from(manifest_prefix));
    let mut objects = store.list(prefix.as_ref()).try_collect::<Vec<_>>().await?;
    objects.sort_by(|left, right| left.location.cmp(&right.location));

    let objects = objects
        .into_iter()
        .filter(|object| {
            let key = object.location.as_ref();
            StdPath::new(key)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("index"))
        })
        .collect::<Vec<_>>();
    let live_keys = objects
        .iter()
        .map(|object| object.location.as_ref().to_string())
        .collect::<BTreeSet<_>>();
    let mut manifests = Vec::new();
    let mut fetched = Vec::<(String, CompactionIndexManifest)>::new();
    for object in objects {
        let key = object.location.as_ref();
        let manifest = if let Some(cache) = cache
            && let Some(manifest) = cache.read().await.get(key).cloned()
        {
            manifest
        } else {
            let bytes = store.get(&object.location).await?.bytes().await?;
            let manifest = CompactionIndexManifest::decode(&bytes)?;
            fetched.push((key.to_string(), manifest.clone()));
            manifest
        };
        if time_range.is_none_or(|(start_ms, end_ms)| {
            manifest.max_ts >= start_ms && manifest.min_ts <= end_ms
        }) {
            manifests.push(manifest);
        }
    }
    if let Some(cache) = cache {
        let mut guard = cache.write().await;
        guard.retain(|key, _| live_keys.contains(key));
        guard.extend(fetched);
    }
    tracing::Span::current().record("manifests", manifests.len());
    Ok(manifests)
}

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn serve_in_memory_prometheus(
    addr: SocketAddr,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    serve_prometheus_router(addr, in_memory_prometheus_router(), shutdown).await
}

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn serve_prometheus_router(
    addr: SocketAddr,
    router: Router,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    let (bound, _server) = serve_prometheus_router_joinable(addr, router, shutdown).await?;
    Ok(bound)
}

/// Like [`serve_prometheus_router`], but hands the spawned server task back to the
/// caller. Awaiting the returned [`JoinHandle`] after signalling `shutdown` lets the
/// process drain in-flight requests (axum's `with_graceful_shutdown`) before exiting,
/// rather than dropping the task detached. Used by the long-running service binaries,
/// which join the handle before returning from their `run_*` entry points.
///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn serve_prometheus_router_joinable(
    addr: SocketAddr,
    router: Router,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<(SocketAddr, JoinHandle<()>)> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    let server = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router)
            .with_graceful_shutdown(shutdown)
            .await
        {
            tracing::warn!(%error, "metrics prometheus server stopped with error");
        }
    });
    Ok((bound, server))
}

impl From<object_store::Error> for MetricsServiceError {
    fn from(error: object_store::Error) -> Self {
        Self::ObjectStore(error.to_string())
    }
}

impl From<crabka_metrics::CompactionIndexError> for MetricsServiceError {
    fn from(error: crabka_metrics::CompactionIndexError) -> Self {
        Self::Manifest(error.to_string())
    }
}

impl From<MetricsServiceError> for crabka_promql::PromqlError {
    fn from(error: MetricsServiceError) -> Self {
        Self::Store(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{SystemTime, UNIX_EPOCH},
    };

    use assert2::check;
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use bytes::Bytes;
    use crabka_client_consumer::ConsumerRecord;
    use crabka_promql::{AlertmanagerSink, MetricStore};
    use futures::{StreamExt, stream::BoxStream};
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
    };
    use tower::ServiceExt;

    struct RecordingWalHeadConsumer {
        batches: Vec<Vec<ConsumerRecord>>,
        commit_calls: usize,
    }

    #[async_trait::async_trait]
    impl super::WalHeadConsumerPoll for RecordingWalHeadConsumer {
        async fn poll(
            &mut self,
            _timeout: std::time::Duration,
        ) -> Result<Vec<ConsumerRecord>, super::WalHeadConsumerError> {
            Ok(self.batches.remove(0))
        }
    }

    #[async_trait::async_trait]
    impl super::WalHeadConsumerCommit for RecordingWalHeadConsumer {
        async fn commit_sync(&mut self) -> Result<(), super::WalHeadConsumerError> {
            self.commit_calls += 1;
            Ok(())
        }
    }

    fn consumer_record(
        topic: &str,
        partition: i32,
        offset: i64,
        value: Option<Vec<u8>>,
    ) -> ConsumerRecord {
        ConsumerRecord {
            topic: topic.to_string(),
            partition,
            offset,
            leader_epoch: -1,
            timestamp: 0,
            key: None,
            value: value.map(Bytes::from),
            headers: Vec::new(),
        }
    }

    struct CountingObjectStore {
        inner: Arc<InMemory>,
        list_calls: Arc<AtomicUsize>,
        get_calls: Arc<AtomicUsize>,
        list_delay: std::time::Duration,
    }

    impl CountingObjectStore {
        fn new(list_calls: Arc<AtomicUsize>, list_delay: std::time::Duration) -> Self {
            Self {
                inner: Arc::new(InMemory::new()),
                list_calls,
                get_calls: Arc::new(AtomicUsize::new(0)),
                list_delay,
            }
        }
    }

    impl std::fmt::Debug for CountingObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("CountingObjectStore")
        }
    }

    impl std::fmt::Display for CountingObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("CountingObjectStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingObjectStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if std::path::Path::new(location.as_ref())
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("index"))
            {
                self.get_calls.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            let delay = self.list_delay;
            Box::pin(self.inner.list(prefix).then(move |item| async move {
                tokio::time::sleep(delay).await;
                item
            }))
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn in_memory_router_serves_prometheus_query_api() {
        let response = super::in_memory_prometheus_router()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/query?query=vector(1)&time=10")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let status_is_success = response.status().is_success();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(status_is_success);
        assert2::assert!(body["status"].as_str() == Some("success"));
        assert2::assert!(body["data"]["resultType"].as_str() == Some("vector"));
    }

    #[tokio::test]
    async fn in_memory_router_serves_mimir_prefixed_query_api() {
        let response = super::in_memory_prometheus_router()
            .oneshot(
                Request::builder()
                    .uri("/prometheus/api/v1/query?query=vector(1)&time=10")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let status_is_success = response.status().is_success();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(status_is_success);
        assert2::assert!(body["status"].as_str() == Some("success"));
        assert2::assert!(body["data"]["resultType"].as_str() == Some("vector"));
    }

    #[tokio::test]
    async fn router_for_store_serves_samples_from_supplied_store() {
        let mut store = crabka_promql::InMemoryMetricStore::new();
        let mut labels = crabka_blockstore::Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", "api");
        store.push_float("tenant-a", labels, 10_000, 1.0);

        let response = super::prometheus_router_for_store(store)
            .oneshot(
                Request::builder()
                    .uri("/api/v1/query?query=up&time=10")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let status_is_success = response.status().is_success();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(status_is_success);
        assert2::assert!(body["data"]["result"][0]["metric"]["job"].as_str() == Some("api"));
        assert2::assert!(body["data"]["result"][0]["value"][1].as_str() == Some("1"));
    }

    #[tokio::test]
    async fn query_frontend_router_serves_split_range_query() {
        let mut store = crabka_promql::InMemoryMetricStore::new();
        let mut labels = crabka_blockstore::Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", "api");
        for (ts_ms, value) in [(0, 1.0), (60_000, 2.0), (120_000, 3.0)] {
            store.push_float("tenant-a", labels.clone(), ts_ms, value);
        }

        let response = super::query_frontend_prometheus_router_for_store(
            store,
            crabka_promql::QueryFrontendOptions {
                split_interval_ms: 60_000,
                shard_count: 1,
            },
        )
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=up&start=0&end=120&step=60")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

        let status_is_success = response.status().is_success();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(status_is_success);
        assert2::assert!(body["data"]["resultType"].as_str() == Some("matrix"));
        assert2::assert!(
            body["data"]["result"][0]["values"]
                .as_array()
                .unwrap()
                .len()
                == 3
        );
    }

    #[derive(Default)]
    struct RecordingRulerWalSink {
        records: std::sync::Mutex<Vec<crabka_metrics::WalRecord>>,
    }

    impl RecordingRulerWalSink {
        fn records(&self) -> Vec<crabka_metrics::WalRecord> {
            self.records
                .lock()
                .expect("recording ruler sink poisoned")
                .clone()
        }
    }

    #[async_trait::async_trait]
    impl crabka_promql::RecordingRuleWalSink for RecordingRulerWalSink {
        async fn append_recording_rule_record(
            &self,
            record: crabka_metrics::WalRecord,
        ) -> Result<(), crabka_promql::RulerWalError> {
            self.records
                .lock()
                .expect("recording ruler sink poisoned")
                .push(record);
            Ok(())
        }
    }

    struct RecordingAlertmanagerSink;

    #[async_trait::async_trait]
    impl crabka_promql::AlertmanagerSink for RecordingAlertmanagerSink {
        async fn dispatch_alerts(
            &self,
            _alerts: Vec<crabka_promql::AlertmanagerAlert>,
        ) -> Result<(), crabka_promql::RulerWalError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn ruler_evaluation_reads_api_rules_and_appends_recording_wal_records() {
        let mut store = crabka_promql::InMemoryMetricStore::new();
        let mut labels = crabka_blockstore::Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", "api");
        store.push_float("tenant-a", labels, 10_000, 1.0);
        let state = super::prometheus_api_state_for_store(store);
        let router = crabka_promql::prometheus_router(std::sync::Arc::clone(&state));

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/prometheus/config/v1/rules/team-a")
                    .header("x-scope-orgid", "tenant-a")
                    .header("content-type", "application/yaml")
                    .body(Body::from(
                        r"
name: recording
interval: 1m
rules:
  - record: job:up:sum
    expr: sum by (job) (up)
",
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert2::assert!(response.status().is_success());

        let wal_sink = RecordingRulerWalSink::default();
        let alert_sink = RecordingAlertmanagerSink;
        let state_sink = super::PrometheusRulerStateSink::new(std::sync::Arc::clone(&state));
        let mut alert_state = crabka_promql::RulerAlertState::default();
        let mut group_state = crabka_promql::RulerGroupState::default();
        let evaluation = super::evaluate_ruler_once(
            &state,
            (&wal_sink, &alert_sink, &state_sink),
            &mut alert_state,
            &mut group_state,
            "tenant-a",
            crabka_promql::RulerShard::new(1, 1).unwrap(),
            10_000,
        )
        .await
        .unwrap();

        assert2::assert!(evaluation.recording_records == 1);
        let records = wal_sink.records();
        let record_labels = records[0].labels();
        assert2::assert!(records.len() == 1);
        assert2::assert!(records[0].tenant.as_str() == "tenant-a");
        assert2::assert!(record_labels.get("__name__") == Some("job:up:sum"));
        assert2::assert!(record_labels.get("job") == Some("api"));
        assert2::assert!(matches!(
            records[0].payload,
            crabka_metrics::SamplePayload::Float { value, .. } if (value - 1.0).abs() < f64::EPSILON
        ));
    }

    #[tokio::test]
    async fn ruler_evaluation_applies_runtime_max_samples_per_query() {
        let mut store = crabka_promql::InMemoryMetricStore::new();
        let mut api_labels = crabka_blockstore::Labels::new();
        api_labels.insert("__name__", "up");
        api_labels.insert("job", "api");
        store.push_float("tenant-a", api_labels, 10_000, 1.0);
        let mut web_labels = crabka_blockstore::Labels::new();
        web_labels.insert("__name__", "up");
        web_labels.insert("job", "web");
        store.push_float("tenant-a", web_labels, 10_000, 1.0);
        let limits = crabka_metrics::Limits {
            max_samples_per_query: 1,
            ..crabka_metrics::Limits::default()
        };
        let state = std::sync::Arc::new(
            crabka_promql::PrometheusApiState::new(
                std::sync::Arc::new(store),
                crabka_promql::EngineOpts::default(),
            )
            .with_query_limits(crabka_metrics::OverridesProvider::new(limits)),
        );
        let router = crabka_promql::prometheus_router(std::sync::Arc::clone(&state));

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/prometheus/config/v1/rules/team-a")
                    .header("x-scope-orgid", "tenant-a")
                    .header("content-type", "application/yaml")
                    .body(Body::from(
                        r"
name: recording
interval: 1m
rules:
  - record: job:up:sum
    expr: sum(up)
",
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert2::assert!(response.status().is_success());

        let wal_sink = RecordingRulerWalSink::default();
        let alert_sink = RecordingAlertmanagerSink;
        let state_sink = super::PrometheusRulerStateSink::new(std::sync::Arc::clone(&state));
        let mut alert_state = crabka_promql::RulerAlertState::default();
        let mut group_state = crabka_promql::RulerGroupState::default();
        let error = super::evaluate_ruler_once(
            &state,
            (&wal_sink, &alert_sink, &state_sink),
            &mut alert_state,
            &mut group_state,
            "tenant-a",
            crabka_promql::RulerShard::new(1, 1).unwrap(),
            10_000,
        )
        .await
        .unwrap_err();

        assert2::assert!(format!("{error}").contains("query exceeds max_samples=1"));
        assert2::assert!(wal_sink.records().is_empty());
    }

    #[tokio::test]
    async fn alertmanager_http_sink_posts_v2_alert_payloads() {
        let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let received_for_route = std::sync::Arc::clone(&received);
        let router = axum::Router::new().route(
            "/api/v2/alerts",
            axum::routing::post(move |body: bytes::Bytes| {
                let received = std::sync::Arc::clone(&received_for_route);
                async move {
                    received
                        .lock()
                        .expect("received alerts poisoned")
                        .push(body.to_vec());
                    axum::http::StatusCode::OK
                }
            }),
        );
        let bound = super::serve_prometheus_router("127.0.0.1:0".parse().unwrap(), router, async {
            std::future::pending::<()>().await;
        })
        .await
        .unwrap();

        let sink = super::AlertmanagerHttpSink::new(format!("http://{bound}/api/v2/alerts"));
        sink.dispatch_alerts(vec![crabka_promql::AlertmanagerAlert {
            labels: std::collections::BTreeMap::from([
                ("alertname".to_string(), "InstanceDown".to_string()),
                ("severity".to_string(), "page".to_string()),
            ]),
            annotations: std::collections::BTreeMap::from([(
                "summary".to_string(),
                "instance is down".to_string(),
            )]),
            starts_at_ms: 60_000,
            ends_at_ms: None,
            generator_url: "http://crabka.example/graph".to_string(),
        }])
        .await
        .unwrap();

        let bodies = received.lock().expect("received alerts poisoned");
        assert2::assert!(bodies.len() == 1);
        let body: serde_json::Value = serde_json::from_slice(&bodies[0]).unwrap();
        let expected = serde_json::json!([{
            "labels": {
                "alertname": "InstanceDown",
                "severity": "page",
            },
            "annotations": {
                "summary": "instance is down",
            },
            "startsAt": "1970-01-01T00:01:00Z",
            "endsAt": null,
            "generatorURL": "http://crabka.example/graph",
        }]);
        assert2::assert!(body == expected);
    }

    #[test]
    fn ruler_state_records_round_trip_with_compacted_keys() {
        let group = crabka_promql::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-a".to_string(),
            group: "recording".to_string(),
            last_eval_ms: 60_000,
        };
        let group_record = super::RulerStateWalRecord::Group(group.clone());
        let group_encoded = group_record.encode().unwrap();
        assert2::assert!(
            super::RulerStateWalRecord::decode(&group_encoded).unwrap() == group_record
        );
        assert2::assert!(
            super::ruler_state_compaction_key(&group_record)
                == bytes::Bytes::from_static(b"group\0tenant-a\0team-a\0recording")
        );

        let alert = crabka_promql::RulerAlertStateRecord {
            tenant: "tenant-a".to_string(),
            rule_id: "InstanceDown\nup == 0".to_string(),
            labels: std::collections::BTreeMap::from([
                ("alertname".to_string(), "InstanceDown".to_string()),
                ("job".to_string(), "api".to_string()),
            ]),
            active_since_ms: Some(120_000),
        };
        let alert_record = super::RulerStateWalRecord::Alert(alert);
        let alert_encoded = alert_record.encode().unwrap();
        assert2::assert!(
            super::RulerStateWalRecord::decode(&alert_encoded).unwrap() == alert_record
        );
        assert2::assert!(
            super::ruler_state_compaction_key(&alert_record)
                == bytes::Bytes::from_static(
                    b"alert\0tenant-a\0InstanceDown\nup == 0\0alertname=InstanceDown\0job=api"
                )
        );
    }

    #[tokio::test]
    async fn replay_ruler_state_records_applies_state_and_reports_commit_offsets() {
        let state =
            super::prometheus_api_state_for_store(crabka_promql::InMemoryMetricStore::new());
        let router = crabka_promql::prometheus_router(std::sync::Arc::clone(&state));
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/prometheus/config/v1/rules/team-a")
                    .header("X-Scope-OrgID", "tenant-a")
                    .header("Content-Type", "application/yaml")
                    .body(Body::from(
                        r"
name: recording
rules:
  - record: job:up:sum
    expr: sum by (job) (up)
",
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert2::assert!(response.status() == StatusCode::ACCEPTED);

        let group_record =
            super::RulerStateWalRecord::Group(crabka_promql::RulerGroupStateRecord {
                tenant: "tenant-a".to_string(),
                namespace: "team-a".to_string(),
                group: "recording".to_string(),
                last_eval_ms: 60_000,
            });
        let alert_record =
            super::RulerStateWalRecord::Alert(crabka_promql::RulerAlertStateRecord {
                tenant: "tenant-a".to_string(),
                rule_id: "InstanceDown\nup == 0".to_string(),
                labels: std::collections::BTreeMap::from([(
                    "alertname".to_string(),
                    "InstanceDown".to_string(),
                )]),
                active_since_ms: Some(120_000),
            });
        let records = vec![
            super::WalHeadConsumerRecord {
                topic: "ignored".to_string(),
                partition: super::PartitionIndex(0),
                offset: super::Offset(10),
                value: Some(group_record.encode().unwrap()),
            },
            super::WalHeadConsumerRecord {
                topic: super::RULER_STATE_TOPIC.to_string(),
                partition: super::PartitionIndex(2),
                offset: super::Offset(20),
                value: Some(group_record.encode().unwrap()),
            },
            super::WalHeadConsumerRecord {
                topic: super::RULER_STATE_TOPIC.to_string(),
                partition: super::PartitionIndex(2),
                offset: super::Offset(21),
                value: Some(alert_record.encode().unwrap()),
            },
        ];

        let result =
            super::replay_ruler_state_records(&state, super::RULER_STATE_TOPIC, &records).unwrap();

        let expected = super::WalHeadReplayResult {
            polled_records: 3,
            replayed_records: 2,
            committed_offsets: vec![super::WalHeadPartitionOffset {
                partition: super::PartitionIndex(2),
                offset: super::Offset(22),
            }],
        };
        assert2::assert!(result == expected);
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/rules")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert2::assert!(response.status() == StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(body["data"]["groups"][0]["lastEvaluation"] == "1970-01-01T00:01:00Z");
    }

    #[tokio::test]
    async fn poll_ruler_state_consumer_once_replays_records_and_commits_on_progress() {
        let state =
            super::prometheus_api_state_for_store(crabka_promql::InMemoryMetricStore::new());
        let state_record =
            super::RulerStateWalRecord::Group(crabka_promql::RulerGroupStateRecord {
                tenant: "tenant-a".to_string(),
                namespace: "team-a".to_string(),
                group: "recording".to_string(),
                last_eval_ms: 60_000,
            });
        let mut consumer = RecordingWalHeadConsumer {
            batches: vec![vec![consumer_record(
                super::RULER_STATE_TOPIC,
                1,
                7,
                Some(state_record.encode().unwrap()),
            )]],
            commit_calls: 0,
        };

        let result = super::poll_ruler_state_consumer_once(
            &mut consumer,
            &state,
            super::RULER_STATE_TOPIC,
            std::time::Duration::from_millis(1),
        )
        .await
        .unwrap();

        let expected = super::WalHeadReplayResult {
            polled_records: 1,
            replayed_records: 1,
            committed_offsets: vec![super::WalHeadPartitionOffset {
                partition: super::PartitionIndex(1),
                offset: super::Offset(8),
            }],
        };
        assert2::assert!(result == expected);
        assert2::assert!(consumer.commit_calls == 1);
    }

    #[tokio::test]
    async fn blockstore_router_loads_compaction_manifests_from_object_store() {
        let object_store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let writer_store = crabka_blockstore::BlockStore::new(object_store.clone(), base.clone());
        let mut labels = crabka_blockstore::Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", "api");
        let fp = labels.fingerprint();
        let batch = crabka_metrics::encode_float_samples(&[(fp, 10_000, 1.0)]).unwrap();
        let block_meta = writer_store
            .writer()
            .write_block(
                "tenant-a",
                "metrics/tenant-a/float/0001.parquet",
                crabka_metrics::float_sample_schema(),
                &[batch],
            )
            .await
            .unwrap();
        let plan = crabka_metrics::CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: "metrics/tenant-a/float/0001.index".to_string(),
            first_offset: 0,
            last_offset: 0,
            row_count: block_meta.row_count,
        };
        let manifest = crabka_metrics::CompactionIndexManifest::from_block_meta(
            crabka_metrics::MetricBlockKind::Float,
            &plan,
            &block_meta,
            vec![crabka_metrics::CompactionSeriesLabels {
                fingerprint: fp,
                labels,
            }],
        );
        crabka_metrics::CompactionIndexSink::write_manifest(
            &crabka_metrics::ObjectStoreCompactionIndexSink::new(object_store.clone()),
            &manifest,
        )
        .await
        .unwrap();

        let router = super::blockstore_prometheus_router(object_store, base, "metrics/tenant-a")
            .await
            .unwrap();
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/query?query=up&time=10")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert2::assert!(response.status().is_success());
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(body["data"]["result"][0]["metric"]["job"].as_str() == Some("api"));
        assert2::assert!(body["data"]["result"][0]["value"][1].as_str() == Some("1"));
    }

    #[tokio::test]
    async fn blockstore_router_sees_manifests_written_after_startup() {
        let object_store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let router = super::refreshing_blockstore_prometheus_router(
            object_store.clone(),
            base.clone(),
            "metrics/tenant-a",
        );

        let writer_store = crabka_blockstore::BlockStore::new(object_store.clone(), base);
        let mut labels = crabka_blockstore::Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", "api");
        let fp = labels.fingerprint();
        let batch = crabka_metrics::encode_float_samples(&[(fp, 10_000, 1.0)]).unwrap();
        let block_meta = writer_store
            .writer()
            .write_block(
                "tenant-a",
                "metrics/tenant-a/float/0002.parquet",
                crabka_metrics::float_sample_schema(),
                &[batch],
            )
            .await
            .unwrap();
        let plan = crabka_metrics::CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: "metrics/tenant-a/float/0002.index".to_string(),
            first_offset: 1,
            last_offset: 1,
            row_count: block_meta.row_count,
        };
        let manifest = crabka_metrics::CompactionIndexManifest::from_block_meta(
            crabka_metrics::MetricBlockKind::Float,
            &plan,
            &block_meta,
            vec![crabka_metrics::CompactionSeriesLabels {
                fingerprint: fp,
                labels,
            }],
        );
        crabka_metrics::CompactionIndexSink::write_manifest(
            &crabka_metrics::ObjectStoreCompactionIndexSink::new(object_store),
            &manifest,
        )
        .await
        .unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/query?query=up&time=10")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert2::assert!(response.status().is_success());
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(body["data"]["result"][0]["metric"]["job"].as_str() == Some("api"));
        assert2::assert!(body["data"]["result"][0]["value"][1].as_str() == Some("1"));
    }

    #[tokio::test]
    async fn refreshing_blockstore_singleflights_concurrent_cold_cache_loads() {
        let list_calls = Arc::new(AtomicUsize::new(0));
        let object_store: std::sync::Arc<dyn ObjectStore> =
            std::sync::Arc::new(CountingObjectStore::new(
                Arc::clone(&list_calls),
                std::time::Duration::from_millis(25),
            ));
        let base = url::Url::parse("memory:///").unwrap();
        let writer_store = crabka_blockstore::BlockStore::new(object_store.clone(), base.clone());
        let mut labels = crabka_blockstore::Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", "api");
        let fp = labels.fingerprint();
        let batch = crabka_metrics::encode_float_samples(&[(fp, 10_000, 1.0)]).unwrap();
        let block_meta = writer_store
            .writer()
            .write_block(
                "tenant-a",
                "metrics/tenant-a/float/0005.parquet",
                crabka_metrics::float_sample_schema(),
                &[batch],
            )
            .await
            .unwrap();
        let plan = crabka_metrics::CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: "metrics/tenant-a/float/0005.index".to_string(),
            first_offset: 4,
            last_offset: 4,
            row_count: block_meta.row_count,
        };
        let manifest = crabka_metrics::CompactionIndexManifest::from_block_meta(
            crabka_metrics::MetricBlockKind::Float,
            &plan,
            &block_meta,
            vec![crabka_metrics::CompactionSeriesLabels {
                fingerprint: fp,
                labels,
            }],
        );
        crabka_metrics::CompactionIndexSink::write_manifest(
            &crabka_metrics::ObjectStoreCompactionIndexSink::new(object_store.clone()),
            &manifest,
        )
        .await
        .unwrap();

        let metric_store = super::RefreshingMetricBlockStore::new(
            object_store,
            base,
            "metrics/tenant-a",
            crabka_promql::WalHead::new(),
        );
        let matchers = Vec::<crabka_blockstore::LabelMatcher>::new();

        let (a, b, c, d) = tokio::join!(
            metric_store.series("tenant-a", &matchers, 0, 20_000),
            metric_store.series("tenant-a", &matchers, 0, 20_000),
            metric_store.series("tenant-a", &matchers, 0, 20_000),
            metric_store.series("tenant-a", &matchers, 0, 20_000),
        );

        let cases = [("a", a), ("b", b), ("c", c), ("d", d)];
        for (_name, result) in cases {
            assert2::assert!(result.unwrap().len() == 1);
        }
        assert2::assert!(list_calls.load(Ordering::SeqCst) == 1);
    }

    #[tokio::test]
    async fn refreshing_blockstore_bounds_cold_manifests_to_query_time() {
        let object_store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let writer_store = crabka_blockstore::BlockStore::new(object_store.clone(), base.clone());
        let sink = crabka_metrics::ObjectStoreCompactionIndexSink::new(object_store.clone());

        let mut old_labels = crabka_blockstore::Labels::new();
        old_labels.insert("__name__", "up");
        old_labels.insert("job", "old");
        let old_fp = old_labels.fingerprint();
        let old_batch = crabka_metrics::encode_float_samples(&[(old_fp, 10_000, 1.0)]).unwrap();
        let old_block = writer_store
            .writer()
            .write_block(
                "tenant-a",
                "metrics/tenant-a/float/old.parquet",
                crabka_metrics::float_sample_schema(),
                &[old_batch],
            )
            .await
            .unwrap();
        let old_plan = crabka_metrics::CompactionObjectPlan {
            block_key: old_block.object_key.clone(),
            index_key: "metrics/tenant-a/float/old.index".to_string(),
            first_offset: 1,
            last_offset: 1,
            row_count: old_block.row_count,
        };
        let old_manifest = crabka_metrics::CompactionIndexManifest::from_block_meta(
            crabka_metrics::MetricBlockKind::Float,
            &old_plan,
            &old_block,
            vec![crabka_metrics::CompactionSeriesLabels {
                fingerprint: old_fp,
                labels: old_labels,
            }],
        );
        crabka_metrics::CompactionIndexSink::write_manifest(&sink, &old_manifest)
            .await
            .unwrap();

        let mut new_labels = crabka_blockstore::Labels::new();
        new_labels.insert("__name__", "up");
        new_labels.insert("job", "new");
        let new_fp = new_labels.fingerprint();
        let new_batch = crabka_metrics::encode_float_samples(&[(new_fp, 1_000_000, 1.0)]).unwrap();
        let new_block = writer_store
            .writer()
            .write_block(
                "tenant-a",
                "metrics/tenant-a/float/new.parquet",
                crabka_metrics::float_sample_schema(),
                &[new_batch],
            )
            .await
            .unwrap();
        let new_plan = crabka_metrics::CompactionObjectPlan {
            block_key: new_block.object_key.clone(),
            index_key: "metrics/tenant-a/float/new.index".to_string(),
            first_offset: 2,
            last_offset: 2,
            row_count: new_block.row_count,
        };
        let new_manifest = crabka_metrics::CompactionIndexManifest::from_block_meta(
            crabka_metrics::MetricBlockKind::Float,
            &new_plan,
            &new_block,
            vec![crabka_metrics::CompactionSeriesLabels {
                fingerprint: new_fp,
                labels: new_labels,
            }],
        );
        crabka_metrics::CompactionIndexSink::write_manifest(&sink, &new_manifest)
            .await
            .unwrap();

        let metric_store = super::RefreshingMetricBlockStore::new(
            object_store,
            base,
            "metrics/tenant-a",
            crabka_promql::WalHead::new(),
        );
        let matchers = [crabka_blockstore::LabelMatcher::new(
            "__name__",
            crabka_blockstore::MatchOp::Eq,
            "up",
        )];

        let recent = metric_store
            .series("tenant-a", &matchers, 990_000, 1_010_000)
            .await
            .unwrap();
        assert2::assert!(recent.len() == 1);
        assert2::assert!(recent[0].get("job") == Some("new"));

        let old = metric_store
            .series("tenant-a", &matchers, 0, 20_000)
            .await
            .unwrap();
        assert2::assert!(old.len() == 1);
        assert2::assert!(old[0].get("job") == Some("old"));
    }

    #[tokio::test]
    async fn refreshing_blockstore_reuses_decoded_manifests_across_cold_refreshes() {
        let list_calls = Arc::new(AtomicUsize::new(0));
        let object_store = Arc::new(CountingObjectStore::new(
            Arc::clone(&list_calls),
            std::time::Duration::ZERO,
        ));
        let get_calls = Arc::clone(&object_store.get_calls);
        let object_store: std::sync::Arc<dyn ObjectStore> = object_store;
        let base = url::Url::parse("memory:///").unwrap();
        let writer_store = crabka_blockstore::BlockStore::new(object_store.clone(), base.clone());
        let sink = crabka_metrics::ObjectStoreCompactionIndexSink::new(object_store.clone());

        write_float_manifest(
            &writer_store,
            &sink,
            "tenant-a",
            "old",
            10_000,
            "metrics/tenant-a/float/old.parquet",
            1,
        )
        .await;
        write_float_manifest(
            &writer_store,
            &sink,
            "tenant-a",
            "new",
            1_000_000,
            "metrics/tenant-a/float/new.parquet",
            2,
        )
        .await;

        let metric_store = super::RefreshingMetricBlockStore::new(
            object_store,
            base,
            "metrics/tenant-a",
            crabka_promql::WalHead::new(),
        );
        let matchers = [crabka_blockstore::LabelMatcher::new(
            "__name__",
            crabka_blockstore::MatchOp::Eq,
            "up",
        )];

        let old = metric_store
            .series("tenant-a", &matchers, 0, 20_000)
            .await
            .unwrap();
        let new = metric_store
            .series("tenant-a", &matchers, 990_000, 1_010_000)
            .await
            .unwrap();

        check!(old.len() == 1);
        check!(old[0].get("job").unwrap() == "old");
        check!(new.len() == 1);
        check!(new[0].get("job").unwrap() == "new");
        check!(
            list_calls.load(Ordering::SeqCst) == 2,
            "cold refresh should list for new manifest keys but not re-download known .index objects"
        );
        check!(
            get_calls.load(Ordering::SeqCst) == 2,
            "cold refresh should list for new manifest keys but not re-download known .index objects"
        );
    }

    #[tokio::test]
    async fn refreshing_blockstore_tsdb_stats_ignores_stale_compacted_blocks() {
        let object_store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let writer_store = crabka_blockstore::BlockStore::new(object_store.clone(), base.clone());
        let sink = crabka_metrics::ObjectStoreCompactionIndexSink::new(object_store.clone());
        let now_ms = super::duration_ms(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock before Unix epoch"),
        );

        write_float_manifest(
            &writer_store,
            &sink,
            "tenant-a",
            "old",
            now_ms - 2 * 60 * 60 * 1_000,
            "metrics/tenant-a/float/stale.parquet",
            1,
        )
        .await;
        write_float_manifest(
            &writer_store,
            &sink,
            "tenant-a",
            "recent",
            now_ms - 60_000,
            "metrics/tenant-a/float/recent.parquet",
            2,
        )
        .await;

        let metric_store = super::RefreshingMetricBlockStore::new(
            object_store,
            base,
            "metrics/tenant-a",
            crabka_promql::WalHead::new(),
        );

        let stats = metric_store.tsdb_stats("tenant-a").await.unwrap();

        let has_recent_series = stats
            .series_count_by_label_value_pair
            .iter()
            .any(|stat| stat.name == "job=recent" && stat.value == 1);
        let has_stale_series = stats
            .series_count_by_label_value_pair
            .iter()
            .any(|stat| stat.name == "job=old");
        check!(stats.head_stats.num_series == 1);
        check!(has_recent_series);
        check!(!has_stale_series);
    }

    #[tokio::test]
    async fn refreshing_router_merges_hot_head_with_compacted_blocks() {
        let object_store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let writer_store = crabka_blockstore::BlockStore::new(object_store.clone(), base.clone());
        let mut labels = crabka_blockstore::Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", "api");
        let fp = labels.fingerprint();
        let batch = crabka_metrics::encode_float_samples(&[(fp, 10_000, 1.0)]).unwrap();
        let block_meta = writer_store
            .writer()
            .write_block(
                "tenant-a",
                "metrics/tenant-a/float/0003.parquet",
                crabka_metrics::float_sample_schema(),
                &[batch],
            )
            .await
            .unwrap();
        let plan = crabka_metrics::CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: "metrics/tenant-a/float/0003.index".to_string(),
            first_offset: 2,
            last_offset: 2,
            row_count: block_meta.row_count,
        };
        let manifest = crabka_metrics::CompactionIndexManifest::from_block_meta(
            crabka_metrics::MetricBlockKind::Float,
            &plan,
            &block_meta,
            vec![crabka_metrics::CompactionSeriesLabels {
                fingerprint: fp,
                labels: labels.clone(),
            }],
        );
        crabka_metrics::CompactionIndexSink::write_manifest(
            &crabka_metrics::ObjectStoreCompactionIndexSink::new(object_store.clone()),
            &manifest,
        )
        .await
        .unwrap();
        let hot_store = crabka_promql::WalHead::new();
        hot_store.apply_wal_record(&crabka_metrics::WalRecord {
            tenant: "tenant-a".to_string(),
            labels: labels
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            payload: crabka_metrics::SamplePayload::Float {
                timestamp_ms: 20_000,
                value: 2.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        });

        let router = super::refreshing_blockstore_prometheus_router_with_hot_store(
            object_store,
            base,
            "metrics/tenant-a",
            hot_store,
        );
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/query?query=up&time=20")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert2::assert!(response.status().is_success());
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(body["data"]["result"][0]["metric"]["job"].as_str() == Some("api"));
        assert2::assert!(body["data"]["result"][0]["value"][1].as_str() == Some("2"));
    }

    async fn write_float_manifest(
        writer_store: &crabka_blockstore::BlockStore,
        sink: &crabka_metrics::ObjectStoreCompactionIndexSink,
        tenant: &str,
        job: &str,
        ts_ms: i64,
        object_key: &str,
        offset: i64,
    ) {
        let mut labels = crabka_blockstore::Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", job);
        let fp = labels.fingerprint();
        let batch = crabka_metrics::encode_float_samples(&[(fp, ts_ms, 1.0)]).unwrap();
        let block_meta = writer_store
            .writer()
            .write_block(
                tenant,
                object_key,
                crabka_metrics::float_sample_schema(),
                &[batch],
            )
            .await
            .unwrap();
        let plan = crabka_metrics::CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: format!("{object_key}.index"),
            first_offset: offset,
            last_offset: offset,
            row_count: block_meta.row_count,
        };
        let manifest = crabka_metrics::CompactionIndexManifest::from_block_meta(
            crabka_metrics::MetricBlockKind::Float,
            &plan,
            &block_meta,
            vec![crabka_metrics::CompactionSeriesLabels {
                fingerprint: fp,
                labels,
            }],
        );
        crabka_metrics::CompactionIndexSink::write_manifest(sink, &manifest)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn replay_wal_head_records_decodes_applies_and_reports_commit_offsets() {
        let head = crabka_promql::WalHead::new();
        let record = crabka_metrics::WalRecord {
            tenant: "tenant-a".to_string(),
            labels: vec![("__name__".to_string(), "up".to_string())],
            payload: crabka_metrics::SamplePayload::Float {
                timestamp_ms: 10_000,
                value: 1.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        };
        let encoded = record.encode().unwrap();

        let result = super::replay_wal_head_records(
            &head,
            crabka_metrics::WAL_TOPIC,
            &[
                super::WalHeadConsumerRecord {
                    topic: "other".to_string(),
                    partition: super::PartitionIndex(0),
                    offset: super::Offset(3),
                    value: Some(encoded.clone()),
                },
                super::WalHeadConsumerRecord {
                    topic: crabka_metrics::WAL_TOPIC.to_string(),
                    partition: super::PartitionIndex(2),
                    offset: super::Offset(41),
                    value: Some(encoded),
                },
            ],
        )
        .unwrap();

        let expected = super::WalHeadReplayResult {
            polled_records: 2,
            replayed_records: 1,
            committed_offsets: vec![super::WalHeadPartitionOffset {
                partition: super::PartitionIndex(2),
                offset: super::Offset(42),
            }],
        };
        assert2::assert!(result == expected);
        let values = head
            .series("tenant-a", &[], 0, 20_000)
            .await
            .expect("series");
        assert2::assert!(values.len() == 1);
    }

    #[tokio::test]
    async fn replay_wal_head_records_prunes_outside_head_retention() {
        let head = crabka_promql::WalHead::with_retention_ms(1_000);
        let record = |job: &str, timestamp_ms: i64| crabka_metrics::WalRecord {
            tenant: "tenant-a".to_string(),
            labels: vec![
                ("__name__".to_string(), "up".to_string()),
                ("job".to_string(), job.to_string()),
            ],
            payload: crabka_metrics::SamplePayload::Float {
                timestamp_ms,
                value: 1.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        };

        super::replay_wal_head_records(
            &head,
            crabka_metrics::WAL_TOPIC,
            &[
                super::WalHeadConsumerRecord {
                    topic: crabka_metrics::WAL_TOPIC.to_string(),
                    partition: super::PartitionIndex(0),
                    offset: super::Offset(1),
                    value: Some(record("old", 1_000).encode().unwrap()),
                },
                super::WalHeadConsumerRecord {
                    topic: crabka_metrics::WAL_TOPIC.to_string(),
                    partition: super::PartitionIndex(0),
                    offset: super::Offset(2),
                    value: Some(record("new", 10_000).encode().unwrap()),
                },
            ],
        )
        .unwrap();

        let matchers = [crabka_blockstore::LabelMatcher::new(
            "__name__",
            crabka_blockstore::MatchOp::Eq,
            "up",
        )];
        let jobs = head
            .label_values("tenant-a", "job", &matchers, i64::MIN, i64::MAX)
            .await
            .expect("label values");

        assert2::assert!(jobs == vec!["new".to_string()]);
    }

    #[test]
    fn replay_wal_head_records_rejects_missing_wal_values() {
        let head = crabka_promql::WalHead::new();
        let error = super::replay_wal_head_records(
            &head,
            crabka_metrics::WAL_TOPIC,
            &[super::WalHeadConsumerRecord {
                topic: crabka_metrics::WAL_TOPIC.to_string(),
                partition: super::PartitionIndex(1),
                offset: super::Offset(9),
                value: None,
            }],
        )
        .unwrap_err();

        assert2::assert!(matches!(
            error,
            super::WalHeadReplayError::MissingValue {
                partition: super::PartitionIndex(1),
                offset: super::Offset(9)
            }
        ));
    }

    #[tokio::test]
    async fn poll_wal_head_consumer_once_replays_records_and_commits_on_progress() {
        let head = crabka_promql::WalHead::new();
        let record = crabka_metrics::WalRecord {
            tenant: "tenant-a".to_string(),
            labels: vec![("__name__".to_string(), "up".to_string())],
            payload: crabka_metrics::SamplePayload::Float {
                timestamp_ms: 10_000,
                value: 1.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        };
        let mut consumer = RecordingWalHeadConsumer {
            batches: vec![vec![consumer_record(
                crabka_metrics::WAL_TOPIC,
                0,
                4,
                Some(record.encode().unwrap()),
            )]],
            commit_calls: 0,
        };

        let result = super::poll_wal_head_consumer_once(
            &mut consumer,
            &head,
            crabka_metrics::WAL_TOPIC,
            std::time::Duration::from_millis(1),
        )
        .await
        .unwrap();

        let expected = super::WalHeadReplayResult {
            polled_records: 1,
            replayed_records: 1,
            committed_offsets: vec![super::WalHeadPartitionOffset {
                partition: super::PartitionIndex(0),
                offset: super::Offset(5),
            }],
        };
        assert2::assert!(result == expected);
        assert2::assert!(consumer.commit_calls == 1);
    }

    #[tokio::test]
    async fn poll_wal_head_consumer_once_skips_commit_when_no_wal_records_replayed() {
        let head = crabka_promql::WalHead::new();
        let mut consumer = RecordingWalHeadConsumer {
            batches: vec![vec![consumer_record("other", 0, 4, Some(vec![1, 2, 3]))]],
            commit_calls: 0,
        };

        let result = super::poll_wal_head_consumer_once(
            &mut consumer,
            &head,
            crabka_metrics::WAL_TOPIC,
            std::time::Duration::from_millis(1),
        )
        .await
        .unwrap();

        assert2::assert!(result.replayed_records == 0);
        assert2::assert!(consumer.commit_calls == 0);
    }

    #[tokio::test]
    async fn run_wal_head_consumer_loop_accumulates_until_stop_predicate() {
        let head = crabka_promql::WalHead::new();
        let record = crabka_metrics::WalRecord {
            tenant: "tenant-a".to_string(),
            labels: vec![("__name__".to_string(), "up".to_string())],
            payload: crabka_metrics::SamplePayload::Float {
                timestamp_ms: 10_000,
                value: 1.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        };
        let encoded = record.encode().unwrap();
        let mut consumer = RecordingWalHeadConsumer {
            batches: vec![
                vec![consumer_record(
                    crabka_metrics::WAL_TOPIC,
                    0,
                    4,
                    Some(encoded.clone()),
                )],
                vec![consumer_record(
                    crabka_metrics::WAL_TOPIC,
                    0,
                    5,
                    Some(encoded),
                )],
            ],
            commit_calls: 0,
        };

        let summary = super::run_wal_head_consumer_loop(
            &mut consumer,
            &head,
            crabka_metrics::WAL_TOPIC,
            std::time::Duration::from_millis(1),
            |summary| summary.polls == 2,
        )
        .await
        .unwrap();

        let expected = super::WalHeadConsumerLoopSummary {
            polls: 2,
            polled_records: 2,
            replayed_records: 2,
            committed_offsets: vec![
                super::WalHeadPartitionOffset {
                    partition: super::PartitionIndex(0),
                    offset: super::Offset(5),
                },
                super::WalHeadPartitionOffset {
                    partition: super::PartitionIndex(0),
                    offset: super::Offset(6),
                },
            ],
        };
        assert2::assert!(summary == expected);
        assert2::assert!(consumer.commit_calls == 2);
    }

    #[tokio::test]
    async fn in_memory_prometheus_server_binds_to_listen_address() {
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let bound = super::serve_in_memory_prometheus("127.0.0.1:0".parse().unwrap(), async {
            let _ = stop_rx.await;
        })
        .await
        .unwrap();
        let _ = stop_tx.send(());

        assert2::assert!(bound.port() != 0);
    }

    #[tokio::test]
    async fn joinable_server_task_completes_after_shutdown_signal() {
        // The joinable variant hands back the server `JoinHandle` so callers can
        // await graceful drain. Signalling `shutdown` must let the task run to
        // completion (axum's `with_graceful_shutdown` returns), so the join
        // resolves rather than the task living forever detached.
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let (bound, server) = super::serve_prometheus_router_joinable(
            "127.0.0.1:0".parse().unwrap(),
            super::in_memory_prometheus_router(),
            async {
                let _ = stop_rx.await;
            },
        )
        .await
        .unwrap();
        assert2::assert!(bound.port() != 0);

        let _ = stop_tx.send(());
        // Bounded so a regression (handle never resolving) fails instead of hanging.
        let joined = tokio::time::timeout(std::time::Duration::from_secs(5), server).await;
        assert2::assert!(matches!(joined, Ok(Ok(()))));
    }
}
