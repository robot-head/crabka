//! [`KafkaMetadataEventLog`] — the production [`MetadataEventLog`]
//! adapter that persists events in the internal `__remote_log_metadata`
//! Kafka topic.
//!
//! Writes flow through a [`crabka_client_producer::Producer`] with
//! explicit per-record partition pinning. Reads come back through one
//! cancellable manual-`Fetch` task per assigned partition, each driving
//! its own dedicated [`crabka_client_core::Connection`] and emitting
//! [`MetadataEventRecord`]s into a shared mpsc. There is **no consumer
//! group and no broker-side offset commit**: the read position is owned
//! by the RLMM (the manager assigns all partitions from offset 0 today;
//! resume from snapshot offsets and restrict the consumed set).
//!
//! A dedicated connection per partition is required because the broker
//! is serial per-connection: a long-`max_wait_ms` fetch would
//! head-of-line-block any other RPC sharing the socket.
//!
//! Topic provisioning runs once at [`KafkaMetadataEventLog::start`] via
//! the [`crabka_client_admin::AdminClient`]: an existing topic is reused
//! (the configured `num_partitions` is overridden by the topic's actual
//! count), an absent topic is created with `cleanup.policy=delete`,
//! `retention.ms=-1`. The same admin round-trip surfaces the topic's
//! `Uuid`, which the manual `Fetch` path needs (Fetch v≥13 carries
//! `topic_id`, not the name).
//!
//! High-water marks are pulled with one `ListOffsets(timestamp=-1)`
//! over the raw [`crabka_client_core::Client`], not via a consumer, so
//! [`MetadataEventLog::high_water_marks`] does not require any fetch
//! task to have made progress.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex as StdMutex},
};

use async_trait::async_trait;
use bytes::Bytes;
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_core::Client;
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use crabka_protocol::{
    owned::list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
    primitives::uuid::Uuid as WireUuid,
};
use crabka_units::prelude::{
    ByteSize, ByteSizeExt as _, Time, TimeExt as _, mebibytes, millis, secs,
};
use futures_util::stream::{StreamExt, unfold};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, warn};

use crate::{
    error::MetadataLogError,
    log::{
        AssignmentHandle, MetadataEventLog, MetadataEventRecord, MetadataEventStream,
        PartitionStart,
    },
};

/// Default name of the internal metadata topic.
pub const METADATA_TOPIC: &str = "__remote_log_metadata";

/// Default partition count for `__remote_log_metadata`, matching
/// Apache Kafka's `remote.log.metadata.topic.num.partitions`.
pub const DEFAULT_NUM_PARTITIONS: i32 = 50;

/// Default replication factor for `__remote_log_metadata`, matching
/// Apache Kafka's `remote.log.metadata.topic.replication.factor`.
pub const DEFAULT_REPLICATION: i32 = 3;

/// How long `CreateTopics` may take to provision `__remote_log_metadata`
/// before the broker gives up on the round-trip.
pub const DEFAULT_METADATA_TOPIC_CREATE_TIMEOUT: Time = secs(30);

/// `max_wait_ms` for the per-partition metadata `Fetch`. Long enough that an
/// idle partition costs one RPC per interval rather than a spin, short enough
/// that cancellation on reassignment is prompt.
pub const DEFAULT_METADATA_FETCH_MAX_WAIT: Time = millis(500);

/// Per-partition budget for the metadata `Fetch`. Metadata events are small,
/// so one mebibyte is many thousands of them per round-trip.
pub const DEFAULT_METADATA_FETCH_MAX_BYTES: ByteSize = mebibytes(1);

/// Pause before retrying a failed metadata `Fetch`, so a broker that is down
/// does not turn the fetch loop into a busy spin.
pub const DEFAULT_METADATA_FETCH_RETRY_BACKOFF: Time = millis(200);

/// Default capacity of the shared metadata-event delivery queue.
pub const DEFAULT_METADATA_EVENT_QUEUE_CAPACITY: usize = 1024;

/// Positive capacity of the shared metadata-event delivery queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataEventQueueCapacity(usize);

impl MetadataEventQueueCapacity {
    /// Validate a metadata-event queue capacity.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: usize) -> Result<Self, String> {
        refined_type::rule::GreaterUsize::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("metadata event queue capacity: {error}"))
    }

    /// Return the validated channel capacity.
    #[must_use]
    pub const fn capacity(self) -> usize {
        self.0
    }
}

impl Default for MetadataEventQueueCapacity {
    fn default() -> Self {
        Self::new(DEFAULT_METADATA_EVENT_QUEUE_CAPACITY)
            .expect("default metadata event queue capacity is positive")
    }
}

/// Construction-time configuration for [`KafkaMetadataEventLog`].
#[derive(Debug, Clone)]
pub struct KafkaMetadataLogConfig {
    /// `host:port` for the Kafka client to bootstrap from. The TBRLMM
    /// in a broker connects via loopback to its own listener.
    pub bootstrap: String,
    /// Internal topic name. Production deployments stick with the
    /// default; the field exists so multiple isolated clusters can
    /// share an environment in tests.
    pub topic: String,
    /// Number of partitions to create the topic with on first startup.
    /// Ignored when the topic already exists — the existing count
    /// wins (re-bucketing on partition growth is not supported).
    pub num_partitions: i32,
    /// Replication factor to create the topic with on first startup.
    /// Ignored when the topic already exists.
    pub replication: i32,
    /// `client_id` for the producer and consumer (diagnostic).
    pub client_id: String,
    /// Client TLS/SASL security applied to the producer, the raw client,
    /// the admin client, and every per-partition fetch connection.
    /// `None` = plaintext loopback (default).
    pub security: Option<crabka_client_core::security::ClientSecurity>,
    /// Timeout for provisioning the internal topic.
    pub topic_create_timeout: Time,
    /// Maximum wait for each per-partition metadata fetch.
    pub fetch_max_wait: Time,
    /// Maximum bytes returned by each per-partition metadata fetch.
    pub fetch_max_bytes: ByteSize,
    /// Backoff after a failed metadata fetch.
    pub fetch_retry_backoff: Time,
    /// Capacity of the shared metadata-event delivery queue.
    pub event_queue_capacity: MetadataEventQueueCapacity,
}

impl KafkaMetadataLogConfig {
    /// Construct a config with the conventional Kafka defaults.
    #[must_use]
    pub fn new(bootstrap: impl Into<String>) -> Self {
        Self {
            bootstrap: bootstrap.into(),
            topic: METADATA_TOPIC.to_string(),
            num_partitions: DEFAULT_NUM_PARTITIONS,
            replication: DEFAULT_REPLICATION,
            client_id: "crabka-rlmm".to_string(),
            security: None,
            topic_create_timeout: DEFAULT_METADATA_TOPIC_CREATE_TIMEOUT,
            fetch_max_wait: DEFAULT_METADATA_FETCH_MAX_WAIT,
            fetch_max_bytes: DEFAULT_METADATA_FETCH_MAX_BYTES,
            fetch_retry_backoff: DEFAULT_METADATA_FETCH_RETRY_BACKOFF,
            event_queue_capacity: MetadataEventQueueCapacity::default(),
        }
    }

    /// Validate values that cross into the Kafka wire client.
    ///
    /// # Errors
    ///
    /// Returns an error for non-positive, non-finite, fractional, or
    /// out-of-range wire values.
    pub fn validate(&self) -> Result<(), String> {
        validate_positive_whole_millis_i32("topic_create_timeout", self.topic_create_timeout)?;
        validate_positive_whole_millis_i32("fetch_max_wait", self.fetch_max_wait)?;
        validate_positive_whole_bytes_i32("fetch_max_bytes", self.fetch_max_bytes)?;
        validate_positive_duration("fetch_retry_backoff", self.fetch_retry_backoff)
    }
}

fn validate_positive_whole_millis_i32(name: &str, value: Time) -> Result<(), String> {
    let millis = value.millis_i64();
    if !value.secs_f64().is_finite() || Time::from_millis(millis) != value {
        return Err(format!(
            "{name} must be a positive whole number of milliseconds within 1..=i32::MAX"
        ));
    }
    let millis = i32::try_from(millis).map_err(|_| {
        format!("{name} must be a positive whole number of milliseconds within 1..=i32::MAX")
    })?;
    refined_type::rule::GreaterI32::<0>::new(millis)
        .map(|_| ())
        .map_err(|error| format!("{name}: {error}"))
}

fn validate_positive_whole_bytes_i32(name: &str, value: ByteSize) -> Result<(), String> {
    let bytes = value.bytes_i64();
    if !value.bytes_f64().is_finite() || ByteSize::from_bytes_i64(bytes) != value {
        return Err(format!(
            "{name} must be a positive whole number of bytes within 1..=i32::MAX"
        ));
    }
    let bytes = i32::try_from(bytes).map_err(|_| {
        format!("{name} must be a positive whole number of bytes within 1..=i32::MAX")
    })?;
    refined_type::rule::GreaterI32::<0>::new(bytes)
        .map(|_| ())
        .map_err(|error| format!("{name}: {error}"))
}

fn validate_positive_duration(name: &str, value: Time) -> Result<(), String> {
    let duration = std::time::Duration::try_from_secs_f64(value.secs_f64())
        .map_err(|error| format!("{name}: {error}"))?;
    if duration.is_zero() {
        return Err(format!("{name} must be positive"));
    }
    Ok(())
}

/// Production [`MetadataEventLog`] backed by an internal Kafka topic.
pub struct KafkaMetadataEventLog {
    producer: Producer,
    client: Client,
    topic: String,
    topic_id: WireUuid,
    partition_count: i32,
    bootstrap: String,
    client_id: String,
    security: Option<crabka_client_core::security::ClientSecurity>,
    fetch_max_wait: Time,
    fetch_max_bytes: ByteSize,
    fetch_retry_backoff: Time,
    event_queue_capacity: MetadataEventQueueCapacity,
    subscriptions: tokio::sync::Mutex<Vec<Arc<ConsumerState>>>,
}

impl KafkaMetadataEventLog {
    /// Provision the topic if missing, connect the producer and the
    /// raw client, learn the topic id, and return the log.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataLogError::Other`] on admin / producer /
    /// client construction failures.
    #[instrument(skip_all, fields(topic = %cfg.topic, bootstrap = %cfg.bootstrap), err)]
    pub async fn start(cfg: KafkaMetadataLogConfig) -> Result<Arc<Self>, MetadataLogError> {
        cfg.validate()
            .map_err(|error| MetadataLogError::Other(format!("invalid config: {error}")))?;

        // 1. Provision the topic, learn its partition count and id. The
        //    manual Fetch path needs the topic Uuid (Fetch v≥13 carries
        //    topic_id, not the name).
        let (partition_count, topic_id) = ensure_topic(&cfg).await?;

        // 2. Producer with acks=All and idempotence on. Read-your-writes
        //    depends on the broker durably acking the publish.
        let producer = Producer::builder()
            .bootstrap(cfg.bootstrap.clone())
            .client_id(format!("{}-producer", cfg.client_id))
            .acks(Acks::All)
            .enable_idempotence(true)
            .maybe_security(cfg.security.clone())
            .build()
            .await
            .map_err(|e| MetadataLogError::Other(format!("producer build failed: {e}")))?;

        // 3. Raw client for ListOffsets and any future low-level queries.
        let client = Client::builder()
            .bootstrap(cfg.bootstrap.clone())
            .client_id(format!("{}-client", cfg.client_id))
            .maybe_security(cfg.security.clone())
            .build()
            .await
            .map_err(|e| MetadataLogError::Other(format!("client build failed: {e}")))?;

        Ok(Arc::new(Self {
            producer,
            client,
            topic: cfg.topic,
            topic_id,
            partition_count,
            bootstrap: cfg.bootstrap,
            client_id: cfg.client_id,
            security: cfg.security,
            fetch_max_wait: cfg.fetch_max_wait,
            fetch_max_bytes: cfg.fetch_max_bytes,
            fetch_retry_backoff: cfg.fetch_retry_backoff,
            event_queue_capacity: cfg.event_queue_capacity,
            subscriptions: tokio::sync::Mutex::new(Vec::new()),
        }))
    }

    /// Cancel every active subscription's fetch tasks. Drop also cancels.
    pub async fn shutdown(&self) {
        let mut subs = self.subscriptions.lock().await;
        for state in subs.drain(..) {
            state.cancel_all();
        }
    }
}

impl Drop for KafkaMetadataEventLog {
    fn drop(&mut self) {
        if let Ok(mut subs) = self.subscriptions.try_lock() {
            for state in subs.drain(..) {
                state.cancel_all();
            }
        }
    }
}

/// Per-subscription live consumer: one cancellable fetch task per
/// assigned partition, all emitting into the shared `tx`.
struct ConsumerState {
    bootstrap: String,
    client_id: String,
    security: Option<crabka_client_core::security::ClientSecurity>,
    topic: String,
    topic_id: WireUuid,
    tx: mpsc::Sender<MetadataEventRecord>,
    fetch_max_wait: Time,
    fetch_max_bytes: ByteSize,
    fetch_retry_backoff: Time,
    /// partition -> cancel token for its fetch task.
    tasks: StdMutex<HashMap<i32, CancellationToken>>,
}

impl ConsumerState {
    fn spawn_partition(self: &Arc<Self>, start: PartitionStart) {
        let mut tasks = self.tasks.lock().expect("metadata tasks mutex poisoned");
        if tasks.contains_key(&start.partition) {
            return; // already assigned
        }
        let cancel = CancellationToken::new();
        tasks.insert(start.partition, cancel.clone());
        tokio::spawn(partition_fetch_loop(
            Arc::clone(self),
            start.partition,
            start.start_offset,
            cancel,
        ));
    }

    fn cancel_partition(&self, partition: i32) {
        if let Some(tok) = self
            .tasks
            .lock()
            .expect("metadata tasks mutex poisoned")
            .remove(&partition)
        {
            tok.cancel();
        }
    }

    fn cancel_all(&self) {
        let mut tasks = self.tasks.lock().expect("metadata tasks mutex poisoned");
        for (_, tok) in tasks.drain() {
            tok.cancel();
        }
    }
}

struct KafkaAssignmentHandle {
    state: Arc<ConsumerState>,
}

impl AssignmentHandle for KafkaAssignmentHandle {
    fn add(&self, start: PartitionStart) {
        self.state.spawn_partition(start);
    }
    fn remove(&self, partition: i32) {
        self.state.cancel_partition(partition);
    }
    fn assigned(&self) -> Vec<i32> {
        let mut v: Vec<i32> = self
            .state
            .tasks
            .lock()
            .expect("metadata tasks mutex poisoned")
            .keys()
            .copied()
            .collect();
        v.sort_unstable();
        v
    }
}

#[async_trait]
impl MetadataEventLog for KafkaMetadataEventLog {
    fn partition_count(&self) -> i32 {
        self.partition_count
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(topic = %self.topic, partition, len = event.len()),
        err
    )]
    async fn publish(&self, partition: i32, event: Bytes) -> Result<i64, MetadataLogError> {
        if partition < 0 || partition >= self.partition_count {
            return Err(MetadataLogError::PartitionOutOfRange {
                partition,
                count: self.partition_count,
            });
        }
        let ack = self
            .producer
            .send(ProducerRecord {
                topic: self.topic.clone(),
                partition: Some(partition),
                value: Some(event),
                ..Default::default()
            })
            .await;
        let meta = ack
            .await
            .map_err(|_| MetadataLogError::Publish("producer dropped before ack".into()))?
            .map_err(|e| MetadataLogError::Publish(e.to_string()))?;
        Ok(meta.offset)
    }

    fn subscribe(
        &self,
        assignment: Vec<PartitionStart>,
    ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>) {
        let (tx, rx) = metadata_event_channel(self.event_queue_capacity);
        let state = Arc::new(ConsumerState {
            bootstrap: self.bootstrap.clone(),
            client_id: format!("{}-consumer", self.client_id),
            security: self.security.clone(),
            topic: self.topic.clone(),
            topic_id: self.topic_id,
            tx,
            fetch_max_wait: self.fetch_max_wait,
            fetch_max_bytes: self.fetch_max_bytes,
            fetch_retry_backoff: self.fetch_retry_backoff,
            tasks: StdMutex::new(HashMap::new()),
        });
        for ps in assignment {
            state.spawn_partition(ps);
        }
        if let Ok(mut subs) = self.subscriptions.try_lock() {
            subs.push(Arc::clone(&state));
        } else {
            warn!("KafkaMetadataEventLog: could not track subscription state");
        }
        let stream = unfold(rx, |mut rx| async move { rx.recv().await.map(|r| (r, rx)) }).boxed();
        let handle: Arc<dyn AssignmentHandle> = Arc::new(KafkaAssignmentHandle { state });
        (stream, handle)
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(topic = %self.topic, partition_count = self.partition_count),
        err
    )]
    async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError> {
        let partitions = (0..self.partition_count)
            .map(|p| ListOffsetsPartition {
                partition_index: p,
                current_leader_epoch: -1,
                timestamp: -1, // LATEST
                ..Default::default()
            })
            .collect();
        let req = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![ListOffsetsTopic {
                name: self.topic.clone(),
                partitions,
                ..Default::default()
            }],
            ..Default::default()
        };
        let resp = self
            .client
            .send(req)
            .await
            .map_err(|e| MetadataLogError::Other(format!("ListOffsets failed: {e}")))?;
        let mut hwms = vec![0i64; usize_count(self.partition_count)?];
        for topic in &resp.topics {
            if topic.name != self.topic {
                continue;
            }
            for p in &topic.partitions {
                if p.error_code != 0 {
                    return Err(MetadataLogError::Other(format!(
                        "ListOffsets partition {} error {}",
                        p.partition_index, p.error_code
                    )));
                }
                if let Ok(idx) = usize::try_from(p.partition_index)
                    && idx < hwms.len()
                {
                    hwms[idx] = p.offset;
                }
            }
        }
        Ok(hwms)
    }
}

fn metadata_event_channel(
    capacity: MetadataEventQueueCapacity,
) -> (
    mpsc::Sender<MetadataEventRecord>,
    mpsc::Receiver<MetadataEventRecord>,
) {
    mpsc::channel(capacity.capacity())
}

/// Provision the topic if missing and return `(partition_count,
/// topic_id)`. An existing topic's count and id win; a freshly-created
/// topic's id is re-read with a second metadata round-trip (the
/// `CreateTopics` outcome does not reliably carry it).
#[instrument(skip_all, fields(topic = %cfg.topic), err)]
async fn ensure_topic(cfg: &KafkaMetadataLogConfig) -> Result<(i32, WireUuid), MetadataLogError> {
    let mut admin =
        AdminClient::connect_secured(std::slice::from_ref(&cfg.bootstrap), cfg.security.clone())
            .await
            .map_err(|e| MetadataLogError::Other(format!("admin connect failed: {e}")))?;

    let topic_ref = cfg.topic.as_str();
    let meta = admin
        .metadata(&[topic_ref])
        .await
        .map_err(|e| MetadataLogError::Other(format!("metadata failed: {e}")))?;

    if let Some(entry) = meta.topics.iter().find(|t| t.name == cfg.topic)
        && entry.error.is_none()
        && entry.partition_count > 0
    {
        debug!(
            topic = %cfg.topic,
            partition_count = entry.partition_count,
            "metadata topic already exists; reusing"
        );
        let topic_id = entry.topic_id.map_or(WireUuid::ZERO, to_wire_uuid);
        warn_if_zero_topic_id(&cfg.topic, topic_id);
        return Ok((entry.partition_count, topic_id));
    }

    let mut configs = BTreeMap::new();
    configs.insert("cleanup.policy".to_string(), "delete".to_string());
    configs.insert("retention.ms".to_string(), "-1".to_string());
    let spec = CreateTopicSpec {
        name: cfg.topic.clone(),
        partitions: cfg.num_partitions,
        replicas: cfg.replication,
        configs,
    };
    let outcomes = admin
        .create_topics(&[spec], cfg.topic_create_timeout)
        .await
        .map_err(|e| MetadataLogError::Other(format!("create_topics failed: {e}")))?;
    let outcome = outcomes
        .into_iter()
        .find(|o| o.name == cfg.topic)
        .ok_or_else(|| MetadataLogError::Other("create_topics returned no outcome".into()))?;
    if let Some(err) = outcome.error {
        return Err(MetadataLogError::Other(format!(
            "create_topics for {} failed: {err:?}",
            cfg.topic
        )));
    }
    debug!(
        topic = %cfg.topic,
        partition_count = cfg.num_partitions,
        "metadata topic created"
    );

    // Re-read metadata to learn the freshly-assigned topic id.
    let topic_id = if let Some(id) = outcome.topic_id {
        to_wire_uuid(id)
    } else {
        let meta = admin
            .metadata(&[topic_ref])
            .await
            .map_err(|e| MetadataLogError::Other(format!("metadata (post-create) failed: {e}")))?;
        meta.topics
            .iter()
            .find(|t| t.name == cfg.topic)
            .and_then(|t| t.topic_id)
            .map_or(WireUuid::ZERO, to_wire_uuid)
    };
    warn_if_zero_topic_id(&cfg.topic, topic_id);
    Ok((cfg.num_partitions, topic_id))
}

/// A zero topic id makes every Fetch v≥13 fail (it carries `topic_id`,
/// not the name), which manifests as the metadata consumer spinning with
/// no progress. Warn loudly so the misconfiguration is diagnosable
/// rather than a silent hang.
fn warn_if_zero_topic_id(topic: &str, topic_id: WireUuid) {
    if topic_id == WireUuid::ZERO {
        warn!(
            topic = %topic,
            "metadata topic resolved to a zero topic_id; Fetch v>=13 will fail \
             and the consumer will make no progress"
        );
    }
}

/// Convert the admin client's `uuid::Uuid` to the wire `Uuid` Fetch
/// requires.
fn to_wire_uuid(u: uuid::Uuid) -> WireUuid {
    WireUuid(*u.as_bytes())
}

/// Manual single-partition fetch loop over a dedicated connection.
///
/// A dedicated connection per partition keeps the metadata consumer off
/// any parkable/shared stream: the broker is serial per-connection, so a
/// long-`max_wait_ms` fetch must not head-of-line-block other RPCs.
// cargo-mutants: live-broker fetch loop over a real connection, not unit-testable
#[cfg_attr(test, mutants::skip)]
#[instrument(level = "debug", skip_all, fields(partition, start_offset))]
async fn partition_fetch_loop(
    state: Arc<ConsumerState>,
    partition: i32,
    start_offset: i64,
    cancel: CancellationToken,
) {
    use std::net::ToSocketAddrs;

    use crabka_client_core::{Connection, ConnectionOptions, fetch_partition};

    // Dedicated connection for this partition's fetch loop. Resolve the
    // bootstrap address; on failure, warn and exit. The partition then
    // never advances past its resume offset, so the manager's readiness
    // gate keeps returning `NotReady` (retryable) for reads that hash
    // there until a later reconcile re-establishes the fetch loop.
    let Some(addr) = state
        .bootstrap
        .to_socket_addrs()
        .ok()
        .and_then(|mut a| a.next())
    else {
        warn!(bootstrap = %state.bootstrap, "metadata consumer: bad bootstrap addr");
        return;
    };
    let opts = ConnectionOptions {
        client_id: state.client_id.clone(),
        security: state.security.clone().map(Box::new),
        ..Default::default()
    };
    let conn = match Connection::connect_with_options(addr, opts).await {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, partition, "metadata consumer: connect failed");
            return;
        }
    };

    let mut next_offset = start_offset.max(0);
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                conn.close();
                return;
            }
            res = fetch_partition(
                &conn,
                &state.topic,
                state.topic_id,
                partition,
                next_offset,
                state.fetch_max_wait,
                state.fetch_max_bytes,
            ) => {
                match res {
                    Ok(records) => {
                        for r in records {
                            // Re-check cancellation before every send: a
                            // remove() (for reassignment) that fires
                            // after fetch_partition resolved must not flush
                            // the rest of an already-fetched batch, or a
                            // task spawned on re-add from a new start_offset
                            // would double-deliver these same records.
                            if cancel.is_cancelled() {
                                conn.close();
                                return;
                            }
                            if r.offset < next_offset {
                                continue; // defensive: never go backwards
                            }
                            let payload = r.value.unwrap_or_default();
                            let record = MetadataEventRecord {
                                partition,
                                offset: r.offset,
                                payload,
                            };
                            next_offset = r.offset + 1;
                            if state.tx.send(record).await.is_err() {
                                conn.close();
                                return; // stream dropped
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, partition, "metadata consumer: fetch failed; retrying");
                        tokio::time::sleep(state.fetch_retry_backoff.to_std()).await;
                    }
                }
            }
        }
    }
}

fn usize_count(n: i32) -> Result<usize, MetadataLogError> {
    usize::try_from(n).map_err(|_| MetadataLogError::Other(format!("partition_count {n} negative")))
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_units::convert::{ByteSizeExt as _, TimeExt as _};

    use super::*;

    #[test]
    fn config_defaults_match_kafka() {
        let cfg = KafkaMetadataLogConfig::new("127.0.0.1:9092");
        check!(cfg.topic == METADATA_TOPIC);
        check!(cfg.num_partitions == 50);
        check!(cfg.replication == 3);
        check!(cfg.bootstrap == "127.0.0.1:9092");
        check!(cfg.security.is_none());
        check!(cfg.topic_create_timeout == secs(30));
        check!(cfg.fetch_max_wait == millis(500));
        check!(cfg.fetch_max_bytes == mebibytes(1));
        check!(cfg.fetch_retry_backoff == millis(200));
        check!(cfg.event_queue_capacity.capacity() == 1024);
        cfg.validate().unwrap();
    }

    #[test]
    fn config_accepts_custom_transport_policy() {
        let cfg = KafkaMetadataLogConfig {
            topic_create_timeout: secs(45),
            fetch_max_wait: millis(750),
            fetch_max_bytes: mebibytes(2),
            fetch_retry_backoff: millis(300),
            event_queue_capacity: MetadataEventQueueCapacity::new(2048).unwrap(),
            ..KafkaMetadataLogConfig::new("127.0.0.1:9092")
        };

        cfg.validate().unwrap();
        check!(cfg.topic_create_timeout == secs(45));
        check!(cfg.fetch_max_wait == millis(750));
        check!(cfg.fetch_max_bytes == mebibytes(2));
        check!(cfg.fetch_retry_backoff == millis(300));
        check!(cfg.event_queue_capacity.capacity() == 2048);
    }

    #[test]
    fn config_rejects_invalid_transport_policy() {
        fn configured(
            configure: impl FnOnce(&mut KafkaMetadataLogConfig),
        ) -> KafkaMetadataLogConfig {
            let mut cfg = KafkaMetadataLogConfig::new("127.0.0.1:9092");
            configure(&mut cfg);
            cfg
        }

        let cases = [
            (
                "topic_create_timeout",
                configured(|cfg| cfg.topic_create_timeout = Time::ZERO),
            ),
            (
                "topic_create_timeout",
                configured(|cfg| cfg.topic_create_timeout = Time::from_micros(500)),
            ),
            (
                "topic_create_timeout",
                configured(|cfg| {
                    cfg.topic_create_timeout = Time::from_secs_f64(f64::INFINITY);
                }),
            ),
            (
                "topic_create_timeout",
                configured(|cfg| {
                    cfg.topic_create_timeout = Time::from_millis(i64::from(i32::MAX) + 1);
                }),
            ),
            (
                "fetch_max_wait",
                configured(|cfg| cfg.fetch_max_wait = Time::ZERO),
            ),
            (
                "fetch_max_wait",
                configured(|cfg| cfg.fetch_max_wait = Time::from_micros(500)),
            ),
            (
                "fetch_max_wait",
                configured(|cfg| cfg.fetch_max_wait = Time::from_secs_f64(f64::INFINITY)),
            ),
            (
                "fetch_max_wait",
                configured(|cfg| {
                    cfg.fetch_max_wait = Time::from_millis(i64::from(i32::MAX) + 1);
                }),
            ),
            (
                "fetch_max_bytes",
                configured(|cfg| cfg.fetch_max_bytes = ByteSize::ZERO),
            ),
            (
                "fetch_max_bytes",
                configured(|cfg| cfg.fetch_max_bytes = ByteSize::from_bytes_f64(0.5)),
            ),
            (
                "fetch_max_bytes",
                configured(|cfg| {
                    cfg.fetch_max_bytes = ByteSize::from_bytes_f64(f64::INFINITY);
                }),
            ),
            (
                "fetch_max_bytes",
                configured(|cfg| {
                    cfg.fetch_max_bytes = ByteSize::from_bytes_i64(i64::from(i32::MAX) + 1);
                }),
            ),
            (
                "fetch_retry_backoff",
                configured(|cfg| cfg.fetch_retry_backoff = Time::ZERO),
            ),
            (
                "fetch_retry_backoff",
                configured(|cfg| {
                    cfg.fetch_retry_backoff = Time::from_secs_f64(f64::INFINITY);
                }),
            ),
        ];

        for (field, cfg) in cases {
            let error = cfg.validate().expect_err("invalid policy must fail");
            assert!(error.contains(field), "field={field}, error={error}");
        }
    }

    #[test]
    fn metadata_event_queue_capacity_rejects_zero() {
        assert!(MetadataEventQueueCapacity::new(0).is_err());
        check!(MetadataEventQueueCapacity::new(1).unwrap().capacity() == 1);
    }

    #[test]
    fn metadata_event_channel_uses_configured_capacity() {
        let (tx, _rx) = metadata_event_channel(MetadataEventQueueCapacity::new(2048).unwrap());
        check!(tx.max_capacity() == 2048);
    }

    #[tokio::test]
    async fn start_rejects_invalid_policy_before_connecting() {
        let cfg = KafkaMetadataLogConfig {
            topic_create_timeout: Time::ZERO,
            ..KafkaMetadataLogConfig::new("not a socket address")
        };

        let Err(error) = KafkaMetadataEventLog::start(cfg).await else {
            panic!("invalid policy must fail before network I/O");
        };
        assert!(error.to_string().contains("topic_create_timeout"));
    }

    /// The metadata client's tunables are quantities but reach Kafka as raw
    /// `int32` milliseconds and bytes. Pin the wire images: a wrong scale here
    /// is invisible in the types and would show up only as a metadata consumer
    /// that spins (too short a `max_wait`) or truncates batches.
    #[test]
    fn client_tunables_convert_to_their_kafka_wire_images() {
        let cfg = KafkaMetadataLogConfig::new("127.0.0.1:9092");
        check!(cfg.topic_create_timeout.millis_i32() == 30_000);
        check!(cfg.fetch_max_wait.millis_i32() == 500);
        check!(cfg.fetch_max_bytes.bytes_i32() == 1 << 20);
        check!(cfg.fetch_retry_backoff.to_std() == std::time::Duration::from_millis(200));
    }

    #[test]
    fn config_carries_security() {
        use crabka_client_core::security::{ClientSecurity, SaslCredentials};
        use crabka_security::ListenerProtocol;
        let cfg = KafkaMetadataLogConfig {
            bootstrap: "127.0.0.1:9092".into(),
            topic: METADATA_TOPIC.into(),
            num_partitions: 1,
            replication: 1,
            client_id: "x".into(),
            security: Some(ClientSecurity {
                protocol: ListenerProtocol::SaslPlaintext,
                tls: None,
                sasl: Some(SaslCredentials::Plain {
                    username: "u".into(),
                    password: "p".into(),
                }),
                sasl_host: None,
            }),
            ..KafkaMetadataLogConfig::new("127.0.0.1:9092")
        };
        assert!(cfg.security.is_some());
    }

    #[tokio::test]
    async fn assignment_handle_tracks_spawned_partitions() {
        let (tx, _rx) = mpsc::channel::<MetadataEventRecord>(1);
        let state = Arc::new(ConsumerState {
            bootstrap: "invalid-bootstrap".into(),
            client_id: "test-consumer".into(),
            security: None,
            topic: METADATA_TOPIC.into(),
            topic_id: WireUuid::ZERO,
            tx,
            fetch_max_wait: millis(750),
            fetch_max_bytes: mebibytes(2),
            fetch_retry_backoff: millis(300),
            tasks: StdMutex::new(HashMap::new()),
        });
        check!(state.fetch_max_wait == millis(750));
        check!(state.fetch_max_bytes == mebibytes(2));
        check!(state.fetch_retry_backoff == millis(300));
        let handle = KafkaAssignmentHandle {
            state: Arc::clone(&state),
        };

        state.spawn_partition(PartitionStart {
            partition: 2,
            start_offset: 7,
        });
        state.spawn_partition(PartitionStart {
            partition: 2,
            start_offset: 9,
        });
        handle.add(PartitionStart {
            partition: 0,
            start_offset: 0,
        });
        handle.remove(2);

        assert!(handle.assigned() == vec![0]);
        state.cancel_all();
    }
}
