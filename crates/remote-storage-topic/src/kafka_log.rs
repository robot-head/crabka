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
    time::Duration,
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
        }
    }
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
        let (tx, rx) = mpsc::channel::<MetadataEventRecord>(1024);
        let state = Arc::new(ConsumerState {
            bootstrap: self.bootstrap.clone(),
            client_id: format!("{}-consumer", self.client_id),
            security: self.security.clone(),
            topic: self.topic.clone(),
            topic_id: self.topic_id,
            tx,
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
        .create_topics(&[spec], 30_000)
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
                500,
                1 << 20,
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
                        tokio::time::sleep(Duration::from_millis(200)).await;
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

    use super::*;

    #[test]
    fn config_defaults_match_kafka() {
        let cfg = KafkaMetadataLogConfig::new("127.0.0.1:9092");
        assert2::assert!(cfg.topic.as_str() == METADATA_TOPIC);
        assert2::assert!(cfg.num_partitions == 50);
        assert2::assert!(cfg.replication == 3);
        assert2::assert!(cfg.bootstrap.as_str() == "127.0.0.1:9092");
        assert2::assert!(cfg.security.is_none());
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
        };
        assert2::assert!(cfg.security.is_some());
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
            tasks: StdMutex::new(HashMap::new()),
        });
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

        assert2::assert!(handle.assigned() == vec![0]);
        state.cancel_all();
    }
}
