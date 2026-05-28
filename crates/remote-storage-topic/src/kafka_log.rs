//! [`KafkaMetadataEventLog`] — the production [`MetadataEventLog`]
//! adapter that persists events in the internal `__remote_log_metadata`
//! Kafka topic.
//!
//! Writes flow through a [`crabka_client_producer::Producer`] with
//! explicit per-record partition pinning; reads come back through a
//! [`crabka_client_consumer::Consumer`] using a unique random
//! `group_id` (so concurrent brokers do not contend on offsets) and
//! `AutoOffsetReset::Earliest` (so every startup replays the whole
//! topic). Topic provisioning runs once at [`KafkaMetadataEventLog::start`] via the
//! [`crabka_client_admin::AdminClient`]: an existing topic is reused
//! (the configured `num_partitions` is overridden by the topic's
//! actual count), an absent topic is created with
//! `cleanup.policy=delete`, `retention.ms=-1`.
//!
//! High-water marks are pulled with one `ListOffsets(timestamp=-1)`
//! over the raw [`crabka_client_core::Client`], not via the consumer,
//! so [`MetadataEventLog::high_water_marks`] does not require the
//! consumer group's first assignment to land.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{StreamExt, unfold};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_core::Client;
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use crabka_protocol::owned::list_offsets_request::{
    ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
};

use crate::error::MetadataLogError;
use crate::log::{MetadataEventLog, MetadataEventRecord, MetadataEventStream};

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
    /// wins (re-bucketing on partition growth is a non-goal of 48f).
    pub num_partitions: i32,
    /// Replication factor to create the topic with on first startup.
    /// Ignored when the topic already exists.
    pub replication: i32,
    /// `client_id` for the producer and consumer (diagnostic).
    pub client_id: String,
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
        }
    }
}

/// Production [`MetadataEventLog`] backed by an internal Kafka topic.
pub struct KafkaMetadataEventLog {
    producer: Producer,
    client: Client,
    topic: String,
    partition_count: i32,
    bootstrap: String,
    client_id: String,
    subscriptions: tokio::sync::Mutex<Vec<CancellationToken>>,
}

impl KafkaMetadataEventLog {
    /// Provision the topic if missing, connect the producer and the
    /// raw client, and return the log.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataLogError::Other`] on admin / producer /
    /// client construction failures.
    pub async fn start(cfg: KafkaMetadataLogConfig) -> Result<Arc<Self>, MetadataLogError> {
        // 1. Provision the topic, learn its partition count.
        let partition_count = ensure_topic(&cfg).await?;

        // 2. Producer with acks=All and idempotence on. Read-your-writes
        //    depends on the broker durably acking the publish.
        let producer = Producer::builder()
            .bootstrap(cfg.bootstrap.clone())
            .client_id(format!("{}-producer", cfg.client_id))
            .acks(Acks::All)
            .enable_idempotence(true)
            .build()
            .await
            .map_err(|e| MetadataLogError::Other(format!("producer build failed: {e}")))?;

        // 3. Raw client for ListOffsets and any future low-level queries.
        let client = Client::builder()
            .bootstrap(cfg.bootstrap.clone())
            .client_id(format!("{}-client", cfg.client_id))
            .build()
            .await
            .map_err(|e| MetadataLogError::Other(format!("client build failed: {e}")))?;

        Ok(Arc::new(Self {
            producer,
            client,
            topic: cfg.topic,
            partition_count,
            bootstrap: cfg.bootstrap,
            client_id: cfg.client_id,
            subscriptions: tokio::sync::Mutex::new(Vec::new()),
        }))
    }

    /// Cancel every active subscription. Drop also cancels.
    pub async fn shutdown(&self) {
        let mut subs = self.subscriptions.lock().await;
        for tok in subs.drain(..) {
            tok.cancel();
        }
    }
}

impl Drop for KafkaMetadataEventLog {
    fn drop(&mut self) {
        if let Ok(mut subs) = self.subscriptions.try_lock() {
            for tok in subs.drain(..) {
                tok.cancel();
            }
        }
    }
}

#[async_trait]
impl MetadataEventLog for KafkaMetadataEventLog {
    fn partition_count(&self) -> i32 {
        self.partition_count
    }

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

    fn subscribe(&self) -> MetadataEventStream {
        let (tx, rx) = mpsc::channel::<MetadataEventRecord>(1024);
        let bootstrap = self.bootstrap.clone();
        let topic = self.topic.clone();
        let client_id = format!("{}-consumer", self.client_id);
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();

        // Each subscriber gets its own unique group_id so concurrent
        // subscribers (e.g. test fixtures) do not share offsets.
        let group_id = format!("crabka-rlmm-{}", uuid::Uuid::new_v4());

        tokio::spawn(consumer_pump(
            bootstrap,
            client_id,
            group_id,
            topic,
            tx,
            task_cancel,
        ));

        if let Ok(mut subs) = self.subscriptions.try_lock() {
            subs.push(cancel);
        } else {
            // Lock contention would be exceptional; drop the token so
            // shutdown can't cancel this subscription. Caller still
            // observes events; manager-driven shutdown stops the
            // consumer poll loop independently.
            warn!("KafkaMetadataEventLog: could not track subscription cancel token");
        }

        unfold(rx, |mut rx| async move { rx.recv().await.map(|r| (r, rx)) }).boxed()
    }

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

async fn ensure_topic(cfg: &KafkaMetadataLogConfig) -> Result<i32, MetadataLogError> {
    let mut admin = AdminClient::connect(std::slice::from_ref(&cfg.bootstrap))
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
        return Ok(entry.partition_count);
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
    Ok(cfg.num_partitions)
}

async fn consumer_pump(
    bootstrap: String,
    client_id: String,
    group_id: String,
    topic: String,
    tx: mpsc::Sender<MetadataEventRecord>,
    cancel: CancellationToken,
) {
    // Build the consumer; on failure, surface a warning and exit —
    // the manager's pump_loop will see the stream end and its
    // wait_for_offsets futures will hang, which the broker's
    // spawn_blocking worker eventually times out.
    let consumer = match Consumer::builder()
        .bootstrap(bootstrap)
        .client_id(client_id)
        .group_id(group_id)
        .subscribe(vec![topic])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "KafkaMetadataEventLog: consumer build failed");
            return;
        }
    };
    let mut consumer = consumer;
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                let _ = consumer.close().await;
                return;
            }
            res = consumer.poll(Duration::from_millis(500)) => {
                match res {
                    Ok(records) => {
                        for r in records {
                            let payload = r.value.unwrap_or_default();
                            let record = MetadataEventRecord {
                                partition: r.partition,
                                offset: r.offset,
                                payload,
                            };
                            if tx.send(record).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "KafkaMetadataEventLog: poll failed; retrying");
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
        assert_eq!(cfg.topic, METADATA_TOPIC);
        assert_eq!(cfg.num_partitions, 50);
        assert_eq!(cfg.replication, 3);
        assert_eq!(cfg.bootstrap, "127.0.0.1:9092");
    }
}
