//! WAL topic sink implementations for safekeeper ingest.

#[cfg(feature = "kafka")]
use std::collections::BTreeMap;
use std::collections::HashMap;

use crate::{
    frame::WalFrame,
    ingest::{AppendAck, IngestError, Result, WalTopic, frame_end_lsn},
};

/// In-memory WAL topic used by unit tests and sans-IO integration tests.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct InMemoryWalTopic {
    topics: HashMap<String, Vec<Vec<u8>>>,
    ensured_topics: Vec<String>,
}

impl InMemoryWalTopic {
    /// Creates an empty in-memory topic sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an in-memory sink with `frame` already present in `topic`.
    pub fn with_frame(topic: &str, frame: &WalFrame) -> Result<Self> {
        let mut sink = Self::new();
        sink.ensure_topic(topic)?;
        sink.topics
            .entry(topic.to_owned())
            .or_default()
            .push(frame.encode()?);
        Ok(sink)
    }

    /// Creates an in-memory sink with pre-existing encoded records for `topic`.
    #[must_use]
    pub fn with_records(topic: &str, records: Vec<Vec<u8>>) -> Self {
        Self {
            topics: HashMap::from([(topic.to_owned(), records)]),
            ensured_topics: Vec::new(),
        }
    }

    /// Returns the topics provisioned through [`WalTopic::ensure_topic`].
    #[must_use]
    pub fn ensured_topics(&self) -> &[String] {
        &self.ensured_topics
    }

    /// Returns the encoded records stored for `topic`.
    #[must_use]
    pub fn records(&self, topic: &str) -> &[Vec<u8>] {
        self.topics.get(topic).map_or(&[], Vec::as_slice)
    }

    /// Consumes the sink and returns all stored records grouped by topic.
    #[must_use]
    pub fn into_topics(self) -> HashMap<String, Vec<Vec<u8>>> {
        self.topics
    }
}

impl WalTopic for InMemoryWalTopic {
    fn ensure_topic(&mut self, topic: &str) -> Result<()> {
        self.topics.entry(topic.to_owned()).or_default();
        self.ensured_topics.push(topic.to_owned());
        Ok(())
    }

    fn append_frame(&mut self, topic: &str, frame: &WalFrame) -> Result<AppendAck> {
        let Some(records) = self.topics.get_mut(topic) else {
            return Err(IngestError::Topic {
                message: format!("topic {topic:?} must be ensured before append"),
            });
        };

        records.push(frame.encode()?);
        Ok(AppendAck {
            end_lsn: frame_end_lsn(frame)?,
        })
    }

    fn last_frame(&self, topic: &str) -> Result<Option<Vec<u8>>> {
        Ok(self
            .topics
            .get(topic)
            .and_then(|records| records.last().cloned()))
    }
}

/// Config for an ordinary Kafka-backed WAL topic sink.
#[cfg(feature = "kafka")]
#[derive(Debug, Clone)]
pub struct KafkaWalTopicConfig {
    /// `host:port` Kafka bootstrap address.
    pub bootstrap: String,
    /// Topic partition. Safekeeper writes all WAL for a cluster to one partition.
    pub partition: i32,
    /// Replication factor for topic creation.
    pub replication: i32,
    /// Client id prefix for admin, producer, and fetch clients.
    pub client_id: String,
    /// Optional Kafka client-side TLS/SASL configuration.
    pub security: Option<crabka_client_core::security::ClientSecurity>,
}

#[cfg(feature = "kafka")]
impl KafkaWalTopicConfig {
    /// Builds a plaintext config using Kafka defaults appropriate for dev clusters.
    #[must_use]
    pub fn new(bootstrap: impl Into<String>) -> Self {
        Self {
            bootstrap: bootstrap.into(),
            partition: 0,
            replication: 1,
            client_id: "crabka-safekeeper".to_owned(),
            security: None,
        }
    }
}

/// Ordinary Kafka client implementation of [`WalTopic`].
#[cfg(feature = "kafka")]
pub struct KafkaWalTopic {
    runtime: tokio::runtime::Runtime,
    producer: crabka_client_producer::Producer,
    client: crabka_client_core::Client,
    config: KafkaWalTopicConfig,
    topic_ids: HashMap<String, crabka_protocol::primitives::uuid::Uuid>,
}

#[cfg(feature = "kafka")]
impl KafkaWalTopic {
    /// Connects admin/producer/fetch clients for Kafka WAL ingest.
    pub fn connect(config: KafkaWalTopicConfig) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|source| IngestError::Topic {
                message: format!("Kafka runtime build failed: {source}"),
            })?;
        let producer = runtime.block_on(build_producer(&config))?;
        let client = runtime.block_on(build_client(&config))?;
        Ok(Self {
            runtime,
            producer,
            client,
            config,
            topic_ids: HashMap::new(),
        })
    }
}

#[cfg(feature = "kafka")]
impl WalTopic for KafkaWalTopic {
    fn ensure_topic(&mut self, topic: &str) -> Result<()> {
        let topic_id = self
            .runtime
            .block_on(ensure_kafka_topic(&self.config, topic))?;
        self.topic_ids.insert(topic.to_owned(), topic_id);
        Ok(())
    }

    fn append_frame(&mut self, topic: &str, frame: &WalFrame) -> Result<AppendAck> {
        let value = bytes::Bytes::from(frame.encode()?);
        let send = self.producer.send(crabka_client_producer::ProducerRecord {
            topic: topic.to_owned(),
            partition: Some(self.config.partition),
            value: Some(value),
            ..Default::default()
        });
        let metadata = self
            .runtime
            .block_on(async { send.await.await })
            .map_err(|_| IngestError::Topic {
                message: "Kafka producer dropped before append ack".to_owned(),
            })?
            .map_err(|source| IngestError::Topic {
                message: source.to_string(),
            })?;
        if metadata.partition != self.config.partition {
            return Err(IngestError::Topic {
                message: format!(
                    "Kafka append acked partition {}, expected {}",
                    metadata.partition, self.config.partition
                ),
            });
        }
        Ok(AppendAck {
            end_lsn: frame_end_lsn(frame)?,
        })
    }

    fn last_frame(&self, topic: &str) -> Result<Option<Vec<u8>>> {
        let Some(topic_id) = self.topic_ids.get(topic).copied() else {
            return Ok(None);
        };
        self.runtime.block_on(read_kafka_tail(
            &self.client,
            &self.config,
            topic,
            topic_id,
            self.config.partition,
        ))
    }
}

#[cfg(feature = "kafka")]
async fn build_producer(config: &KafkaWalTopicConfig) -> Result<crabka_client_producer::Producer> {
    crabka_client_producer::Producer::builder()
        .bootstrap(config.bootstrap.clone())
        .client_id(format!("{}-producer", config.client_id))
        .acks(crabka_client_producer::Acks::All)
        .enable_idempotence(true)
        .maybe_security(config.security.clone())
        .build()
        .await
        .map_err(|source| IngestError::Topic {
            message: format!("Kafka producer build failed: {source}"),
        })
}

#[cfg(feature = "kafka")]
async fn build_client(config: &KafkaWalTopicConfig) -> Result<crabka_client_core::Client> {
    crabka_client_core::Client::builder()
        .bootstrap(config.bootstrap.clone())
        .client_id(format!("{}-fetch", config.client_id))
        .maybe_security(config.security.clone())
        .build()
        .await
        .map_err(|source| IngestError::Topic {
            message: format!("Kafka fetch client build failed: {source}"),
        })
}

#[cfg(feature = "kafka")]
async fn ensure_kafka_topic(
    config: &KafkaWalTopicConfig,
    topic: &str,
) -> Result<crabka_protocol::primitives::uuid::Uuid> {
    let mut admin = crabka_client_admin::AdminClient::connect_secured(
        std::slice::from_ref(&config.bootstrap),
        config.security.clone(),
    )
    .await
    .map_err(|source| IngestError::Topic {
        message: format!("Kafka admin connect failed: {source}"),
    })?;

    if let Some(topic_id) = existing_topic_id(&mut admin, topic).await? {
        return Ok(topic_id);
    }

    let spec = crabka_client_admin::CreateTopicSpec {
        name: topic.to_owned(),
        partitions: 1,
        replicas: config.replication,
        configs: BTreeMap::from([
            ("cleanup.policy".to_owned(), "delete".to_owned()),
            ("retention.ms".to_owned(), "-1".to_owned()),
        ]),
    };
    let outcomes = admin
        .create_topics(&[spec], 30_000)
        .await
        .map_err(|source| IngestError::Topic {
            message: format!("Kafka create_topics failed: {source}"),
        })?;
    if let Some(error) = outcomes
        .iter()
        .find(|outcome| outcome.name == topic)
        .and_then(|outcome| outcome.error.as_ref())
    {
        return Err(IngestError::Topic {
            message: format!("Kafka create topic {topic:?} failed: {error:?}"),
        });
    }

    existing_topic_id(&mut admin, topic)
        .await?
        .ok_or_else(|| IngestError::Topic {
            message: format!("Kafka topic {topic:?} was not visible after creation"),
        })
}

#[cfg(feature = "kafka")]
async fn existing_topic_id(
    admin: &mut crabka_client_admin::AdminClient,
    topic: &str,
) -> Result<Option<crabka_protocol::primitives::uuid::Uuid>> {
    let metadata = admin
        .metadata(&[topic])
        .await
        .map_err(|source| IngestError::Topic {
            message: format!("Kafka metadata failed: {source}"),
        })?;
    let Some(entry) = metadata.topics.iter().find(|entry| entry.name == topic) else {
        return Ok(None);
    };
    if entry.error.is_some() || entry.partition_count <= 0 {
        return Ok(None);
    }
    Ok(entry.topic_id.map(|id| {
        let bytes = *id.as_bytes();
        crabka_protocol::primitives::uuid::Uuid(bytes)
    }))
}

#[cfg(feature = "kafka")]
async fn read_kafka_tail(
    client: &crabka_client_core::Client,
    config: &KafkaWalTopicConfig,
    topic: &str,
    topic_id: crabka_protocol::primitives::uuid::Uuid,
    partition: i32,
) -> Result<Option<Vec<u8>>> {
    use std::net::ToSocketAddrs;

    use crabka_client_core::{Connection, ConnectionOptions};

    let Some(addr) = config
        .bootstrap
        .to_socket_addrs()
        .ok()
        .and_then(|mut addrs| addrs.next())
    else {
        return Err(IngestError::Topic {
            message: format!(
                "Kafka bootstrap address {:?} did not resolve",
                config.bootstrap
            ),
        });
    };
    let connection = Connection::connect_with_options(
        addr,
        ConnectionOptions {
            client_id: format!("{}-tail", config.client_id),
            security: config.security.clone().map(Box::new),
            ..Default::default()
        },
    )
    .await
    .map_err(|source| IngestError::Topic {
        message: format!("Kafka tail fetch connection failed: {source}"),
    })?;
    let high_watermark = latest_kafka_offset(client, topic, partition).await?;
    if high_watermark == 0 {
        return Ok(None);
    }
    let records = crabka_client_core::fetch_partition(
        &connection,
        topic,
        topic_id,
        partition,
        high_watermark - 1,
        1_000,
        1 << 20,
    )
    .await
    .map_err(|source| IngestError::Topic {
        message: format!("Kafka tail fetch failed: {source}"),
    })?;
    Ok(records
        .into_iter()
        .last()
        .and_then(|record| record.value.map(Vec::from)))
}

#[cfg(feature = "kafka")]
async fn latest_kafka_offset(
    client: &crabka_client_core::Client,
    topic: &str,
    partition: i32,
) -> Result<i64> {
    use crabka_protocol::owned::list_offsets_request::{
        ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
    };

    let response = client
        .send(ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![ListOffsetsTopic {
                name: topic.to_owned(),
                partitions: vec![ListOffsetsPartition {
                    partition_index: partition,
                    current_leader_epoch: -1,
                    timestamp: -1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .map_err(|source| IngestError::Topic {
            message: format!("Kafka ListOffsets failed: {source}"),
        })?;
    let Some(partition_response) = response
        .topics
        .iter()
        .find(|entry| entry.name == topic)
        .and_then(|entry| {
            entry
                .partitions
                .iter()
                .find(|entry| entry.partition_index == partition)
        })
    else {
        return Err(IngestError::Topic {
            message: format!("Kafka ListOffsets omitted {topic:?} partition {partition}"),
        });
    };
    if partition_response.error_code != 0 {
        return Err(IngestError::Topic {
            message: format!(
                "Kafka ListOffsets for {topic:?} partition {partition} failed with error {}",
                partition_response.error_code
            ),
        });
    }
    Ok(partition_response.offset)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;

    use super::*;
    use crate::{Lsn, frame::WalFrameError};

    #[test]
    fn in_memory_topic_requires_ensure_before_append() {
        let frame = WalFrame::new(Lsn(10), Bytes::from_static(b"wal"));
        assert!(let Ok(frame) = frame);
        let mut topic = InMemoryWalTopic::new();

        let append_before_ensure = topic.append_frame("__pg_wal.alpha", &frame);

        assert!(matches!(
            append_before_ensure,
            Err(IngestError::Topic { message })
                if message.contains("must be ensured before append")
        ));
        assert!(topic.records("__pg_wal.alpha").is_empty());

        let ensured = topic.ensure_topic("__pg_wal.alpha");
        assert!(ensured == Ok(()));

        let ack = topic.append_frame("__pg_wal.alpha", &frame);
        assert!(ack == Ok(AppendAck { end_lsn: Lsn(13) }));
        assert!(topic.ensured_topics() == ["__pg_wal.alpha".to_owned()]);
        assert!(topic.records("__pg_wal.alpha").len() == 1);
    }

    #[test]
    fn in_memory_topic_returns_last_complete_pgw1_frame_for_resume() {
        let first = WalFrame::new(Lsn(0), Bytes::from_static(b"ab"));
        let second = WalFrame::new(Lsn(2), Bytes::from_static(b"cde"));
        assert!(let Ok(first) = first);
        assert!(let Ok(second) = second);
        let mut topic = InMemoryWalTopic::new();
        topic
            .ensure_topic("__pg_wal.alpha")
            .expect("test topic ensures");
        topic
            .append_frame("__pg_wal.alpha", &first)
            .expect("first frame appends");
        topic
            .append_frame("__pg_wal.alpha", &second)
            .expect("second frame appends");

        let last = topic.last_frame("__pg_wal.alpha");

        assert!(let Ok(Some(last)) = last);
        assert!(&last[..4] == b"PGW1");
        assert!(WalFrame::decode(&last) == Ok(second));
    }

    #[test]
    fn in_memory_topic_surfaces_corrupt_tail_without_repairing_it() {
        let topic = InMemoryWalTopic::with_records(
            "__pg_wal.alpha",
            vec![b"BAD!\0\0\0\0\0\0\0\0payload".to_vec()],
        );

        let tail = topic.last_frame("__pg_wal.alpha");

        assert!(let Ok(Some(tail)) = tail);
        assert!(WalFrame::decode(&tail) == Err(WalFrameError::InvalidMagic { got: *b"BAD!" }));
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn kafka_feature_exposes_plaintext_topic_config_and_trait_impl() {
        fn assert_wal_topic<T: WalTopic>() {}

        assert_wal_topic::<KafkaWalTopic>();

        let config = KafkaWalTopicConfig::new("127.0.0.1:9092");

        assert!(config.bootstrap == "127.0.0.1:9092");
        assert!(config.partition == 0);
        assert!(config.replication == 1);
        assert!(config.client_id == "crabka-safekeeper");
        assert!(config.security.is_none());
    }
}
