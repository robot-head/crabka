//! Real broker-backed implementations of the [`RecordFetcher`],
//! [`RecordProducer`], and [`OffsetStore`] I/O traits.
//!
//! # Multi-broker routing limitation
//!
//! `BrokerFetcher` opens a single connection to the bootstrap address and uses
//! that connection for every `fetch_partition` call. In a single-node broker
//! setup (the common test scenario) this is always correct because the
//! bootstrap broker is the leader for all partitions. In a multi-broker
//! cluster the fetch may be routed to a broker that is not the partition
//! leader, resulting in `NOT_LEADER_OR_FOLLOWER` errors.
//!
//! TODO: implement per-partition leader routing for `BrokerFetcher` to support
//! multi-broker deployments (look up `leader_id` from metadata, dial the
//! correct broker connection from a pool).

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Mutex;

use crabka_client_core::{Client, Connection, ConnectionOptions, fetch_partition};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

use crabka_client_producer::{Acks, Producer, ProducerRecord};

use crabka_protocol::owned::list_offsets_request::{
    ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
};
use crabka_protocol::owned::offset_commit_request::{
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use crabka_protocol::owned::offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestTopic};

use crate::error::StreamsClientError;
use crate::runtime::io::{FetchBatch, FetchedRec, OffsetStore, RecordFetcher, RecordProducer};

// ─── BrokerFetcher ────────────────────────────────────────────────────────────

/// A [`RecordFetcher`] backed by a real Kafka broker.
///
/// Uses a single connection to the bootstrap broker for all fetch calls.
/// See module-level doc for the multi-broker routing limitation.
#[allow(dead_code)]
pub(crate) struct BrokerFetcher {
    /// Dedicated connection used for every `fetch_partition` call.
    conn: Connection,
    /// Metadata client used to resolve `topic_id` on cache miss.
    client: Client,
    /// Cache of topic name → `topic_id` (populated lazily via metadata refresh).
    topic_ids: Mutex<HashMap<String, WireUuid>>,
    /// Maximum time the broker waits before returning an empty fetch (ms).
    max_wait_ms: i32,
    /// Maximum bytes the broker returns per partition per fetch.
    partition_max_bytes: i32,
}

#[async_trait::async_trait]
impl RecordFetcher for BrokerFetcher {
    async fn fetch(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<FetchBatch, StreamsClientError> {
        let topic_id = self.resolve_topic_id(topic).await?;

        let fetched = fetch_partition(
            &self.conn,
            topic,
            topic_id,
            partition,
            offset,
            self.max_wait_ms,
            self.partition_max_bytes,
        )
        .await?;

        let records = fetched
            .into_iter()
            .map(|r| FetchedRec {
                offset: r.offset,
                key: r.key,
                value: r.value,
                timestamp: -1,
            })
            .collect();

        Ok(FetchBatch { records })
    }
}

impl BrokerFetcher {
    /// Look up the `topic_id` in the local cache; on miss, refresh metadata
    /// from the broker and populate the cache.
    async fn resolve_topic_id(&self, topic: &str) -> Result<WireUuid, StreamsClientError> {
        {
            let cache = self.topic_ids.lock().await;
            if let Some(&id) = cache.get(topic) {
                return Ok(id);
            }
        }

        // Cache miss — fetch fresh metadata.
        let meta = self.client.refresh_metadata().await?;
        let mut cache = self.topic_ids.lock().await;
        for t in &meta.topics {
            if let Some(name) = &t.name {
                cache.insert(name.clone(), t.topic_id);
            }
        }
        // Return the id for the requested topic (fall back to ZERO if not found).
        Ok(cache.get(topic).copied().unwrap_or_default())
    }
}

// ─── BrokerProducer ───────────────────────────────────────────────────────────

/// A [`RecordProducer`] backed by a real Kafka `Producer`.
#[allow(dead_code)]
pub(crate) struct BrokerProducer {
    inner: Producer,
}

#[async_trait::async_trait]
impl RecordProducer for BrokerProducer {
    async fn send(
        &self,
        topic: &str,
        key: Option<Bytes>,
        value: Option<Bytes>,
    ) -> Result<(), StreamsClientError> {
        // Drop the receiver — durability is guaranteed by `flush`.
        let _rx = self
            .inner
            .send(ProducerRecord {
                topic: topic.to_string(),
                partition: None,
                key,
                value,
                ..Default::default()
            })
            .await;
        Ok(())
    }

    async fn flush(&self) -> Result<(), StreamsClientError> {
        self.inner
            .flush()
            .await
            .map_err(|e| StreamsClientError::Runtime(e.to_string()))
    }
}

// ─── BrokerOffsetStore ────────────────────────────────────────────────────────

/// An [`OffsetStore`] backed by a real Kafka broker.
///
/// Uses the group coordinator protocol (`OffsetFetch` / `OffsetCommit` /
/// `ListOffsets`) via the streams consumer group id.
#[allow(dead_code)]
pub(crate) struct BrokerOffsetStore {
    client: Client,
    group_id: String,
}

#[async_trait::async_trait]
impl OffsetStore for BrokerOffsetStore {
    async fn committed(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, StreamsClientError> {
        let resp = self
            .client
            .send(OffsetFetchRequest {
                group_id: self.group_id.clone(),
                topics: Some(vec![OffsetFetchRequestTopic {
                    name: topic.to_string(),
                    partition_indexes: vec![partition],
                    ..Default::default()
                }]),
                ..Default::default()
            })
            .await?;

        // The response may come back in either the legacy `topics` field (v0-7)
        // or the `groups` array (v8+). Try groups first, fall back to topics.
        if let Some(group) = resp.groups.first() {
            for t in &group.topics {
                let topic_name = if t.name.is_empty() {
                    // v10: name is empty; we only requested one topic so this
                    // must be the one we asked about.
                    topic.to_string()
                } else {
                    t.name.clone()
                };
                if topic_name == topic {
                    for p in &t.partitions {
                        if p.partition_index == partition {
                            return Ok(if p.committed_offset < 0 {
                                None
                            } else {
                                Some(p.committed_offset)
                            });
                        }
                    }
                }
            }
        } else {
            for t in &resp.topics {
                if t.name == topic {
                    for p in &t.partitions {
                        if p.partition_index == partition {
                            return Ok(if p.committed_offset < 0 {
                                None
                            } else {
                                Some(p.committed_offset)
                            });
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    async fn earliest(&self, topic: &str, partition: i32) -> Result<i64, StreamsClientError> {
        let resp = self
            .client
            .send(ListOffsetsRequest {
                replica_id: -1,
                topics: vec![ListOffsetsTopic {
                    name: topic.to_string(),
                    partitions: vec![ListOffsetsPartition {
                        partition_index: partition,
                        timestamp: -2, // LIST_OFFSETS_EARLIEST
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await?;

        resp.topics
            .first()
            .and_then(|t| t.partitions.first())
            .map_or_else(
                || {
                    Err(StreamsClientError::Runtime(format!(
                        "ListOffsets: no partition in response for {topic}/{partition}"
                    )))
                },
                |p| {
                    if p.error_code != 0 {
                        Err(StreamsClientError::Runtime(format!(
                            "ListOffsets error code {} for {topic}/{partition}",
                            p.error_code
                        )))
                    } else {
                        Ok(p.offset)
                    }
                },
            )
    }

    async fn commit(&self, offsets: &[(String, i32, i64)]) -> Result<(), StreamsClientError> {
        // Group offsets by topic name.
        let mut by_topic: HashMap<String, Vec<(i32, i64)>> = HashMap::new();
        for (topic, partition, offset) in offsets {
            by_topic
                .entry(topic.clone())
                .or_default()
                .push((*partition, *offset));
        }

        let topics: Vec<OffsetCommitRequestTopic> = by_topic
            .into_iter()
            .map(|(name, parts)| OffsetCommitRequestTopic {
                name,
                partitions: parts
                    .into_iter()
                    .map(
                        |(partition_index, committed_offset)| OffsetCommitRequestPartition {
                            partition_index,
                            committed_offset,
                            committed_leader_epoch: -1,
                            committed_metadata: Some(String::new()),
                            ..Default::default()
                        },
                    )
                    .collect(),
                ..Default::default()
            })
            .collect();

        let resp = self
            .client
            .send(OffsetCommitRequest {
                group_id: self.group_id.clone(),
                generation_id_or_member_epoch: -1,
                member_id: String::new(),
                topics,
                ..Default::default()
            })
            .await?;

        // Surface the first non-zero error code.
        for t in &resp.topics {
            for p in &t.partitions {
                if p.error_code != 0 {
                    return Err(StreamsClientError::Runtime(format!(
                        "OffsetCommit error code {} for topic {} partition {}",
                        p.error_code, t.name, p.partition_index
                    )));
                }
            }
        }
        Ok(())
    }
}

// ─── build ────────────────────────────────────────────────────────────────────

/// Construct the three broker-backed I/O trait objects from a single bootstrap
/// address.
///
/// Returns `(BrokerFetcher, Arc<BrokerProducer>, Arc<BrokerOffsetStore>)`.
/// The producer is `Arc`-wrapped so it can be shared across multiple
/// `StreamTask`s within the same `StreamThread`.
#[allow(dead_code)]
pub(crate) async fn build(
    bootstrap: &str,
    group_id: &str,
    client_id: &str,
) -> Result<(BrokerFetcher, Arc<BrokerProducer>, Arc<BrokerOffsetStore>), StreamsClientError> {
    // 1. Client for metadata + offset RPCs.
    let metadata_client = Client::builder()
        .bootstrap(bootstrap)
        .client_id(client_id)
        .build()
        .await?;

    // 2. Dedicated fetch connection (single bootstrap broker).
    // Resolve the bootstrap address (e.g. "localhost:9092") to a SocketAddr.
    let addr = tokio::net::lookup_host(bootstrap)
        .await
        .map_err(|e| {
            StreamsClientError::Runtime(format!("failed to resolve bootstrap address: {e}"))
        })?
        .next()
        .ok_or_else(|| {
            StreamsClientError::Runtime(format!("no addresses resolved for bootstrap: {bootstrap}"))
        })?;
    let fetch_conn = Connection::connect_with_options(
        addr,
        ConnectionOptions {
            client_id: client_id.to_string(),
            ..Default::default()
        },
    )
    .await?;

    // 3. Idempotent producer.
    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .client_id(format!("{client_id}-producer"))
        .enable_idempotence(true)
        .acks(Acks::All)
        .build()
        .await
        .map_err(|e| StreamsClientError::Runtime(e.to_string()))?;

    // 4. Offset client — re-use a second Client for offset RPCs so the
    //    metadata client's connection isn't head-of-line-blocked by slow
    //    OffsetFetch or OffsetCommit calls.
    let offset_client = Client::builder()
        .bootstrap(bootstrap)
        .client_id(format!("{client_id}-offsets"))
        .build()
        .await?;

    let fetcher = BrokerFetcher {
        conn: fetch_conn,
        client: metadata_client,
        topic_ids: Mutex::new(HashMap::new()),
        max_wait_ms: 500,
        partition_max_bytes: 1 << 20,
    };
    let broker_producer = Arc::new(BrokerProducer { inner: producer });
    let offset_store = Arc::new(BrokerOffsetStore {
        client: offset_client,
        group_id: group_id.to_string(),
    });

    Ok((fetcher, broker_producer, offset_store))
}
