//! Real broker-backed implementations of the [`RecordFetcher`],
//! [`RecordProducer`], and [`OffsetStore`] I/O traits.

use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use crabka_client_core::{
    Client, ClientDnsTimeout, ClientFrameMax, ConnectionDispatchQueueCapacity,
    DEFAULT_FETCH_RESPONSE_MAX, FetchMinBytes, IsolatedFetch,
};
use crabka_client_producer::{Acks, Producer, ProducerError, ProducerRecord, RecordMetadata};
use crabka_protocol::{
    owned::{
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        offset_commit_request::{
            OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        offset_fetch_request::{
            OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopic,
            OffsetFetchRequestTopics,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};
use crabka_units::prelude::*;
use tokio::sync::{Mutex, oneshot};

use crate::{
    error::StreamsClientError,
    runtime::{
        eos::{StreamsGroupMeta, TransactionalProducer},
        io::{FetchBatch, FetchedRec, IsolationLevel, OffsetStore, RecordFetcher, RecordProducer},
    },
};

// ─── BrokerFetcher ────────────────────────────────────────────────────────────

/// A [`RecordFetcher`] backed by a real Kafka broker.
///
/// Routes each fetch to the partition leader learned from broker metadata.
pub(crate) struct BrokerFetcher {
    /// Metadata client and per-broker connection pool.
    client: Client,
    /// Topic ids and partition leaders, replaced atomically on metadata refresh.
    routes: Mutex<HashMap<String, TopicRoute>>,
    /// Maximum time the broker waits before returning an empty fetch.
    max_wait: Time,
    /// Maximum size the broker returns for each partition for each fetch.
    partition_max: ByteSize,
    fetch_min: FetchMinBytes,
}

#[derive(Debug, Clone)]
struct TopicRoute {
    topic_id: WireUuid,
    leaders: HashMap<i32, i32>,
}

#[derive(Clone, Copy)]
pub(crate) struct ClientResourcePolicy {
    pub(crate) dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    pub(crate) frame_max: ClientFrameMax,
    pub(crate) fetch_min: FetchMinBytes,
}

#[async_trait::async_trait]
impl RecordFetcher for BrokerFetcher {
    // cargo-mutants: live broker routing and wire projection; integration-tested.
    #[cfg_attr(test, mutants::skip)]
    async fn fetch(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        isolation: IsolationLevel,
    ) -> Result<FetchBatch, StreamsClientError> {
        let route = self.resolve_route(topic, partition).await?;

        // Map the runtime isolation level to the Kafka `Fetch.isolation_level`
        // wire value (READ_UNCOMMITTED = 0, READ_COMMITTED = 1).
        let isolation_level: i8 = match isolation {
            IsolationLevel::ReadUncommitted => 0,
            IsolationLevel::ReadCommitted => 1,
        };

        let request = IsolatedFetch {
            topic,
            topic_id: route.topic_id,
            partition,
            fetch_offset: offset,
            // `IsolatedFetch` mirrors the Kafka `Fetch` wire fields, so the
            // quantities render back to raw integers here.
            max_wait: self.max_wait,
            max: DEFAULT_FETCH_RESPONSE_MAX,
            partition_max: self.partition_max,
            fetch_min: self.fetch_min,
            isolation_level,
        };
        let fetched = match self
            .client
            .fetch_partition_with_isolation_on(route.leader_id, request)
            .await
        {
            Err(crabka_client_core::ClientError::Server { error_code })
                if is_stale_route_error(error_code) =>
            {
                self.refresh_routes().await?;
                let route = self.cached_route(topic, partition).await?;
                self.client
                    .fetch_partition_with_isolation_on(
                        route.leader_id,
                        IsolatedFetch {
                            topic_id: route.topic_id,
                            ..request
                        },
                    )
                    .await?
            }
            other => other?,
        };

        let records = fetched
            .into_iter()
            .map(|r| FetchedRec {
                offset: r.offset,
                key: r.key,
                value: r.value,
                timestamp: r.timestamp,
            })
            .collect();

        Ok(FetchBatch { records })
    }

    /// Resolve the real partition list for `topic` from broker metadata. The
    /// global consumer reads every partition to materialize the fully-replicated
    /// global store, so the default `vec![0]` would silently drop records on any
    /// partition greater than zero.
    async fn partitions(&self, topic: &str) -> Result<Vec<i32>, StreamsClientError> {
        self.refresh_routes().await?;
        let routes = self.routes.lock().await;
        let mut partitions: Vec<i32> = routes
            .get(topic)
            .ok_or_else(|| missing_topic(topic))?
            .leaders
            .keys()
            .copied()
            .collect();
        partitions.sort_unstable();
        if partitions.is_empty() {
            return Err(StreamsClientError::Runtime(format!(
                "Metadata: topic {topic} has no partitions"
            )));
        }
        Ok(partitions)
    }
}

impl BrokerFetcher {
    async fn resolve_route(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<PartitionRoute, StreamsClientError> {
        {
            let routes = self.routes.lock().await;
            if let Some(route) = route_for(&routes, topic, partition) {
                return Ok(route);
            }
        }
        self.refresh_routes().await?;
        self.cached_route(topic, partition).await
    }

    async fn cached_route(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<PartitionRoute, StreamsClientError> {
        route_for(&*self.routes.lock().await, topic, partition).ok_or_else(|| {
            StreamsClientError::Runtime(format!(
                "Metadata: partition {topic}-{partition} not present in response"
            ))
        })
    }

    async fn refresh_routes(&self) -> Result<(), StreamsClientError> {
        let meta = self.client.refresh_metadata().await?;
        *self.routes.lock().await = routes_from_metadata(&meta);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartitionRoute {
    topic_id: WireUuid,
    leader_id: i32,
}

fn route_for(
    routes: &HashMap<String, TopicRoute>,
    topic: &str,
    partition: i32,
) -> Option<PartitionRoute> {
    let topic = routes.get(topic)?;
    Some(PartitionRoute {
        topic_id: topic.topic_id,
        leader_id: *topic.leaders.get(&partition)?,
    })
}

fn routes_from_metadata(
    metadata: &crabka_protocol::owned::metadata_response::MetadataResponse,
) -> HashMap<String, TopicRoute> {
    metadata
        .topics
        .iter()
        .filter(|topic| topic.error_code == 0)
        .filter_map(|topic| {
            Some((
                topic.name.clone()?,
                TopicRoute {
                    topic_id: topic.topic_id,
                    leaders: topic
                        .partitions
                        .iter()
                        .filter(|partition| partition.error_code == 0 && partition.leader_id >= 0)
                        .map(|partition| (partition.partition_index, partition.leader_id))
                        .collect(),
                },
            ))
        })
        .collect()
}

fn missing_topic(topic: &str) -> StreamsClientError {
    StreamsClientError::Runtime(format!("Metadata: topic {topic} not present in response"))
}

fn is_stale_route_error(error_code: i16) -> bool {
    matches!(error_code, 3 | 5 | 6 | 100)
}

// ─── BrokerProducer ───────────────────────────────────────────────────────────

/// A [`RecordProducer`] backed by a real Kafka `Producer`.
///
/// This producer collects pending ack receivers in `pending`, so `flush` can
/// see per-record produce failures. This keeps the at-least-once guarantee.
pub(crate) struct BrokerProducer {
    inner: Producer,
    /// Receivers from pending `Producer::send` calls. `flush` drains them.
    pending: Mutex<Vec<oneshot::Receiver<Result<RecordMetadata, ProducerError>>>>,
}

#[async_trait::async_trait]
impl RecordProducer for BrokerProducer {
    async fn send(
        &self,
        topic: &str,
        partition: Option<i32>,
        key: Option<Bytes>,
        value: Option<Bytes>,
    ) -> Result<(), StreamsClientError> {
        let rx = self
            .inner
            .send(ProducerRecord {
                topic: topic.to_string(),
                partition,
                key,
                value,
                ..Default::default()
            })
            .await;
        self.pending.lock().await.push(rx);
        Ok(())
    }

    async fn send_with_timestamp(
        &self,
        topic: &str,
        partition: Option<i32>,
        key: Option<Bytes>,
        value: Option<Bytes>,
        timestamp_ms: Option<i64>,
    ) -> Result<(), StreamsClientError> {
        let rx = self
            .inner
            .send(ProducerRecord {
                topic: topic.to_string(),
                partition,
                key,
                value,
                timestamp_ms,
                ..Default::default()
            })
            .await;
        self.pending.lock().await.push(rx);
        Ok(())
    }

    /// Flushes the inner producer, then awaits every pending per-record ack.
    ///
    /// This method first asks the inner producer to drain its batch buffer. It
    /// then returns any `Err` result from a record ack, so the caller knows
    /// that a commit would be unsafe.
    // cargo-mutants: live producer flush and per-record ack orchestration;
    // exercised by the streams integration suite.
    #[cfg_attr(test, mutants::skip)]
    async fn flush(&self) -> Result<(), StreamsClientError> {
        self.inner.flush().await.map_err(StreamsClientError::from)?;

        let receivers: Vec<_> = std::mem::take(&mut *self.pending.lock().await);
        for rx in receivers {
            match rx.await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => {
                    return Err(StreamsClientError::Runtime(
                        "produce ack receiver dropped".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

// ─── BrokerTransactionalProducer ────────────────────────────────────────────────

/// A transactional (EOS-v2 / KIP-447) [`RecordProducer`] backed by a real Kafka
/// `Producer` built with a `transactional_id`.
///
/// This type implements [`crate::runtime::eos::TransactionalProducer`], so the
/// runtime can wrap each process-then-commit cycle in a transaction.
///
/// Unlike [`BrokerProducer`], this wrapper does NOT collect per-record ack
/// receivers. The EOS commit path never calls `flush`. It goes
/// `send` → `send_offsets_to_transaction` → `commit_transaction`, and the inner
/// `Transaction::commit` already flushes the batch buffer through
/// `end_transaction`. It also blocks on every in-flight ack before it sends the
/// COMMIT marker.
///
/// The dropped receiver does NOT cancel the send. The record's ack sender lives
/// in the accumulator batch. See `Producer::send` and `Accumulator::try_append`.
/// The producer sends the record and commits it durably even when no code
/// awaits the receiver. If this type collected receivers, they would leak
/// without a bound for the lifetime of the app.
pub(crate) struct BrokerTransactionalProducer {
    inner: Arc<Producer>,
    /// The currently-open transaction, if any. `begin_transaction` fills this
    /// field with `Producer::begin_transaction_owned`. The `OwnedTransaction`
    /// guard must survive until the separate `commit_transaction` or
    /// `abort_transaction` call, which arrives on a later poll cycle. A
    /// borrowed `Transaction<'p>` cannot live in a struct field across that
    /// gap. It would need either an unsafe self-reference or a lifetime
    /// parameter, and that lifetime parameter would break the `'static`
    /// `Arc<dyn TransactionalProducer>` storage that holds this type.
    txn: Mutex<Option<crabka_client_producer::OwnedTransaction>>,
}

#[async_trait::async_trait]
impl RecordProducer for BrokerTransactionalProducer {
    async fn send(
        &self,
        topic: &str,
        partition: Option<i32>,
        key: Option<Bytes>,
        value: Option<Bytes>,
    ) -> Result<(), StreamsClientError> {
        // Drop the returned ack receiver: under EOS the commit barrier is
        // `commit_transaction` (which flushes + awaits all in-flight records),
        // so per-record acks need not be tracked here. Dropping the receiver
        // does not cancel the queued send. Explicit `drop` (not `let _ =`)
        // because the receiver is itself a `Future`; we intentionally never
        // await it.
        drop(
            self.inner
                .send(ProducerRecord {
                    topic: topic.to_string(),
                    partition,
                    key,
                    value,
                    ..Default::default()
                })
                .await,
        );
        Ok(())
    }

    async fn send_with_timestamp(
        &self,
        topic: &str,
        partition: Option<i32>,
        key: Option<Bytes>,
        value: Option<Bytes>,
        timestamp_ms: Option<i64>,
    ) -> Result<(), StreamsClientError> {
        drop(
            self.inner
                .send(ProducerRecord {
                    topic: topic.to_string(),
                    partition,
                    key,
                    value,
                    timestamp_ms,
                    ..Default::default()
                })
                .await,
        );
        Ok(())
    }

    /// Does nothing, because the EOS path never calls `flush`.
    ///
    /// `commit_transaction` is the durability barrier. It flushes the inner
    /// producer and awaits in-flight acks before the COMMIT marker, so this
    /// method has nothing to drain.
    async fn flush(&self) -> Result<(), StreamsClientError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl TransactionalProducer for BrokerTransactionalProducer {
    async fn init_transactions(&self) -> Result<(), StreamsClientError> {
        self.inner
            .init_transactions()
            .await
            .map_err(StreamsClientError::from)
    }

    async fn begin_transaction(&self) -> Result<(), StreamsClientError> {
        let t = Arc::clone(&self.inner)
            .begin_transaction_owned()
            .await
            .map_err(StreamsClientError::from)?;
        *self.txn.lock().await = Some(t);
        Ok(())
    }

    async fn send_offsets_to_transaction(
        &self,
        offsets: &[(String, i32, i64)],
        m: &StreamsGroupMeta,
    ) -> Result<(), StreamsClientError> {
        let meta = crabka_client_consumer::ConsumerGroupMetadata {
            group_id: m.group.clone(),
            generation_id: m.generation,
            member_id: m.member.clone(),
            group_instance_id: m.group_instance.clone(),
        };
        let off = offsets.iter().map(|(t, p, o)| ((t.clone(), *p), *o));
        self.inner
            .send_offsets_to_transaction(off, &meta)
            .await
            .map_err(StreamsClientError::from)
    }

    async fn commit_transaction(&self) -> Result<(), StreamsClientError> {
        let t = self.txn.lock().await.take().ok_or_else(|| {
            StreamsClientError::Runtime(
                "commit_transaction called without an open transaction".into(),
            )
        })?;
        // On failure the broker may consider the transaction still open (e.g.
        // CONCURRENT_TRANSACTIONS), so put the guard back rather than drop
        // it -- the caller's abort-after-failed-commit recovery path (see
        // StreamThread::abort_and_rollback) needs a live guard to actually
        // reach the broker instead of failing locally with "no open
        // transaction".
        match t.commit().await {
            Ok(()) => Ok(()),
            Err(e) => {
                let source = StreamsClientError::from(e.source);
                *self.txn.lock().await = Some(e.transaction);
                Err(source)
            }
        }
    }

    async fn abort_transaction(&self) -> Result<(), StreamsClientError> {
        let t = self.txn.lock().await.take().ok_or_else(|| {
            StreamsClientError::Runtime(
                "abort_transaction called without an open transaction".into(),
            )
        })?;
        match t.abort().await {
            Ok(()) => Ok(()),
            Err(e) => {
                let source = StreamsClientError::from(e.source);
                *self.txn.lock().await = Some(e.transaction);
                Err(source)
            }
        }
    }
}

// ─── BrokerOffsetStore ────────────────────────────────────────────────────────

/// An [`OffsetStore`] backed by a real Kafka broker.
///
/// This store uses the group coordinator protocol (`OffsetFetch` /
/// `OffsetCommit` / `ListOffsets`) with the streams consumer group id.
///
/// Both `commit` and `committed` fill the v8+ `groups[]` shape AND set
/// `topic_id` on each topic, which v10 needs. The codec encodes only the
/// fields that are valid for the negotiated version, so one request
/// construction works across v0-10.
pub(crate) struct BrokerOffsetStore {
    client: Client,
    group_id: String,
    /// Cache of topic name → `topic_id`. A metadata refresh fills it lazily.
    topic_ids: Mutex<HashMap<String, WireUuid>>,
}

impl BrokerOffsetStore {
    /// Constructs a `BrokerOffsetStore` directly. Integration tests and
    /// [`build`] call it.
    pub(crate) fn new(client: Client, group_id: impl Into<String>) -> Self {
        Self {
            client,
            group_id: group_id.into(),
            topic_ids: Mutex::new(HashMap::new()),
        }
    }

    /// Looks up the `topic_id` for `topic`. On a cache miss, this method
    /// refreshes metadata.
    async fn resolve_topic_id(&self, topic: &str) -> Result<WireUuid, StreamsClientError> {
        {
            let cache = self.topic_ids.lock().await;
            if let Some(&id) = cache.get(topic) {
                return Ok(id);
            }
        }
        // Cache miss — refresh metadata.
        let meta = self.client.refresh_metadata().await?;
        let mut cache = self.topic_ids.lock().await;
        for t in &meta.topics {
            if let Some(name) = &t.name {
                cache.insert(name.clone(), t.topic_id);
            }
        }
        Ok(cache.get(topic).copied().unwrap_or_default())
    }
}

#[async_trait::async_trait]
impl OffsetStore for BrokerOffsetStore {
    /// Fetches the committed offset for `(topic, partition)` from the group
    /// coordinator.
    ///
    /// This method sends the request in the v8+ `groups[]` shape, with
    /// `topic_id` for v10. It parses the response from the `groups[]` field.
    /// For v0-7 responses it falls back to the legacy `topics` field.
    async fn committed(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, StreamsClientError> {
        let topic_id = self.resolve_topic_id(topic).await?;

        let resp = self
            .client
            .send(OffsetFetchRequest {
                // Legacy fields (v0-7): kept for version negotiation fallback.
                group_id: self.group_id.clone(),
                topics: Some(vec![OffsetFetchRequestTopic {
                    name: topic.to_string(),
                    partition_indexes: vec![partition],
                    ..Default::default()
                }]),
                // v8+ groups[] shape (also carries topic_id for v10).
                groups: vec![OffsetFetchRequestGroup {
                    group_id: self.group_id.clone(),
                    topics: Some(vec![OffsetFetchRequestTopics {
                        name: topic.to_string(),
                        topic_id,
                        partition_indexes: vec![partition],
                        ..Default::default()
                    }]),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await?;

        if resp.groups.is_empty() {
            // v0-7 fallback: data in top-level topics[].
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
        } else {
            // v8+ response: data lives in groups[].topics[].partitions[].
            for g in &resp.groups {
                for t in &g.topics {
                    // At v10 the topic name is empty; match by topic_id or
                    // accept any since we only requested one topic.
                    let name_matches = t.name.is_empty() || t.name == topic;
                    let id_matches = t.topic_id == topic_id
                        || t.topic_id == WireUuid::default()
                        || topic_id == WireUuid::default();
                    if name_matches || id_matches {
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

    async fn latest(&self, topic: &str, partition: i32) -> Result<i64, StreamsClientError> {
        let resp = self
            .client
            .send(ListOffsetsRequest {
                replica_id: -1,
                topics: vec![ListOffsetsTopic {
                    name: topic.to_string(),
                    partitions: vec![ListOffsetsPartition {
                        partition_index: partition,
                        timestamp: -1, // LIST_OFFSETS_LATEST
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

    /// Commits `offsets` to the group coordinator.
    ///
    /// This method tags each topic with its `topic_id`. `OffsetCommit` v10
    /// needs the `topic_id`, and the encoder ignores it at v0-9. The method
    /// returns an error if the broker reports a non-zero partition error code,
    /// so a broken commit is never silently swallowed.
    async fn commit(&self, offsets: &[(String, i32, i64)]) -> Result<(), StreamsClientError> {
        // Group offsets by topic name, resolving topic_ids in parallel.
        let mut by_topic: HashMap<String, Vec<(i32, i64)>> = HashMap::new();
        for (topic, partition, offset) in offsets {
            by_topic
                .entry(topic.clone())
                .or_default()
                .push((*partition, *offset));
        }

        // Resolve topic_ids for all topics that appear in this commit.
        let mut topic_ids: HashMap<String, WireUuid> = HashMap::new();
        for name in by_topic.keys() {
            let id = self.resolve_topic_id(name).await?;
            topic_ids.insert(name.clone(), id);
        }

        let topics: Vec<OffsetCommitRequestTopic> = by_topic
            .into_iter()
            .map(|(name, parts)| {
                let topic_id = topic_ids.get(&name).copied().unwrap_or_default();
                OffsetCommitRequestTopic {
                    name,
                    topic_id,
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
                }
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
/// This function wraps the producer in an `Arc` so that many `StreamTask`s in
/// the same `StreamThread` can share it.
pub(crate) async fn build(
    bootstrap: &str,
    group_id: &str,
    client_id: &str,
    broker_dns_timeout: ClientDnsTimeout,
    policy: ClientResourcePolicy,
) -> Result<(BrokerFetcher, Arc<BrokerProducer>, Arc<BrokerOffsetStore>), StreamsClientError> {
    let ClientResourcePolicy {
        dispatch_queue_capacity,
        frame_max,
        fetch_min,
    } = policy;
    // 1. Client for metadata + offset RPCs.
    let metadata_client = Client::builder()
        .bootstrap(bootstrap)
        .client_id(client_id)
        .dns_timeout(broker_dns_timeout.time())
        .dispatch_queue_capacity(dispatch_queue_capacity.get())
        .frame_max(frame_max.size())
        .build()
        .await?;

    // 2. Idempotent producer.
    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .client_id(format!("{client_id}-producer"))
        .enable_idempotence(true)
        .acks(Acks::All)
        .dns_timeout(broker_dns_timeout.time())
        .dispatch_queue_capacity(dispatch_queue_capacity.get())
        .frame_max(frame_max.size())
        .build()
        .await
        .map_err(|e| StreamsClientError::Runtime(e.to_string()))?;

    // 3. Offset client — re-use a second Client for offset RPCs so the
    //    metadata client's connection isn't head-of-line-blocked by slow
    //    OffsetFetch or OffsetCommit calls.
    let offset_client = Client::builder()
        .bootstrap(bootstrap)
        .client_id(format!("{client_id}-offsets"))
        .dns_timeout(broker_dns_timeout.time())
        .dispatch_queue_capacity(dispatch_queue_capacity.get())
        .frame_max(frame_max.size())
        .build()
        .await?;

    let fetcher = BrokerFetcher {
        client: metadata_client,
        routes: Mutex::new(HashMap::new()),
        max_wait: millis(500),
        partition_max: mebibytes(1),
        fetch_min,
    };
    let broker_producer = Arc::new(BrokerProducer {
        inner: producer,
        pending: Mutex::new(Vec::new()),
    });
    let offset_store = Arc::new(BrokerOffsetStore::new(offset_client, group_id));

    Ok((fetcher, broker_producer, offset_store))
}

// ─── build_eos ──────────────────────────────────────────────────────────────────

/// Builds broker I/O for EOS.
///
/// This function returns a transactional producer with a `transactional_id`,
/// plus the fetcher and the offset store for committed-offset reads and seek.
///
/// It mirrors [`build`] exactly, except that it constructs the producer with a
/// `transactional_id` and wraps it in a [`BrokerTransactionalProducer`].
/// Transactions imply `enable_idempotence`.
pub(crate) async fn build_eos(
    bootstrap: &str,
    group_id: &str,
    client_id: &str,
    transactional_id: &str,
    broker_dns_timeout: ClientDnsTimeout,
    policy: ClientResourcePolicy,
) -> Result<
    (
        BrokerFetcher,
        Arc<BrokerTransactionalProducer>,
        Arc<BrokerOffsetStore>,
    ),
    StreamsClientError,
> {
    let ClientResourcePolicy {
        dispatch_queue_capacity,
        frame_max,
        fetch_min,
    } = policy;
    // 1. Client for metadata + offset RPCs.
    let metadata_client = Client::builder()
        .bootstrap(bootstrap)
        .client_id(client_id)
        .dns_timeout(broker_dns_timeout.time())
        .dispatch_queue_capacity(dispatch_queue_capacity.get())
        .frame_max(frame_max.size())
        .build()
        .await?;

    // 2. Transactional producer (idempotence is implied by transactions).
    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .client_id(format!("{client_id}-producer"))
        .enable_idempotence(true)
        .acks(Acks::All)
        .transactional_id(transactional_id.to_string())
        .dns_timeout(broker_dns_timeout.time())
        .dispatch_queue_capacity(dispatch_queue_capacity.get())
        .frame_max(frame_max.size())
        .build()
        .await
        .map_err(|e| StreamsClientError::Runtime(e.to_string()))?;

    // 3. Offset client — re-use a second Client for offset RPCs so the
    //    metadata client's connection isn't head-of-line-blocked by slow
    //    OffsetFetch or OffsetCommit calls.
    let offset_client = Client::builder()
        .bootstrap(bootstrap)
        .client_id(format!("{client_id}-offsets"))
        .dns_timeout(broker_dns_timeout.time())
        .dispatch_queue_capacity(dispatch_queue_capacity.get())
        .frame_max(frame_max.size())
        .build()
        .await?;

    let fetcher = BrokerFetcher {
        client: metadata_client,
        routes: Mutex::new(HashMap::new()),
        max_wait: millis(500),
        partition_max: mebibytes(1),
        fetch_min,
    };
    let txn_producer = Arc::new(BrokerTransactionalProducer {
        inner: Arc::new(producer),
        txn: Mutex::new(None),
    });
    let offset_store = Arc::new(BrokerOffsetStore::new(offset_client, group_id));

    Ok((fetcher, txn_producer, offset_store))
}

// ─── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use bytes::Bytes;
    use crabka_broker::{Broker, BrokerConfig};
    use crabka_client_core::Client;
    use crabka_client_producer::Producer;
    use crabka_protocol::owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        metadata_response::{MetadataResponse, MetadataResponsePartition, MetadataResponseTopic},
    };
    use tokio::sync::Mutex;

    use super::{
        BrokerOffsetStore, BrokerTransactionalProducer, is_stale_route_error, route_for,
        routes_from_metadata,
    };
    use crate::{
        error::StreamsClientError,
        runtime::{
            eos::TransactionalProducer as _,
            io::{OffsetStore as _, RecordProducer as _},
        },
    };

    async fn boot() -> (crabka_broker::BrokerHandle, String, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        (broker, bootstrap, dir)
    }

    async fn create_topic(client: &Client, topic: &str, partitions: i32) {
        let resp = client
            .send(CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: topic.into(),
                    num_partitions: partitions,
                    replication_factor: 1,
                    ..Default::default()
                }],
                timeout_ms: 5_000,
                ..Default::default()
            })
            .await
            .expect("CreateTopics");
        assert_eq!(
            resp.topics[0].error_code, 0,
            "topic create failed: {resp:?}"
        );
    }

    #[test]
    fn metadata_routes_include_only_usable_partition_leaders() {
        let topic_id = crabka_protocol::primitives::uuid::Uuid([7; 16]);
        let metadata = MetadataResponse {
            topics: vec![MetadataResponseTopic {
                name: Some("orders".into()),
                topic_id,
                partitions: vec![
                    MetadataResponsePartition {
                        partition_index: 0,
                        leader_id: 2,
                        ..Default::default()
                    },
                    MetadataResponsePartition {
                        partition_index: 1,
                        leader_id: -1,
                        ..Default::default()
                    },
                    MetadataResponsePartition {
                        error_code: 3,
                        partition_index: 2,
                        leader_id: 3,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };

        let routes = routes_from_metadata(&metadata);

        assert2::assert!(route_for(&routes, "orders", 0).unwrap().topic_id == topic_id);
        assert2::assert!(route_for(&routes, "orders", 0).unwrap().leader_id == 2);
        assert2::assert!(route_for(&routes, "orders", 1).is_none());
        assert2::assert!(route_for(&routes, "orders", 2).is_none());
    }

    #[test]
    fn stale_partition_routes_refresh_only_for_metadata_errors() {
        for code in [3, 5, 6, 100] {
            assert2::assert!(is_stale_route_error(code), "code {code}");
        }
        for code in [0, 1, 29, 45] {
            assert2::assert!(!is_stale_route_error(code), "code {code}");
        }
    }

    /// Round-trip test: `committed` returns `None` before any commit, and
    /// `Some(42)` after a commit of offset 42.
    ///
    /// This test exercises both C1, which sets `topic_id` on
    /// `OffsetCommitRequestTopic`, and C2, which uses the `groups[]` shape on
    /// `OffsetFetchRequest`.
    ///
    /// The test must FAIL with the old implementation, which misses `topic_id`
    /// and sends a legacy topics-only request. It must PASS with the fixed
    /// implementation.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn offset_store_commits_and_reads_back() {
        let (_broker, bootstrap, _dir) = boot().await;

        // Admin client: create the topic so topic_id is resolvable.
        let admin = Client::builder()
            .bootstrap(&bootstrap)
            .client_id("ostore-admin")
            .build()
            .await
            .unwrap();
        create_topic(&admin, "ostore-topic", 1).await;

        // Build a BrokerOffsetStore for group "ostore-grp".
        let offset_client = Client::builder()
            .bootstrap(&bootstrap)
            .client_id("ostore-client")
            .build()
            .await
            .unwrap();
        let store = BrokerOffsetStore::new(offset_client, "ostore-grp");

        // 1. No commit yet → None.
        let before = store.committed("ostore-topic", 0).await.unwrap();
        assert_eq!(
            before, None,
            "expected no committed offset before first commit"
        );

        // 2. Commit offset 42.
        store
            .commit(&[("ostore-topic".to_string(), 0, 42)])
            .await
            .unwrap();

        // 3. Now reads back Some(42).
        let after = store.committed("ostore-topic", 0).await.unwrap();
        assert_eq!(after, Some(42), "expected committed offset 42 after commit");
    }

    /// `abort_transaction` must consume the open guard.
    ///
    /// A second call with no `begin_transaction` between the two must report
    /// "no open transaction". It must not succeed silently again. A no-op stub
    /// that never touches `self.txn` or the broker would succeed silently.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abort_transaction_consumes_the_open_guard() {
        let (_broker, bootstrap, _dir) = boot().await;

        let admin = Client::builder()
            .bootstrap(&bootstrap)
            .client_id("abort-admin")
            .build()
            .await
            .unwrap();
        create_topic(&admin, "abort-topic", 1).await;

        let inner = Producer::builder()
            .bootstrap(&bootstrap)
            .client_id("abort-producer")
            .transactional_id("abort-txn")
            .build()
            .await
            .unwrap();
        let txn_producer = BrokerTransactionalProducer {
            inner: Arc::new(inner),
            txn: Mutex::new(None),
        };

        txn_producer
            .init_transactions()
            .await
            .expect("init_transactions");
        txn_producer
            .begin_transaction()
            .await
            .expect("begin_transaction");

        // A transaction with no partitions added is still `Empty` on the
        // coordinator, which cannot transition straight to `PrepareAbort` —
        // send one record so the coordinator sees `AddPartitionsToTxn` and
        // moves to `Ongoing`, matching what any real transactional producer
        // does between `begin` and `abort`/`commit`.
        txn_producer
            .send("abort-topic", None, None, Some(Bytes::from_static(b"v")))
            .await
            .expect("send");

        // First abort succeeds and must consume the open guard.
        txn_producer.abort_transaction().await.expect("first abort");

        // No transaction is open now, so a second abort must fail rather than
        // silently succeed again.
        let err = txn_producer.abort_transaction().await.unwrap_err();
        assert!(matches!(
            &err,
            StreamsClientError::Runtime(msg) if msg.contains("without an open transaction")
        ));
    }
}
