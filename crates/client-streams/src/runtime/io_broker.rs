//! Real broker-backed implementations of the [`RecordFetcher`],
//! [`RecordProducer`], and [`OffsetStore`] I/O traits.
use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use crabka_client_core::{Client, Connection, ConnectionOptions, fetch_partition_with_isolation};
use crabka_client_producer::{Acks, Producer, ProducerError, ProducerRecord, RecordMetadata};
use crabka_protocol::{
    owned::{
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        metadata_response::MetadataResponse,
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
/// Routes each fetch to the current partition leader learned from metadata.
pub(crate) struct BrokerFetcher {
    /// Original bootstrap address used when metadata lacks a dialable endpoint.
    bootstrap: String,
    /// Metadata client used to resolve `topic_id` on cache miss.
    client: Client,
    /// Options used when dialing per-leader fetch connections.
    connection_options: ConnectionOptions,
    /// Metadata-derived fetch routing cache.
    routes: Mutex<FetchRoutes>,
    /// Cached fetch connections by broker id.
    connections: Mutex<HashMap<i32, Connection>>,
    /// Cached fallback connection to the bootstrap address.
    bootstrap_connection: Mutex<Option<Connection>>,
    /// Maximum time the broker waits before returning an empty fetch (ms).
    max_wait_ms: i32,
    /// Maximum bytes the broker returns per partition per fetch.
    partition_max_bytes: i32,
}

#[derive(Debug, Default)]
struct FetchRoutes {
    topic_ids: HashMap<String, WireUuid>,
    partition_leaders: HashMap<(String, i32), i32>,
    broker_endpoints: HashMap<i32, BrokerEndpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BrokerEndpoint {
    host: String,
    port: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FetchRoute {
    topic_id: WireUuid,
    leader_id: i32,
}

const NOT_LEADER_OR_FOLLOWER: i16 = 6;
const UNKNOWN_TOPIC_ID: i16 = 100;

#[async_trait::async_trait]
impl RecordFetcher for BrokerFetcher {
    async fn fetch(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        isolation: IsolationLevel,
    ) -> Result<FetchBatch, StreamsClientError> {
        let route = self.resolve_fetch_route(topic, partition).await?;

        // Map the runtime isolation level to the Kafka `Fetch.isolation_level`
        // wire value (READ_UNCOMMITTED = 0, READ_COMMITTED = 1).
        let isolation_level: i8 = match isolation {
            IsolationLevel::ReadUncommitted => 0,
            IsolationLevel::ReadCommitted => 1,
        };

        let conn = self.connection_for_leader(route.leader_id).await?;
        let fetched = match fetch_partition_with_isolation(
            &conn,
            topic,
            route.topic_id,
            partition,
            offset,
            self.max_wait_ms,
            self.partition_max_bytes,
            isolation_level,
        )
        .await
        {
            Ok(fetched) => fetched,
            Err(crabka_client_core::ClientError::Server { error_code })
                if is_stale_fetch_route_error(error_code) =>
            {
                self.refresh_fetch_routes().await?;
                let route = self.resolve_fetch_route(topic, partition).await?;
                let conn = self.connection_for_leader(route.leader_id).await?;
                fetch_partition_with_isolation(
                    &conn,
                    topic,
                    route.topic_id,
                    partition,
                    offset,
                    self.max_wait_ms,
                    self.partition_max_bytes,
                    isolation_level,
                )
                .await?
            }
            Err(e) if is_retryable_fetch_transport_error(&e) => {
                self.connections.lock().await.remove(&route.leader_id);
                *self.bootstrap_connection.lock().await = None;
                self.refresh_fetch_routes().await?;
                let route = self.resolve_fetch_route(topic, partition).await?;
                let conn = self.connection_for_leader(route.leader_id).await?;
                fetch_partition_with_isolation(
                    &conn,
                    topic,
                    route.topic_id,
                    partition,
                    offset,
                    self.max_wait_ms,
                    self.partition_max_bytes,
                    isolation_level,
                )
                .await?
            }
            Err(e) => return Err(e.into()),
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

    /// Resolve the real partition list for `topic` via a topic-scoped
    /// `MetadataRequest`, returning `0..partition_count`. The global consumer
    /// reads every partition to materialize the fully-replicated global store, so
    /// the default `vec![0]` would silently drop records on any partition > 0.
    async fn partitions(&self, topic: &str) -> Result<Vec<i32>, StreamsClientError> {
        let resp = self
            .client
            .send(MetadataRequest {
                topics: Some(vec![MetadataRequestTopic {
                    name: Some(topic.to_string()),
                    ..Default::default()
                }]),
                ..Default::default()
            })
            .await?;

        let count = resp
            .topics
            .iter()
            .find(|t| t.name.as_deref() == Some(topic))
            .map(|t| t.partitions.len())
            .ok_or_else(|| {
                StreamsClientError::Runtime(format!(
                    "Metadata: topic {topic} not present in response"
                ))
            })?;

        // Fall back to a single partition if the broker reports none (e.g. a
        // not-yet-created topic) so the consumer still reads partition 0.
        let count = i32::try_from(count.max(1)).unwrap_or(1);
        Ok((0..count).collect())
    }
}

impl BrokerFetcher {
    /// Look up the route for `(topic, partition)`; on miss, refresh metadata.
    async fn resolve_fetch_route(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<FetchRoute, StreamsClientError> {
        {
            let routes = self.routes.lock().await;
            if let Some(route) = routes.fetch_route(topic, partition) {
                return Ok(route);
            }
        }

        self.refresh_fetch_routes().await?;
        let routes = self.routes.lock().await;
        routes.fetch_route(topic, partition).ok_or_else(|| {
            StreamsClientError::Runtime(format!(
                "Metadata: no leader for topic {topic} partition {partition}"
            ))
        })
    }

    async fn refresh_fetch_routes(&self) -> Result<(), StreamsClientError> {
        let meta = self.client.refresh_metadata().await?;
        *self.routes.lock().await = FetchRoutes::from_metadata(&meta);
        Ok(())
    }

    async fn connection_for_leader(
        &self,
        leader_id: i32,
    ) -> Result<Connection, StreamsClientError> {
        if leader_id < 0 {
            return Err(StreamsClientError::Runtime(format!(
                "Metadata: partition has no available leader ({leader_id})"
            )));
        }

        {
            let connections = self.connections.lock().await;
            if let Some(conn) = connections.get(&leader_id) {
                return Ok(conn.clone());
            }
        }

        let Some(endpoint) = self.broker_endpoint(leader_id).await? else {
            return self.bootstrap_connection().await;
        };
        let addr = endpoint.resolve_socket_addr().await?;
        let conn = Connection::connect_with_options(addr, self.connection_options.clone()).await?;

        let mut connections = self.connections.lock().await;
        let conn = connections.entry(leader_id).or_insert(conn);
        Ok(conn.clone())
    }

    async fn broker_endpoint(
        &self,
        broker_id: i32,
    ) -> Result<Option<BrokerEndpoint>, StreamsClientError> {
        {
            let routes = self.routes.lock().await;
            if let Some(endpoint) = routes.broker_endpoints.get(&broker_id) {
                return Ok(Some(endpoint.clone()));
            }
        }

        self.refresh_fetch_routes().await?;
        let routes = self.routes.lock().await;
        Ok(routes.broker_endpoints.get(&broker_id).cloned())
    }

    async fn bootstrap_connection(&self) -> Result<Connection, StreamsClientError> {
        {
            let connection = self.bootstrap_connection.lock().await;
            if let Some(conn) = &*connection {
                return Ok(conn.clone());
            }
        }

        let addr = tokio::net::lookup_host(&self.bootstrap)
            .await
            .map_err(|e| {
                StreamsClientError::Runtime(format!("failed to resolve bootstrap address: {e}"))
            })?
            .next()
            .ok_or_else(|| {
                StreamsClientError::Runtime(format!(
                    "no addresses resolved for bootstrap: {}",
                    self.bootstrap
                ))
            })?;
        let conn = Connection::connect_with_options(addr, self.connection_options.clone()).await?;

        let mut connection = self.bootstrap_connection.lock().await;
        let conn = connection.get_or_insert(conn);
        Ok(conn.clone())
    }
}

impl FetchRoutes {
    fn from_metadata(meta: &MetadataResponse) -> Self {
        let broker_endpoints = meta
            .brokers
            .iter()
            .filter_map(|broker| {
                BrokerEndpoint::new(broker.host.clone(), broker.port)
                    .map(|endpoint| (broker.node_id, endpoint))
            })
            .collect();

        let mut topic_ids = HashMap::new();
        let mut partition_leaders = HashMap::new();
        for topic in &meta.topics {
            if topic.error_code != 0 {
                continue;
            }
            let Some(name) = topic.name.clone() else {
                continue;
            };
            topic_ids.insert(name.clone(), topic.topic_id);
            for partition in &topic.partitions {
                if partition.error_code != 0 {
                    continue;
                }
                partition_leaders.insert(
                    (name.clone(), partition.partition_index),
                    partition.leader_id,
                );
            }
        }

        Self {
            topic_ids,
            partition_leaders,
            broker_endpoints,
        }
    }

    fn fetch_route(&self, topic: &str, partition: i32) -> Option<FetchRoute> {
        let topic_id = self.topic_ids.get(topic).copied().unwrap_or_default();
        let leader_id = *self
            .partition_leaders
            .get(&(topic.to_string(), partition))?;
        Some(FetchRoute {
            topic_id,
            leader_id,
        })
    }
}

impl BrokerEndpoint {
    fn new(host: String, port: i32) -> Option<Self> {
        let port = u16::try_from(port).ok()?;
        if port == 0 {
            return None;
        }

        Some(Self { host, port })
    }

    async fn resolve_socket_addr(&self) -> Result<SocketAddr, StreamsClientError> {
        tokio::net::lookup_host((self.host.as_str(), self.port))
            .await
            .map_err(|e| {
                StreamsClientError::Runtime(format!(
                    "failed to resolve broker endpoint {}:{}: {e}",
                    self.host, self.port
                ))
            })?
            .next()
            .ok_or_else(|| {
                StreamsClientError::Runtime(format!(
                    "no addresses resolved for broker endpoint {}:{}",
                    self.host, self.port
                ))
            })
    }
}

fn is_stale_fetch_route_error(error_code: i16) -> bool {
    matches!(error_code, NOT_LEADER_OR_FOLLOWER | UNKNOWN_TOPIC_ID)
}

fn is_retryable_fetch_transport_error(error: &crabka_client_core::ClientError) -> bool {
    matches!(
        error,
        crabka_client_core::ClientError::Connect { .. }
            | crabka_client_core::ClientError::Timeout(_)
            | crabka_client_core::ClientError::Disconnected
            | crabka_client_core::ClientError::Io(_)
    )
}

// ─── BrokerProducer ───────────────────────────────────────────────────────────

/// A [`RecordProducer`] backed by a real Kafka `Producer`.
///
/// Pending ack receivers are accumulated in `pending` so that `flush` can
/// observe per-record produce failures, preserving the at-least-once guarantee.
pub(crate) struct BrokerProducer {
    inner: Producer,
    /// Receivers from pending `Producer::send` calls. Drained by `flush`.
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

    /// Flush: first ask the inner producer to drain its batch buffer, then
    /// await every pending per-record ack. Any `Err` result from a record ack
    /// is surfaced so the caller knows a commit would be unsafe.
    async fn flush(&self) -> Result<(), StreamsClientError> {
        self.inner
            .flush()
            .await
            .map_err(|e| StreamsClientError::Runtime(e.to_string()))?;

        let receivers: Vec<_> = std::mem::take(&mut *self.pending.lock().await);
        for rx in receivers {
            match rx.await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    return Err(StreamsClientError::Runtime(format!(
                        "produce ack failed: {e}"
                    )));
                }
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
/// `Producer` built with a `transactional_id`. It implements
/// [`crate::runtime::eos::TransactionalProducer`] so the runtime can wrap each
/// process-then-commit cycle in a transaction.
///
/// Unlike [`BrokerProducer`], this wrapper does NOT accumulate per-record ack
/// receivers: the EOS commit path never calls `flush` — it goes
/// `send` → `send_offsets_to_transaction` → `commit_transaction`, and the inner
/// `Transaction::commit` (via `end_transaction`) already flushes the batch
/// buffer and blocks on every in-flight ack before sending the COMMIT marker.
/// The dropped receiver
/// does NOT cancel the send: the record's ack sender lives in the accumulator
/// batch (see `Producer::send` / `Accumulator::try_append`), so the record is
/// produced and durably committed regardless of whether the receiver is awaited.
/// Accumulating receivers here would leak unboundedly for the app's lifetime.
pub(crate) struct BrokerTransactionalProducer {
    inner: Arc<Producer>,
    /// The currently-open transaction, if any. Populated by `begin_transaction`
    /// (via `Producer::begin_transaction_owned`, whose `OwnedTransaction` guard
    /// must survive across the separate `commit_transaction`/`abort_transaction`
    /// call that arrives on a later poll cycle — a borrowed `Transaction<'p>`
    /// can't be stored in a struct field across that gap without either unsafe
    /// self-reference or a lifetime parameter that would break the `'static`
    /// `Arc<dyn TransactionalProducer>` storage this type is used behind).
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

    /// No-op: the EOS path never calls `flush`. `commit_transaction` is the
    /// durability barrier (it flushes the inner producer and awaits in-flight
    /// acks before the COMMIT marker), so there is nothing to drain here.
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
            .map_err(|e| StreamsClientError::Runtime(e.to_string()))
    }

    async fn begin_transaction(&self) -> Result<(), StreamsClientError> {
        let t = Arc::clone(&self.inner)
            .begin_transaction_owned()
            .await
            .map_err(|e| StreamsClientError::Runtime(e.to_string()))?;
        *self.txn.lock().await = Some(t);
        Ok(())
    }

    async fn send_offsets_to_transaction(
        &self,
        offsets: &[(String, i32, i64)],
        m: &StreamsGroupMeta,
    ) -> Result<(), StreamsClientError> {
        let meta = crabka_client_consumer::ConsumerGroupMetadata {
            group_id: m.group_id.clone(),
            generation_id: m.generation_id,
            member_id: m.member_id.clone(),
            group_instance_id: m.group_instance_id.clone(),
        };
        let off = offsets.iter().map(|(t, p, o)| ((t.clone(), *p), *o));
        self.inner
            .send_offsets_to_transaction(off, &meta)
            .await
            .map_err(|e| StreamsClientError::Runtime(e.to_string()))
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
                let source = e.source.to_string();
                *self.txn.lock().await = Some(e.transaction);
                Err(StreamsClientError::Runtime(source))
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
                let source = e.source.to_string();
                *self.txn.lock().await = Some(e.transaction);
                Err(StreamsClientError::Runtime(source))
            }
        }
    }
}

// ─── BrokerOffsetStore ────────────────────────────────────────────────────────

/// An [`OffsetStore`] backed by a real Kafka broker.
///
/// Uses the group coordinator protocol (`OffsetFetch` / `OffsetCommit` /
/// `ListOffsets`) via the streams consumer group id.
///
/// Both `commit` and `committed` populate the v8+ `groups[]` shape AND set
/// `topic_id` on each topic (required at v10). The codec encodes only the
/// fields that are valid for the negotiated version, so a single request
/// construction works across v0-10.
pub(crate) struct BrokerOffsetStore {
    client: Client,
    group_id: String,
    /// Cache of topic name → `topic_id` (populated lazily via metadata refresh).
    topic_ids: Mutex<HashMap<String, WireUuid>>,
}

impl BrokerOffsetStore {
    /// Construct a `BrokerOffsetStore` directly. Used from integration tests and
    /// from [`build`].
    pub(crate) fn new(client: Client, group_id: impl Into<String>) -> Self {
        Self {
            client,
            group_id: group_id.into(),
            topic_ids: Mutex::new(HashMap::new()),
        }
    }

    /// Look up the `topic_id` for `topic`; refreshes metadata on cache miss.
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
    /// Fetch the committed offset for `(topic, partition)` from the group
    /// coordinator. Sends the request using the v8+ `groups[]` shape (with
    /// `topic_id` for v10). Parses the response from the `groups[]` field;
    /// falls back to the legacy `topics` field for v0-7 responses.
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

    /// Commit `offsets` to the group coordinator. Each topic is tagged with its
    /// `topic_id` (required at `OffsetCommit` v10; encoder ignores it at v0-9).
    /// Returns an error if the broker reports a non-zero partition error code so
    /// that a broken commit is never silently swallowed.
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
/// The producer is `Arc`-wrapped so it can be shared across multiple
/// `StreamTask`s within the same `StreamThread`.
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

    // 2. Idempotent producer.
    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .client_id(format!("{client_id}-producer"))
        .enable_idempotence(true)
        .acks(Acks::All)
        .build()
        .await
        .map_err(|e| StreamsClientError::Runtime(e.to_string()))?;

    // 3. Offset client — re-use a second Client for offset RPCs so the
    //    metadata client's connection isn't head-of-line-blocked by slow
    //    OffsetFetch or OffsetCommit calls.
    let offset_client = Client::builder()
        .bootstrap(bootstrap)
        .client_id(format!("{client_id}-offsets"))
        .build()
        .await?;

    let fetcher = BrokerFetcher {
        bootstrap: bootstrap.to_string(),
        client: metadata_client,
        connection_options: ConnectionOptions {
            client_id: client_id.to_string(),
            ..Default::default()
        },
        routes: Mutex::new(FetchRoutes::default()),
        connections: Mutex::new(HashMap::new()),
        bootstrap_connection: Mutex::new(None),
        max_wait_ms: 500,
        partition_max_bytes: 1 << 20,
    };
    let broker_producer = Arc::new(BrokerProducer {
        inner: producer,
        pending: Mutex::new(Vec::new()),
    });
    let offset_store = Arc::new(BrokerOffsetStore::new(offset_client, group_id));

    Ok((fetcher, broker_producer, offset_store))
}

// ─── build_eos ──────────────────────────────────────────────────────────────────

/// Build broker I/O for EOS: a transactional producer (with `transactional_id`),
/// plus the fetcher + offset store (for committed-offset reads / seek).
///
/// Mirrors [`build`] exactly, except the producer is constructed with a
/// `transactional_id` (and `enable_idempotence` is implied by transactions) and
/// wrapped in a [`BrokerTransactionalProducer`].
pub(crate) async fn build_eos(
    bootstrap: &str,
    group_id: &str,
    client_id: &str,
    transactional_id: &str,
) -> Result<
    (
        BrokerFetcher,
        Arc<BrokerTransactionalProducer>,
        Arc<BrokerOffsetStore>,
    ),
    StreamsClientError,
> {
    // 1. Client for metadata + offset RPCs.
    let metadata_client = Client::builder()
        .bootstrap(bootstrap)
        .client_id(client_id)
        .build()
        .await?;

    // 2. Transactional producer (idempotence is implied by transactions).
    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .client_id(format!("{client_id}-producer"))
        .enable_idempotence(true)
        .acks(Acks::All)
        .transactional_id(transactional_id.to_string())
        .build()
        .await
        .map_err(|e| StreamsClientError::Runtime(e.to_string()))?;

    // 3. Offset client — re-use a second Client for offset RPCs so the
    //    metadata client's connection isn't head-of-line-blocked by slow
    //    OffsetFetch or OffsetCommit calls.
    let offset_client = Client::builder()
        .bootstrap(bootstrap)
        .client_id(format!("{client_id}-offsets"))
        .build()
        .await?;

    let fetcher = BrokerFetcher {
        bootstrap: bootstrap.to_string(),
        client: metadata_client,
        connection_options: ConnectionOptions {
            client_id: client_id.to_string(),
            ..Default::default()
        },
        routes: Mutex::new(FetchRoutes::default()),
        connections: Mutex::new(HashMap::new()),
        bootstrap_connection: Mutex::new(None),
        max_wait_ms: 500,
        partition_max_bytes: 1 << 20,
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
    use crabka_protocol::{
        owned::{
            create_topics_request::{CreatableTopic, CreateTopicsRequest},
            metadata_response::{
                MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
                MetadataResponseTopic,
            },
        },
        primitives::uuid::Uuid as WireUuid,
    };
    use tokio::sync::Mutex;

    use super::{BrokerEndpoint, BrokerOffsetStore, BrokerTransactionalProducer, FetchRoutes};
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
    fn fetch_routes_map_partitions_to_their_metadata_leaders() {
        let topic_id = WireUuid([7; 16]);
        let routes = FetchRoutes::from_metadata(&MetadataResponse {
            brokers: vec![
                MetadataResponseBroker {
                    node_id: 1,
                    host: "broker-one".into(),
                    port: 19_092,
                    ..Default::default()
                },
                MetadataResponseBroker {
                    node_id: 2,
                    host: "broker-two".into(),
                    port: 29_092,
                    ..Default::default()
                },
            ],
            topics: vec![MetadataResponseTopic {
                name: Some("routed-topic".into()),
                topic_id,
                partitions: vec![
                    MetadataResponsePartition {
                        partition_index: 0,
                        leader_id: 1,
                        ..Default::default()
                    },
                    MetadataResponsePartition {
                        partition_index: 1,
                        leader_id: 2,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        });

        assert_eq!(
            routes.fetch_route("routed-topic", 0).unwrap().topic_id,
            topic_id
        );
        assert_eq!(routes.fetch_route("routed-topic", 0).unwrap().leader_id, 1);
        assert_eq!(routes.fetch_route("routed-topic", 1).unwrap().leader_id, 2);
        assert_eq!(
            routes.broker_endpoints.get(&2),
            Some(&BrokerEndpoint {
                host: "broker-two".into(),
                port: 29_092,
            })
        );
    }

    #[test]
    fn fetch_routes_ignore_unusable_metadata_entries() {
        let routes = FetchRoutes::from_metadata(&MetadataResponse {
            brokers: vec![
                MetadataResponseBroker {
                    node_id: 1,
                    host: "broker-one".into(),
                    port: 0,
                    ..Default::default()
                },
                MetadataResponseBroker {
                    node_id: 2,
                    host: "broker-two".into(),
                    port: i32::from(u16::MAX) + 1,
                    ..Default::default()
                },
            ],
            topics: vec![MetadataResponseTopic {
                name: Some("routed-topic".into()),
                partitions: vec![
                    MetadataResponsePartition {
                        error_code: 3,
                        partition_index: 0,
                        leader_id: 1,
                        ..Default::default()
                    },
                    MetadataResponsePartition {
                        partition_index: 1,
                        leader_id: 2,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        });

        assert!(routes.fetch_route("routed-topic", 0).is_none());
        assert_eq!(routes.fetch_route("routed-topic", 1).unwrap().leader_id, 2);
        assert!(routes.broker_endpoints.is_empty());
    }

    /// Round-trip test: `committed` returns `None` before any commit, `Some(42)`
    /// after committing offset 42. This exercises both C1 (`topic_id` set on
    /// `OffsetCommitRequestTopic`) and C2 (`groups[]` shape on `OffsetFetchRequest`).
    ///
    /// The test must FAIL with the old implementation (missing `topic_id` + legacy
    /// topics-only request) and PASS with the fixed implementation.
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

    /// `abort_transaction` must consume the open guard: a second call with no
    /// intervening `begin_transaction` has to report "no open transaction"
    /// rather than silently succeeding again, which is what a no-op stub
    /// (never touching `self.txn` or the broker) would do instead.
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
