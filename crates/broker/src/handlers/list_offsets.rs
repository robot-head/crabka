//! `ListOffsets` (`api_key=2`). The handler resolves the EARLIEST / LATEST
//! sentinels with each partition's log. For tiered topics (KIP-405),
//! EARLIEST and by-timestamp lookups consult the
//! [`RemoteLogMetadataManager`](crabka_remote_storage::RemoteLogMetadataManager).
//! Local-retention deletes some offsets locally, but they still live in the
//! remote tier, and this keeps them visible. KIP-1005's latest-tiered (`-5`)
//! and KIP-1023's earliest-pending-upload (`-6`) sentinels read the same
//! metadata asynchronously. KIP-1075 bounds that remote work by the request
//! timeout and resolves all requested partitions concurrently.
//!
//! Positive-timestamp lookups resolve against the remote tier first, because
//! it holds the oldest records. They then fall back to the local log's
//! time index (KIP-405/734). The handler resolves the `MAX_TIMESTAMP` (-3) and
//! `EARLIEST_LOCAL_TIMESTAMP` (-4) sentinels against the local log.

use std::{future::Future, time::Duration};

use bytes::Bytes;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode,
    owned::{
        list_offsets_request::ListOffsetsRequest,
        list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
        },
    },
};

use crate::{broker::Broker, codes, error::BrokerError};

/// Request timestamp sentinel (-2): resolve the earliest available offset.
/// Kafka's `ListOffsetsRequest.EARLIEST_TIMESTAMP`.
const EARLIEST_TIMESTAMP: i64 = -2;
/// Request timestamp sentinel (-1): resolve the log-end (next) offset.
/// Kafka's `ListOffsetsRequest.LATEST_TIMESTAMP`.
const LATEST_TIMESTAMP: i64 = -1;
/// Request timestamp sentinel (-3, KIP-734): resolve the offset of the record
/// with the highest timestamp. Kafka's `ListOffsetsRequest.MAX_TIMESTAMP`.
const MAX_TIMESTAMP: i64 = -3;
/// Request timestamp sentinel (-4, KIP-405): resolve the earliest offset still
/// in local storage. Kafka's `ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP`.
const EARLIEST_LOCAL_TIMESTAMP: i64 = -4;
/// Request timestamp sentinel (-5, KIP-1005): resolve the highest offset in a
/// finished remote segment.
const LATEST_TIERED_TIMESTAMP: i64 = -5;
/// Request timestamp sentinel (-6, KIP-1023): resolve the first offset that
/// has not been uploaded to the remote tier.
const EARLIEST_PENDING_UPLOAD_TIMESTAMP: i64 = -6;
/// Response placeholder (-1) meaning "no record timestamp matched/echoed".
/// Kafka's `ListOffsetsResponse.UNKNOWN_TIMESTAMP`.
const UNKNOWN_TIMESTAMP: i64 = -1;
/// Response placeholder (-1) meaning "no offset was resolved".
/// Kafka's `ListOffsetsResponse.UNKNOWN_OFFSET`.
const UNKNOWN_OFFSET: i64 = -1;
async fn concurrently<F>(futures: impl IntoIterator<Item = F>) -> Vec<F::Output>
where
    F: Future,
{
    futures_util::future::join_all(futures).await
}

async fn await_remote<T>(
    timeout: Duration,
    future: impl Future<Output = Result<T, crabka_remote_storage::RemoteStorageError>>,
) -> Option<Result<T, crabka_remote_storage::RemoteStorageError>> {
    tokio::time::timeout(timeout, future).await.ok()
}

fn remote_timeout(version: i16, timeout_ms: i32, server_timeout: Duration) -> Duration {
    if version >= 10 && timeout_ms > 0 {
        Duration::from_millis(u64::from(timeout_ms.unsigned_abs()))
    } else {
        server_timeout
    }
}

fn timestamp_supported(timestamp: i64, version: i16) -> bool {
    let minimum_version = match timestamp {
        EARLIEST_TIMESTAMP | LATEST_TIMESTAMP => 0,
        MAX_TIMESTAMP => 7,
        EARLIEST_LOCAL_TIMESTAMP => 8,
        LATEST_TIERED_TIMESTAMP => 9,
        EARLIEST_PENDING_UPLOAD_TIMESTAMP => 11,
        timestamp if timestamp >= 0 => return true,
        _ => return false,
    };
    version >= minimum_version
}

fn error_response(partition_index: i32, error_code: i16) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse {
        partition_index,
        error_code,
        timestamp: UNKNOWN_TIMESTAMP,
        offset: UNKNOWN_OFFSET,
        ..Default::default()
    }
}

async fn diskless_earliest_offset(
    local_start: i64,
    diskless_read: Option<&crate::diskless::read::DisklessReadHandle>,
    topic_id: Option<uuid::Uuid>,
    partition: i32,
) -> i64 {
    let Some((handle, topic_id)) = diskless_read.zip(topic_id) else {
        return local_start;
    };
    handle
        .index
        .lock()
        .await
        .earliest_covered(topic_id, partition)
        .map_or(local_start, |object_start| local_start.min(object_start))
}

#[tracing::instrument(
    name = "handle_list_offsets",
    level = "info",
    skip_all,
    fields(api = "ListOffsets", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let controller = broker.controller.clone();
    {
        let mut cur: &[u8] = req_bytes;
        let req = ListOffsetsRequest::decode(&mut cur, version)?;

        // ── ACL preamble ────────────────────────────────────────────
        // Per-topic `Describe` on `Topic(name)`. A denied topic gets
        // `TOPIC_AUTHORIZATION_FAILED (29)` on every partition row it
        // requested; authorized topics proceed unchanged.
        let acl_image = controller.current_image();

        let timeout = remote_timeout(
            version,
            req.timeout_ms,
            crate::config_keys::resolve_remote_list_offsets_timeout(
                &acl_image,
                broker.config.node_id,
            ),
        );
        let topics_out = concurrently(req.topics.into_iter().map(|topic| {
            let acl_image = acl_image.clone();
            async move {
                let name = topic.name;
                let partitions = if crate::handlers::acl_denied(
                    broker.config.authorizer.as_ref(),
                    &acl_image,
                    ctx,
                    ResourceType::Topic,
                    &name,
                    AclOperation::Describe,
                ) {
                    topic
                        .partitions
                        .into_iter()
                        .map(|part| {
                            error_response(part.partition_index, codes::TOPIC_AUTHORIZATION_FAILED)
                        })
                        .collect()
                } else {
                    concurrently(
                        topic
                            .partitions
                            .into_iter()
                            .map(|part| resolve_partition(broker, &name, part, version, timeout)),
                    )
                    .await
                };
                ListOffsetsTopicResponse {
                    name,
                    partitions,
                    ..Default::default()
                }
            }
        }))
        .await;

        let resp = ListOffsetsResponse {
            throttle_time_ms: 0,
            topics: topics_out,
            ..Default::default()
        };
        crate::handlers::encode_response(&resp, version)
    }
}

async fn resolve_partition(
    broker: &Broker,
    topic_name: &str,
    request: crabka_protocol::owned::list_offsets_request::ListOffsetsPartition,
    version: i16,
    remote_timeout: Duration,
) -> ListOffsetsPartitionResponse {
    let index = request.partition_index;
    let mut response = ListOffsetsPartitionResponse {
        partition_index: index,
        timestamp: UNKNOWN_TIMESTAMP,
        ..Default::default()
    };
    if !timestamp_supported(request.timestamp, version) {
        response.error_code = codes::UNSUPPORTED_VERSION;
        response.offset = UNKNOWN_OFFSET;
        return response;
    }
    let Some(partition) = broker
        .partitions
        .get(topic_name, crabka_ids::PartitionIndex(index))
    else {
        response.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
        return response;
    };
    let (local_start, local_end, local_log_start, remote_enabled) = {
        let log = partition.log.lock().expect("log mutex poisoned");
        (
            log.log_start_offset().0,
            log.log_end_offset().0,
            log.local_log_start_offset().0,
            log.config_snapshot().remote_storage_enable,
        )
    };
    let diskless = partition.diskless && broker.diskless_read.is_some();
    let topic_id = if (remote_enabled && broker.remote_reader.is_some()) || diskless {
        broker
            .controller
            .current_image()
            .topic(topic_name)
            .map(|topic| topic.topic_id)
    } else {
        None
    };
    let remote_topic_id = if remote_enabled { topic_id } else { None };
    let (offset, timestamp) = match request.timestamp {
        EARLIEST_TIMESTAMP => {
            let mut earliest = local_start;
            if let (Some(reader), Some(id)) = (broker.remote_reader.as_ref(), remote_topic_id) {
                let topic_partition =
                    crabka_remote_storage::TopicIdPartition::new(id, topic_name.to_string(), index);
                match await_remote(remote_timeout, reader.earliest_offset(&topic_partition)).await {
                    None => return error_response(index, codes::REQUEST_TIMED_OUT),
                    Some(Ok(Some(remote_start))) => earliest = earliest.min(remote_start),
                    Some(Ok(None)) => {}
                    Some(Err(error)) => tracing::warn!(topic = topic_name, partition = index,
                        error = %error, "list_offsets: remote earliest_offset failed"),
                }
            }
            earliest = diskless_earliest_offset(
                earliest,
                broker.diskless_read.as_deref(),
                topic_id,
                index,
            )
            .await;
            (earliest, UNKNOWN_TIMESTAMP)
        }
        LATEST_TIMESTAMP => (local_end, UNKNOWN_TIMESTAMP),
        EARLIEST_LOCAL_TIMESTAMP => {
            let offset = if remote_enabled {
                local_log_start
            } else {
                local_start
            };
            response.leader_epoch = leader_epoch_for_offset(&partition, offset);
            (offset, UNKNOWN_TIMESTAMP)
        }
        LATEST_TIERED_TIMESTAMP => {
            if let Some((reader, id)) = broker.remote_reader.as_ref().zip(remote_topic_id) {
                let topic_partition =
                    crabka_remote_storage::TopicIdPartition::new(id, topic_name.to_string(), index);
                match await_remote(
                    remote_timeout,
                    reader.latest_tiered_offset(&topic_partition),
                )
                .await
                {
                    None => return error_response(index, codes::REQUEST_TIMED_OUT),
                    Some(Ok(Some(tiered))) => {
                        response.leader_epoch = tiered.leader_epoch.0;
                        (tiered.offset, UNKNOWN_TIMESTAMP)
                    }
                    Some(Ok(None)) => (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP),
                    Some(Err(error)) => {
                        tracing::warn!(topic = topic_name, partition = index,
                            error = %error, "list_offsets: remote latest_tiered_offset failed");
                        (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP)
                    }
                }
            } else {
                (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP)
            }
        }
        EARLIEST_PENDING_UPLOAD_TIMESTAMP => {
            if let Some((reader, id)) = broker.remote_reader.as_ref().zip(remote_topic_id) {
                let topic_partition =
                    crabka_remote_storage::TopicIdPartition::new(id, topic_name.to_string(), index);
                match await_remote(
                    remote_timeout,
                    reader.latest_tiered_offset(&topic_partition),
                )
                .await
                {
                    None => return error_response(index, codes::REQUEST_TIMED_OUT),
                    Some(Ok(Some(tiered))) => {
                        // KIP-1023 deliberately allows this to be below the
                        // leader's log-start offset. That tells an empty
                        // follower the remote tier currently has no valid
                        // segment and it must rebuild from local storage.
                        let offset = tiered.offset.saturating_add(1);
                        let local_epoch = leader_epoch_for_offset(&partition, offset);
                        response.leader_epoch = if local_epoch < 0 {
                            tiered.leader_epoch.0
                        } else {
                            local_epoch
                        };
                        (offset, UNKNOWN_TIMESTAMP)
                    }
                    Some(Ok(None)) => (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP),
                    Some(Err(error)) => {
                        tracing::warn!(topic = topic_name, partition = index,
                            error = %error, "list_offsets: remote earliest_pending_upload failed");
                        (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP)
                    }
                }
            } else {
                (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP)
            }
        }
        MAX_TIMESTAMP => {
            let log = partition.log.lock().expect("log mutex poisoned");
            log.max_timestamp_offset_and_ts().map_or_else(
                || (log.offset_of_max_timestamp().0, UNKNOWN_TIMESTAMP),
                |(offset, timestamp)| (offset.0, timestamp),
            )
        }
        requested_timestamp if requested_timestamp >= 0 => {
            match resolve_timestamp_offset(
                broker,
                &partition,
                topic_name,
                index,
                remote_topic_id,
                requested_timestamp,
                remote_timeout,
            )
            .await
            {
                Some(result) => result,
                None => return error_response(index, codes::REQUEST_TIMED_OUT),
            }
        }
        _ => (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP),
    };
    response.error_code = codes::NONE;
    response.offset = offset;
    response.timestamp = timestamp;
    response
}

fn leader_epoch_for_offset(partition: &crate::partition::Partition, offset: i64) -> i32 {
    let log = partition.log.lock().expect("log mutex poisoned");
    log.epoch_checkpoint()
        .epoch_for_offset(crabka_log::Offset(offset))
        .map_or(-1, |epoch| epoch.0)
}

async fn resolve_timestamp_offset(
    broker: &Broker,
    partition: &crate::partition::Partition,
    topic_name: &str,
    partition_index: i32,
    topic_id: Option<uuid::Uuid>,
    timestamp: i64,
    remote_timeout: Duration,
) -> Option<(i64, i64)> {
    if let (Some(reader), Some(id)) = (broker.remote_reader.as_ref(), topic_id) {
        let topic_partition = crabka_remote_storage::TopicIdPartition::new(
            id,
            topic_name.to_string(),
            partition_index,
        );
        match await_remote(
            remote_timeout,
            reader.offset_for_timestamp(&topic_partition, timestamp),
        )
        .await
        {
            None => return None,
            Some(Ok(Some(offset_and_timestamp))) => return Some(offset_and_timestamp),
            Some(Ok(None)) => {}
            Some(Err(error)) => tracing::warn!(topic = topic_name, partition = partition_index,
                error = %error, "list_offsets: remote offset_for_timestamp failed"),
        }
    }
    let log = partition.log.lock().expect("log mutex poisoned");
    Some(
        log.offset_for_timestamp(timestamp)
            .map_or((UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP), |(offset, matched)| {
                (offset.0, matched)
            }),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use bytes::BytesMut;
    use crabka_protocol::{
        Encode,
        owned::{
            create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
            list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
        },
    };

    use crate::test_support::{DenyAll, peer, principal};

    crate::test_support::wire_helpers!(
        ListOffsetsRequest,
        ListOffsetsResponse,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

    async fn client_for(broker: &crate::broker::BrokerHandle) -> crabka_client_core::Client {
        crabka_client_core::Client::builder()
            .bootstrap(broker.listen_addr().to_string())
            .client_id("list-offsets-test")
            .build()
            .await
            .expect("client build")
    }

    async fn create_topic(
        client: &crabka_client_core::Client,
        name: &str,
        configs: Vec<CreatableTopicConfig>,
    ) {
        let response = client
            .send(CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: name.to_string(),
                    num_partitions: 1,
                    replication_factor: 1,
                    configs,
                    ..Default::default()
                }],
                timeout_ms: 5_000,
                ..Default::default()
            })
            .await
            .expect("CreateTopics");
        assert!(response.topics[0].error_code == codes::NONE, "{response:?}");
    }

    async fn list_one(
        client: &crabka_client_core::Client,
        topic: &str,
        timestamp: i64,
    ) -> ListOffsetsPartitionResponse {
        client
            .send(ListOffsetsRequest {
                replica_id: -1,
                topics: vec![ListOffsetsTopic {
                    name: topic.to_string(),
                    partitions: vec![ListOffsetsPartition {
                        partition_index: 0,
                        timestamp,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                timeout_ms: 5_000,
                ..Default::default()
            })
            .await
            .expect("ListOffsets")
            .topics
            .remove(0)
            .partitions
            .remove(0)
    }

    #[tokio::test]
    async fn diskless_earliest_uses_object_floor_but_leaves_local_floor_visible() {
        let topic_id = uuid::Uuid::from_u128(42);
        let mut cache = crate::diskless::wal_index::WalIndexCache::default();
        cache.apply(&crate::diskless::wal_index::WalFlushRecord {
            object_key: "o".into(),
            format_version: 1,
            entries: vec![crate::diskless::wal_index::WalIndexEntry {
                topic_id,
                partition: 0,
                first_offset: 0,
                last_offset: 8,
                byte_start: 0,
                byte_len: 1,
            }],
        });
        let handle = crate::diskless::read::DisklessReadHandle::new(
            Arc::new(tokio::sync::Mutex::new(cache)),
            Arc::new(object_store::memory::InMemory::new()),
        );

        let earliest = diskless_earliest_offset(9, Some(&handle), Some(topic_id), 0).await;
        assert!(earliest == 0);
    }

    #[test]
    fn sentinel_constants_match_kafka_wire_values() {
        let cases = [
            ("EARLIEST_TIMESTAMP", EARLIEST_TIMESTAMP, -2),
            ("LATEST_TIMESTAMP", LATEST_TIMESTAMP, -1),
            ("MAX_TIMESTAMP", MAX_TIMESTAMP, -3),
            ("EARLIEST_LOCAL_TIMESTAMP", EARLIEST_LOCAL_TIMESTAMP, -4),
            ("LATEST_TIERED_TIMESTAMP", LATEST_TIERED_TIMESTAMP, -5),
            (
                "EARLIEST_PENDING_UPLOAD_TIMESTAMP",
                EARLIEST_PENDING_UPLOAD_TIMESTAMP,
                -6,
            ),
        ];
        for (name, sentinel, want) in cases {
            assert!(sentinel == want, "{name}");
        }
    }

    #[test]
    fn tiered_sentinels_require_their_kafka_versions() {
        let cases = [
            (EARLIEST_TIMESTAMP, 0, true),
            (LATEST_TIMESTAMP, 0, true),
            (MAX_TIMESTAMP, 6, false),
            (MAX_TIMESTAMP, 7, true),
            (EARLIEST_LOCAL_TIMESTAMP, 7, false),
            (EARLIEST_LOCAL_TIMESTAMP, 8, true),
            (LATEST_TIERED_TIMESTAMP, 8, false),
            (LATEST_TIERED_TIMESTAMP, 9, true),
            (EARLIEST_PENDING_UPLOAD_TIMESTAMP, 10, false),
            (EARLIEST_PENDING_UPLOAD_TIMESTAMP, 11, true),
            (-7, 11, false),
            (0, 1, true),
        ];
        for (timestamp, version, expected) in cases {
            assert!(timestamp_supported(timestamp, version) == expected);
        }
    }

    #[test]
    fn remote_timeout_uses_v10_positive_value_or_server_default() {
        let server_timeout = Duration::from_millis(123);
        let cases = [
            (9, 7, server_timeout),
            (10, -1, server_timeout),
            (10, 0, server_timeout),
            (10, 7, Duration::from_millis(7)),
            (11, 19, Duration::from_millis(19)),
        ];
        for (version, timeout_ms, expected) in cases {
            assert!(remote_timeout(version, timeout_ms, server_timeout) == expected);
        }
    }

    #[test]
    fn remote_timeout_resolves_dynamic_broker_over_cluster_default() {
        use crabka_metadata::{BrokerConfigRecord, MetadataRecord, NodeId};

        let mut image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        for (node_id, value) in [
            (crabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID, "400"),
            (NodeId(1), "250"),
        ] {
            image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id,
                config_name: crate::config_keys::REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS.to_owned(),
                config_value: Some(value.to_owned()),
            }));
        }

        assert!(
            crate::config_keys::resolve_remote_list_offsets_timeout(&image, NodeId(1))
                == Duration::from_millis(250)
        );
        assert!(
            crate::config_keys::resolve_remote_list_offsets_timeout(&image, NodeId(2))
                == Duration::from_millis(400)
        );
    }

    #[tokio::test]
    async fn remote_work_expires_at_the_request_deadline() {
        let result = await_remote(
            Duration::from_millis(1),
            std::future::pending::<Result<(), crabka_remote_storage::RemoteStorageError>>(),
        )
        .await;
        assert!(result.is_none());
        assert!(
            error_response(3, codes::REQUEST_TIMED_OUT)
                == ListOffsetsPartitionResponse {
                    partition_index: 3,
                    error_code: codes::REQUEST_TIMED_OUT,
                    timestamp: UNKNOWN_TIMESTAMP,
                    offset: UNKNOWN_OFFSET,
                    ..Default::default()
                }
        );
    }

    #[tokio::test]
    async fn partition_futures_are_polled_concurrently() {
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let work = (0..2).map(|value| {
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                value
            }
        });
        let output = tokio::time::timeout(Duration::from_secs(1), concurrently(work))
            .await
            .expect("both futures must start before either can finish");
        assert!(output == vec![0, 1]);
    }

    #[tokio::test]
    async fn non_tiered_sentinels_use_ordinary_earliest_and_unknown_remote_offsets() {
        const TOPIC: &str = "list-offsets-local";

        let (broker, _dir) = crate::test_support::start_broker_with(|config| {
            config.audit_enabled = false;
        })
        .await;
        let client = client_for(&broker).await;
        create_topic(&client, TOPIC, Vec::new()).await;
        broker.wait_until_partition_present(TOPIC, 0).await;
        broker
            .produce_records_for_test(TOPIC, 0, 8)
            .await
            .expect("produce");
        broker
            .test_advance_log_start(TOPIC, 0, 5)
            .await
            .expect("advance log start");

        assert!(
            list_one(&client, TOPIC, EARLIEST_LOCAL_TIMESTAMP).await
                == ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::NONE,
                    timestamp: UNKNOWN_TIMESTAMP,
                    offset: 5,
                    leader_epoch: 0,
                    ..Default::default()
                }
        );
        for timestamp in [LATEST_TIERED_TIMESTAMP, EARLIEST_PENDING_UPLOAD_TIMESTAMP] {
            assert!(
                list_one(&client, TOPIC, timestamp).await
                    == ListOffsetsPartitionResponse {
                        partition_index: 0,
                        error_code: codes::NONE,
                        timestamp: UNKNOWN_TIMESTAMP,
                        offset: UNKNOWN_OFFSET,
                        leader_epoch: -1,
                        ..Default::default()
                    }
            );
        }

        drop(client);
        broker.shutdown().await;
    }

    #[tokio::test]
    async fn tiered_sentinels_return_finished_remote_frontier_and_pending_epoch() {
        use std::collections::BTreeMap;

        use crabka_ids::LeaderEpoch;
        use crabka_remote_storage::{
            RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentMetadata,
            RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, TopicIdPartition,
        };

        const TOPIC: &str = "list-offsets-tiered";

        let remote_dir = tempfile::tempdir().expect("remote tempdir");
        let remote_path = remote_dir.path().to_path_buf();
        let (broker, _dir) = crate::test_support::start_broker_with(move |config| {
            config.audit_enabled = false;
            config.remote_storage_backend =
                Some(crate::config::RemoteStorageBackend::Local { dir: remote_path });
        })
        .await;
        let client = client_for(&broker).await;
        create_topic(
            &client,
            TOPIC,
            vec![CreatableTopicConfig {
                name: "remote.storage.enable".into(),
                value: Some("true".into()),
                ..Default::default()
            }],
        )
        .await;
        broker.wait_until_partition_present(TOPIC, 0).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if broker
                    .partition_log_config_for_test(TOPIC, 0)
                    .is_some_and(|config| config.remote_storage_enable)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("remote topic config propagated");
        broker
            .produce_records_for_test(TOPIC, 0, 10)
            .await
            .expect("produce");

        let broker_arc = broker.broker_arc_for_test();
        let topic_id = broker_arc
            .controller
            .current_image()
            .topic(TOPIC)
            .expect("topic metadata")
            .topic_id;
        let topic_partition = TopicIdPartition::new(topic_id, TOPIC, 0);
        let rlmm = broker_arc
            .remote_reader
            .as_ref()
            .expect("remote reader")
            .rlmm
            .clone();
        let finished_id = RemoteLogSegmentId::new(topic_partition.clone(), uuid::Uuid::new_v4());
        let finished = RemoteLogSegmentMetadata::new(
            finished_id.clone(),
            0,
            4,
            0,
            1,
            0,
            RemoteLogSegmentDetails::new(
                1,
                RemoteLogSegmentState::CopySegmentStarted,
                BTreeMap::from([(LeaderEpoch(0), 0)]),
            ),
        )
        .expect("finished metadata");
        rlmm.add_remote_log_segment_metadata(finished)
            .expect("add finished segment");
        rlmm.update_remote_log_segment_metadata(RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: finished_id,
            event_timestamp_ms: 0,
            custom_metadata: None,
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        })
        .expect("finish segment");
        rlmm.add_remote_log_segment_metadata(
            RemoteLogSegmentMetadata::new(
                RemoteLogSegmentId::new(topic_partition, uuid::Uuid::new_v4()),
                5,
                8,
                0,
                1,
                0,
                RemoteLogSegmentDetails::new(
                    1,
                    RemoteLogSegmentState::CopySegmentStarted,
                    BTreeMap::from([(LeaderEpoch(0), 5)]),
                ),
            )
            .expect("started metadata"),
        )
        .expect("add in-progress segment");

        assert!(
            list_one(&client, TOPIC, LATEST_TIERED_TIMESTAMP).await
                == ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::NONE,
                    timestamp: UNKNOWN_TIMESTAMP,
                    offset: 4,
                    leader_epoch: 0,
                    ..Default::default()
                }
        );
        assert!(
            list_one(&client, TOPIC, EARLIEST_PENDING_UPLOAD_TIMESTAMP).await
                == ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::NONE,
                    timestamp: UNKNOWN_TIMESTAMP,
                    offset: 5,
                    leader_epoch: 0,
                    ..Default::default()
                }
        );

        // KIP-1023 requires returning the raw remote frontier even when it is
        // now below the leader log-start offset. The follower interprets that
        // relation as "no currently valid remote segments" and rebuilds from
        // the leader's local log.
        broker
            .test_advance_log_start(TOPIC, 0, 7)
            .await
            .expect("advance leader log start");
        assert!(
            list_one(&client, TOPIC, EARLIEST_PENDING_UPLOAD_TIMESTAMP).await
                == ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::NONE,
                    timestamp: UNKNOWN_TIMESTAMP,
                    offset: 5,
                    leader_epoch: 0,
                    ..Default::default()
                }
        );

        drop(client);
        broker.shutdown().await;
    }

    #[tokio::test]
    async fn positive_timestamp_wire_response_returns_exact_remote_record() {
        use std::collections::BTreeMap;

        use bytes::Bytes;
        use crabka_ids::LeaderEpoch;
        use crabka_protocol::records::{Record, RecordBatch};
        use crabka_remote_storage::{
            LogSegmentData, RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentMetadata,
            RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, TopicIdPartition,
        };

        const TOPIC: &str = "list-offsets-remote-timestamp";

        let remote_dir = tempfile::tempdir().expect("remote tempdir");
        let remote_path = remote_dir.path().to_path_buf();
        let (broker, _dir) = crate::test_support::start_broker_with(move |config| {
            config.audit_enabled = false;
            config.remote_storage_backend =
                Some(crate::config::RemoteStorageBackend::Local { dir: remote_path });
        })
        .await;
        let client = client_for(&broker).await;
        create_topic(
            &client,
            TOPIC,
            vec![CreatableTopicConfig {
                name: "remote.storage.enable".into(),
                value: Some("true".into()),
                ..Default::default()
            }],
        )
        .await;
        broker.wait_until_partition_present(TOPIC, 0).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if broker
                    .partition_log_config_for_test(TOPIC, 0)
                    .is_some_and(|config| config.remote_storage_enable)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("remote topic config propagated");

        let source_dir = tempfile::tempdir().expect("source tempdir");
        let batches = [
            (0, &[1_000, 1_100][..]),
            (2, &[1_600, 1_700][..]),
            (4, &[2_000, 2_200, 2_400][..]),
        ];
        let mut log_bytes = BytesMut::new();
        let mut last_position = 0;
        for (base_offset, timestamps) in batches {
            if base_offset == 4 {
                last_position = u32::try_from(log_bytes.len()).expect("segment position");
            }
            let base_timestamp = timestamps[0];
            RecordBatch {
                base_offset,
                last_offset_delta: i32::try_from(timestamps.len() - 1).expect("record count"),
                base_timestamp,
                max_timestamp: *timestamps.iter().max().expect("timestamps"),
                records: timestamps
                    .iter()
                    .enumerate()
                    .map(|(offset_delta, timestamp)| Record {
                        timestamp_delta: *timestamp - base_timestamp,
                        offset_delta: i32::try_from(offset_delta).expect("offset delta"),
                        value: Some(Bytes::from_static(b"value")),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }
            .encode(&mut log_bytes)
            .expect("encode batch");
        }
        let log_path = source_dir.path().join("segment.log");
        let offset_index_path = source_dir.path().join("segment.index");
        let time_index_path = source_dir.path().join("segment.timeindex");
        std::fs::write(&log_path, &log_bytes).expect("write log");
        std::fs::write(
            &offset_index_path,
            [
                0_u32.to_be_bytes(),
                0_u32.to_be_bytes(),
                4_u32.to_be_bytes(),
                last_position.to_be_bytes(),
            ]
            .concat(),
        )
        .expect("write offset index");
        let mut time_index = Vec::new();
        time_index.extend_from_slice(&1_100_i64.to_be_bytes());
        time_index.extend_from_slice(&0_u32.to_be_bytes());
        time_index.extend_from_slice(&2_400_i64.to_be_bytes());
        time_index.extend_from_slice(&4_u32.to_be_bytes());
        std::fs::write(&time_index_path, time_index).expect("write time index");

        let broker_arc = broker.broker_arc_for_test();
        let topic_id = broker_arc
            .controller
            .current_image()
            .topic(TOPIC)
            .expect("topic metadata")
            .topic_id;
        let topic_partition = TopicIdPartition::new(topic_id, TOPIC, 0);
        let reader = broker_arc.remote_reader.as_ref().expect("remote reader");
        let segment_id = RemoteLogSegmentId::new(topic_partition, uuid::Uuid::new_v4());
        let metadata = RemoteLogSegmentMetadata::new(
            segment_id.clone(),
            0,
            6,
            2_400,
            1,
            2_400,
            RemoteLogSegmentDetails::new(
                i32::try_from(log_bytes.len()).expect("segment size"),
                RemoteLogSegmentState::CopySegmentStarted,
                BTreeMap::from([(LeaderEpoch(0), 0)]),
            ),
        )
        .expect("segment metadata");
        reader
            .rlmm
            .add_remote_log_segment_metadata(metadata.clone())
            .expect("add segment metadata");
        reader
            .rsm
            .copy_log_segment_data(
                &metadata,
                &LogSegmentData {
                    log_segment: log_path,
                    offset_index: offset_index_path,
                    time_index: time_index_path,
                    transaction_index: None,
                    producer_snapshot_index: None,
                    leader_epoch_index: Bytes::from_static(b"0\n1\n0 0\n"),
                },
            )
            .expect("copy remote segment");
        reader
            .rlmm
            .update_remote_log_segment_metadata(RemoteLogSegmentMetadataUpdate {
                remote_log_segment_id: segment_id,
                event_timestamp_ms: 2_400,
                custom_metadata: None,
                state: RemoteLogSegmentState::CopySegmentFinished,
                broker_id: 1,
            })
            .expect("finish segment");

        assert!(
            list_one(&client, TOPIC, 1_500).await
                == ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::NONE,
                    timestamp: 1_600,
                    offset: 2,
                    leader_epoch: -1,
                    ..Default::default()
                }
        );

        drop(client);
        broker.shutdown().await;
    }

    use super::*;

    #[test]
    fn topic_describe_denied_yields_topic_authorization_failed_rows() {
        use crabka_protocol::owned::list_offsets_response::{
            self, ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
        };

        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let principal = crabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        };
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));

        let ctx = crate::handlers::RequestContext {
            principal: &principal,
            peer: &peer,
            client_id: "client-a",
            connection_id: "connection-a",
            sendfile_capable: false,
            connection_listener_name: "PLAINTEXT",
        };
        assert!(crate::handlers::acl_denied(
            &authorizer,
            &image,
            &ctx,
            ResourceType::Topic,
            "t",
            AclOperation::Describe,
        ));

        // The denied-topic shape the handler emits: every partition row
        // carries TOPIC_AUTHORIZATION_FAILED.
        let resp = ListOffsetsResponse {
            throttle_time_ms: 0,
            topics: vec![ListOffsetsTopicResponse {
                name: "t".into(),
                partitions: vec![ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    timestamp: -1,
                    offset: -1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(list_offsets_response::MAX_VERSION));
        resp.encode(&mut buf, list_offsets_response::MAX_VERSION)
            .expect("encode");
        let mut cur: &[u8] = &buf;
        let decoded =
            ListOffsetsResponse::decode(&mut cur, list_offsets_response::MAX_VERSION).unwrap();
        assert!(decoded.topics[0].partitions[0].error_code == codes::TOPIC_AUTHORIZATION_FAILED);
    }

    #[tokio::test]
    async fn denied_handler_preserves_topic_and_partition_response_fields() {
        let version = crabka_protocol::owned::list_offsets_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let ctx = test_context(&p, &peer);
        let req = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![ListOffsetsTopic {
                name: "orders".into(),
                partitions: vec![
                    ListOffsetsPartition {
                        partition_index: 0,
                        current_leader_epoch: -1,
                        timestamp: LATEST_TIMESTAMP,
                        ..Default::default()
                    },
                    ListOffsetsPartition {
                        partition_index: 2,
                        current_leader_epoch: -1,
                        timestamp: EARLIEST_TIMESTAMP,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            timeout_ms: 30_000,
            ..Default::default()
        };
        let req = encode_request(&req, version);

        let bytes = handle(&broker, version, 123, &req, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&bytes, version);

        let denied_row = |partition_index: i32| ListOffsetsPartitionResponse {
            partition_index,
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            timestamp: -1,
            offset: -1,
            leader_epoch: -1,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        let expected = ListOffsetsResponse {
            throttle_time_ms: 0,
            topics: vec![ListOffsetsTopicResponse {
                name: "orders".to_string(),
                partitions: vec![denied_row(0), denied_row(2)],
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected, "{resp:?}");
        broker_handle.shutdown().await;
    }
}
