//! `ListOffsets` (`api_key=2`). Resolves the EARLIEST / LATEST sentinels
//! using each partition's log. For tiered topics (KIP-405),
//! EARLIEST and by-timestamp lookups consult the
//! [`RemoteLogMetadataManager`](crabka_remote_storage::RemoteLogMetadataManager)
//! so offsets that have been deleted locally by local-retention but
//! still live in the remote tier are visible.
//!
//! Positive-timestamp lookups resolve against the remote tier first
//! (it holds the oldest records) and fall back to the local log's
//! time index (KIP-405/734). The `MAX_TIMESTAMP` (-3) and
//! `EARLIEST_LOCAL_TIMESTAMP` (-4) sentinels are resolved against the
//! local log.

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
/// Response placeholder (-1) meaning "no record timestamp matched/echoed".
/// Kafka's `ListOffsetsResponse.UNKNOWN_TIMESTAMP`.
const UNKNOWN_TIMESTAMP: i64 = -1;
/// Response placeholder (-1) meaning "no offset was resolved".
/// Kafka's `ListOffsetsResponse.UNKNOWN_OFFSET`.
const UNKNOWN_OFFSET: i64 = -1;

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

        let mut topics_out: Vec<ListOffsetsTopicResponse> = Vec::with_capacity(req.topics.len());
        for topic in req.topics {
            if crate::handlers::acl_denied(
                broker.config.authorizer.as_ref(),
                &acl_image,
                ctx,
                ResourceType::Topic,
                &topic.name,
                AclOperation::Describe,
            ) {
                let parts_out = topic
                    .partitions
                    .iter()
                    .map(|part| ListOffsetsPartitionResponse {
                        partition_index: part.partition_index,
                        error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                        timestamp: UNKNOWN_TIMESTAMP,
                        offset: UNKNOWN_OFFSET,
                        ..Default::default()
                    })
                    .collect();
                topics_out.push(ListOffsetsTopicResponse {
                    name: topic.name,
                    partitions: parts_out,
                    ..Default::default()
                });
                continue;
            }
            let mut parts_out: Vec<ListOffsetsPartitionResponse> =
                Vec::with_capacity(topic.partitions.len());
            for part in topic.partitions {
                parts_out.push(resolve_partition(broker, &topic.name, part).await);
            }
            topics_out.push(ListOffsetsTopicResponse {
                name: topic.name,
                partitions: parts_out,
                ..Default::default()
            });
        }

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
) -> ListOffsetsPartitionResponse {
    let index = request.partition_index;
    let mut response = ListOffsetsPartitionResponse {
        partition_index: index,
        timestamp: UNKNOWN_TIMESTAMP,
        ..Default::default()
    };
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
    let (offset, timestamp) = match request.timestamp {
        EARLIEST_TIMESTAMP => {
            let mut earliest = local_start;
            if let (Some(reader), Some(id)) = (broker.remote_reader.as_ref(), topic_id) {
                let topic_partition =
                    crabka_remote_storage::TopicIdPartition::new(id, topic_name.to_string(), index);
                match reader.earliest_offset(&topic_partition) {
                    Ok(Some(remote_start)) => earliest = earliest.min(remote_start),
                    Ok(None) => {}
                    Err(error) => tracing::warn!(topic = topic_name, partition = index,
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
        EARLIEST_LOCAL_TIMESTAMP => (local_log_start, UNKNOWN_TIMESTAMP),
        MAX_TIMESTAMP => {
            let log = partition.log.lock().expect("log mutex poisoned");
            log.max_timestamp_offset_and_ts().map_or_else(
                || (log.offset_of_max_timestamp().0, UNKNOWN_TIMESTAMP),
                |(offset, timestamp)| (offset.0, timestamp),
            )
        }
        requested_timestamp if requested_timestamp > 0 => {
            resolve_timestamp_offset(
                broker,
                &partition,
                topic_name,
                index,
                topic_id,
                requested_timestamp,
            )
            .await
        }
        _ => (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP),
    };
    response.error_code = codes::NONE;
    response.offset = offset;
    response.timestamp = timestamp;
    response
}

async fn resolve_timestamp_offset(
    broker: &Broker,
    partition: &crate::partition::Partition,
    topic_name: &str,
    partition_index: i32,
    topic_id: Option<uuid::Uuid>,
    timestamp: i64,
) -> (i64, i64) {
    if let (Some(reader), Some(id)) = (broker.remote_reader.as_ref(), topic_id) {
        let topic_partition = crabka_remote_storage::TopicIdPartition::new(
            id,
            topic_name.to_string(),
            partition_index,
        );
        match reader
            .offset_for_timestamp(&topic_partition, timestamp)
            .await
        {
            Ok(Some(offset)) => return (offset, UNKNOWN_TIMESTAMP),
            Ok(None) => {}
            Err(error) => tracing::warn!(topic = topic_name, partition = partition_index,
                error = %error, "list_offsets: remote offset_for_timestamp failed"),
        }
    }
    let log = partition.log.lock().expect("log mutex poisoned");
    log.offset_for_timestamp(timestamp)
        .map_or((UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP), |(offset, matched)| {
            (offset.0, matched)
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use bytes::BytesMut;
    use crabka_protocol::{
        Encode,
        owned::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    };

    use crate::test_support::{DenyAll, peer, principal};

    crate::test_support::wire_helpers!(
        ListOffsetsRequest,
        ListOffsetsResponse,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

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
        ];
        for (name, sentinel, want) in cases {
            assert!(sentinel == want, "{name}");
        }
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
