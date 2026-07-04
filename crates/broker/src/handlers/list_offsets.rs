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

use bytes::{Bytes, BytesMut};
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        list_offsets_request::ListOffsetsRequest,
        list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
        },
    },
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
};

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

#[allow(clippy::too_many_lines)]
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
    let partitions = broker.partitions.clone();
    let controller = broker.controller.clone();
    let remote_reader = broker.remote_reader.clone();
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
            if topic_describe_denied(
                broker.config.authorizer.as_ref(),
                &acl_image,
                ctx.principal,
                ctx.peer,
                &topic.name,
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
                let idx = part.partition_index;
                let mut out = ListOffsetsPartitionResponse {
                    partition_index: idx,
                    timestamp: UNKNOWN_TIMESTAMP,
                    ..Default::default()
                };

                let Some(p) = partitions.get(&topic.name, crabka_ids::PartitionIndex(idx)) else {
                    out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
                    parts_out.push(out);
                    continue;
                };

                let (local_start, local_end, local_log_start, remote_storage_enable) = {
                    let log = p.log.lock().expect("log mutex poisoned");
                    (
                        // Unwrap the log-layer `Offset`s into broker's `i64` world at the seam.
                        log.log_start_offset().0,
                        log.log_end_offset().0,
                        log.local_log_start_offset().0,
                        log.config_snapshot().remote_storage_enable,
                    )
                };

                let tiered = remote_storage_enable && remote_reader.is_some();
                let topic_id = if tiered {
                    controller
                        .current_image()
                        .topic(&topic.name)
                        .map(|t| t.topic_id)
                } else {
                    None
                };

                let (offset, resp_timestamp) = match part.timestamp {
                    EARLIEST_TIMESTAMP => {
                        let mut earliest = local_start;
                        if let (Some(reader), Some(tid)) = (remote_reader.as_ref(), topic_id) {
                            let tp = crabka_remote_storage::TopicIdPartition::new(
                                tid,
                                topic.name.clone(),
                                idx,
                            );
                            match reader.earliest_offset(&tp) {
                                Ok(Some(remote_start)) => earliest = earliest.min(remote_start),
                                Ok(None) => {}
                                // Includes RemoteStorageError::NotReady (metadata
                                // partition catching up): warn + keep the local
                                // earliest as the conservative answer.
                                Err(e) => tracing::warn!(
                                    topic = %topic.name, partition = idx, error = %e,
                                    "list_offsets: remote earliest_offset failed"
                                ),
                            }
                        }
                        (earliest, UNKNOWN_TIMESTAMP)
                    }
                    LATEST_TIMESTAMP => (local_end, UNKNOWN_TIMESTAMP),
                    EARLIEST_LOCAL_TIMESTAMP => (local_log_start, UNKNOWN_TIMESTAMP),
                    MAX_TIMESTAMP => {
                        let log = p.log.lock().expect("log mutex poisoned");
                        // Unwrap the log-layer `Offset`s into broker's `i64` world at the seam.
                        match log.max_timestamp_offset_and_ts() {
                            Some((offset, ts)) => (offset.0, ts),
                            None => (log.offset_of_max_timestamp().0, UNKNOWN_TIMESTAMP),
                        }
                    }
                    ts if ts > 0 => {
                        let remote_result =
                            if let (Some(reader), Some(tid)) = (remote_reader.as_ref(), topic_id) {
                                let tp = crabka_remote_storage::TopicIdPartition::new(
                                    tid,
                                    topic.name.clone(),
                                    idx,
                                );
                                match reader.offset_for_timestamp(&tp, ts).await {
                                    Ok(Some(o)) => Some(o),
                                    Ok(None) => None,
                                    // Includes RemoteStorageError::NotReady
                                    // (metadata partition catching up): warn +
                                    // fall back to the local answer below.
                                    Err(e) => {
                                        tracing::warn!(
                                            topic = %topic.name, partition = idx, error = %e,
                                            "list_offsets: remote offset_for_timestamp failed"
                                        );
                                        None
                                    }
                                }
                            } else {
                                None
                            };
                        if let Some(o) = remote_result {
                            // Remote hit covers the oldest records; the remote reader
                            // does not surface the matched record timestamp, so echo -1.
                            (o, UNKNOWN_TIMESTAMP)
                        } else {
                            let local = {
                                let log = p.log.lock().expect("log mutex poisoned");
                                log.offset_for_timestamp(ts)
                            };
                            // Unwrap the log-layer `Offset` into broker's `i64` world at the seam.
                            local.map_or((UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP), |(o, matched_ts)| {
                                (o.0, matched_ts)
                            })
                        }
                    }
                    _ => (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP),
                };

                out.error_code = codes::NONE;
                out.offset = offset;
                out.timestamp = resp_timestamp;
                parts_out.push(out);
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
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    }
}

/// `Describe` on `Topic(name)` gate. Returns `true` when denied.
fn topic_describe_denied(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &crabka_metadata::MetadataImage,
    principal: &crabka_security::Principal,
    host: &std::net::SocketAddr,
    topic: &str,
) -> bool {
    authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal,
            host,
            resource_type: ResourceType::Topic,
            resource_name: topic,
            operation: AclOperation::Describe,
        },
    ) == AuthorizationResult::Deny
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_protocol::owned::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};

    use crate::test_support::{DenyAll, peer, principal};

    crate::test_support::wire_helpers!(
        ListOffsetsRequest,
        ListOffsetsResponse,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

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

        assert!(topic_describe_denied(
            &authorizer,
            &image,
            &principal,
            &peer,
            "t"
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
