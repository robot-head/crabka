//! `ListOffsets` (`api_key=2`). Resolves the EARLIEST / LATEST sentinels
//! using each partition's log. For tiered topics (KIP-405),
//! EARLIEST and by-timestamp lookups consult the
//! [`RemoteLogMetadataManager`](crabka_remote_storage::RemoteLogMetadataManager)
//! so offsets that have been deleted locally by local-retention but
//! still live in the remote tier are visible.
//!
//! Positive-timestamp lookups resolve against the remote tier first
//! (it holds the oldest records) and fall back to the local log's
//! time index (KIP-405/734). The `MAX_TIMESTAMP` (-3) and `EARLIEST_LOCAL`
//! (-4) sentinels are resolved against the local log.

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::list_offsets_request::ListOffsetsRequest;
use crabka_protocol::owned::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
};
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

const EARLIEST: i64 = -2;
const LATEST: i64 = -1;
const MAX_TIMESTAMP: i64 = -3; // KIP-734
const EARLIEST_LOCAL: i64 = -4; // KIP-405

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
                        timestamp: -1,
                        offset: -1,
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
                    timestamp: -1,
                    ..Default::default()
                };

                let Some(p) = partitions.get(&topic.name, idx) else {
                    out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
                    parts_out.push(out);
                    continue;
                };

                let (local_start, local_end, local_log_start, remote_storage_enable) = {
                    let log = p.log.lock().expect("log mutex poisoned");
                    (
                        log.log_start_offset(),
                        log.log_end_offset(),
                        log.local_log_start_offset(),
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
                    EARLIEST => {
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
                        (earliest, -1)
                    }
                    LATEST => (local_end, -1),
                    EARLIEST_LOCAL => (local_log_start, -1),
                    MAX_TIMESTAMP => {
                        let log = p.log.lock().expect("log mutex poisoned");
                        match log.max_timestamp_offset_and_ts() {
                            Some((offset, ts)) => (offset, ts),
                            None => (log.offset_of_max_timestamp(), -1),
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
                            (o, -1)
                        } else {
                            let local = {
                                let log = p.log.lock().expect("log mutex poisoned");
                                log.offset_for_timestamp(ts)
                            };
                            local.map_or((-1, -1), |(o, matched_ts)| (o, matched_ts))
                        }
                    }
                    _ => (-1, -1),
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
    use super::*;
    use assert2::assert;
    use crabka_protocol::owned::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
    use crabka_security::{AuthMethod, Principal};
    use std::net::SocketAddr;
    use std::sync::Arc;

    use crate::authorizer::Authorizer;
    use crate::broker::{Broker, BrokerHandle};
    use crate::config::BrokerConfig;

    #[derive(Debug)]
    struct DenyAll;

    impl Authorizer for DenyAll {
        fn authorize(
            &self,
            _source: &dyn crabka_authz::AclSource,
            _req: &AuthorizationRequest<'_>,
        ) -> AuthorizationResult {
            AuthorizationResult::Deny
        }
    }

    fn encode_request(req: &ListOffsetsRequest, version: i16) -> Bytes {
        let mut buf = BytesMut::with_capacity(req.encoded_len(version));
        req.encode(&mut buf, version).expect("encode request");
        buf.freeze()
    }

    fn decode_response(bytes: &Bytes, version: i16) -> ListOffsetsResponse {
        let mut cur: &[u8] = bytes.as_ref();
        let resp = ListOffsetsResponse::decode(&mut cur, version).expect("decode response");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    fn principal(name: &str) -> Principal {
        Principal {
            name: name.into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        }
    }

    fn peer() -> SocketAddr {
        "127.0.0.1:9092".parse().unwrap()
    }

    fn test_context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::handlers::RequestContext {
            principal,
            peer,
            client_id: "admin-client",
            sendfile_capable: false,
            connection_listener_name: "PLAINTEXT",
        }
    }

    async fn start_broker(authorizer: Arc<dyn Authorizer>) -> (BrokerHandle, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
        cfg.audit_enabled = false;
        cfg.authorizer = authorizer;
        let handle = Broker::start(cfg).await.expect("start broker");
        (handle, dir)
    }

    #[test]
    fn sentinel_constants_match_kafka_wire_values() {
        let cases = [
            ("EARLIEST", EARLIEST, -2),
            ("LATEST", LATEST, -1),
            ("MAX_TIMESTAMP", MAX_TIMESTAMP, -3),
            ("EARLIEST_LOCAL", EARLIEST_LOCAL, -4),
        ];
        for (name, sentinel, want) in cases {
            assert!(sentinel == want, "{name}");
        }
    }

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
                        timestamp: LATEST,
                        ..Default::default()
                    },
                    ListOffsetsPartition {
                        partition_index: 2,
                        current_leader_epoch: -1,
                        timestamp: EARLIEST,
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
