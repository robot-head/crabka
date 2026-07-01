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
}
