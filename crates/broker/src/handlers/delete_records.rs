//! `DeleteRecords` (`api_key=21`). Leader-only local segment trim. The
//! follower side picks up the new `log_start_offset` on the next Fetch
//! via the existing `OFFSET_OUT_OF_RANGE` recovery path — matching the
//! Apache Kafka model.

use bytes::{Bytes, BytesMut};

use crabka_metadata::AclOperation;
use crabka_protocol::owned::delete_records_request::DeleteRecordsRequest;
use crabka_protocol::owned::delete_records_response::{
    DeleteRecordsPartitionResult, DeleteRecordsResponse, DeleteRecordsTopicResult,
};
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationResult, authorize_topics};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

fn denied_topic_names(
    acl_results: &std::collections::HashMap<&str, AuthorizationResult>,
) -> std::collections::HashSet<String> {
    acl_results
        .iter()
        .filter_map(|(name, r)| {
            if *r == AuthorizationResult::Deny {
                Some((*name).to_string())
            } else {
                None
            }
        })
        .collect()
}

fn partition_result(
    partition_index: i32,
    low_watermark: i64,
    error_code: i16,
) -> DeleteRecordsPartitionResult {
    DeleteRecordsPartitionResult {
        partition_index,
        low_watermark,
        error_code,
        ..Default::default()
    }
}

fn error_partition_result(partition_index: i32, error_code: i16) -> DeleteRecordsPartitionResult {
    partition_result(partition_index, -1, error_code)
}

fn topic_result(
    name: String,
    partitions: Vec<DeleteRecordsPartitionResult>,
) -> DeleteRecordsTopicResult {
    DeleteRecordsTopicResult {
        name,
        partitions,
        ..Default::default()
    }
}

fn delete_records_response(topics: Vec<DeleteRecordsTopicResult>) -> DeleteRecordsResponse {
    DeleteRecordsResponse {
        topics,
        ..Default::default()
    }
}

fn target_offset(requested_offset: i64, high_watermark: i64) -> i64 {
    if requested_offset == -1 {
        high_watermark
    } else {
        requested_offset
    }
}

fn offset_out_of_range(target: i64, log_end_offset: i64) -> bool {
    target < 0 || target > log_end_offset
}

#[allow(clippy::too_many_lines)]
#[tracing::instrument(
    name = "handle_delete_records",
    level = "info",
    skip_all,
    fields(api = "DeleteRecords", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = DeleteRecordsRequest::decode(&mut cur, version)?;

    let partitions = broker.partitions.clone();
    let node_id = broker.config.node_id;

    let image = broker.controller.current_image();

    // ── ACL preamble ────────────────────────────────────────
    // Batch-authorize every topic name for `Delete`. Topics that come
    // back `Deny` short-circuit the trim loop and emit
    // TOPIC_AUTHORIZATION_FAILED on every partition row for that topic.
    let topic_names: Vec<&str> = req.topics.iter().map(|t| t.name.as_str()).collect();
    let acl_results = authorize_topics(
        broker.config.authorizer.as_ref(),
        &*image,
        ctx.principal,
        ctx.peer,
        AclOperation::Delete,
        topic_names.iter().copied(),
    );
    let denied_topics = denied_topic_names(&acl_results);

    let mut topic_results: Vec<DeleteRecordsTopicResult> = Vec::with_capacity(req.topics.len());

    for topic in req.topics {
        // Per-topic ACL check: if denied, mark every partition in the topic.
        if denied_topics.contains(&topic.name) {
            let part_results: Vec<DeleteRecordsPartitionResult> = topic
                .partitions
                .iter()
                .map(|fp| {
                    error_partition_result(fp.partition_index, codes::TOPIC_AUTHORIZATION_FAILED)
                })
                .collect();
            topic_results.push(topic_result(topic.name, part_results));
            continue;
        }

        let mut part_results: Vec<DeleteRecordsPartitionResult> =
            Vec::with_capacity(topic.partitions.len());

        for fp in topic.partitions {
            let part_opt = partitions.get(&topic.name, fp.partition_index);
            let Some(part) = part_opt else {
                part_results.push(error_partition_result(
                    fp.partition_index,
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                ));
                continue;
            };

            let cur_leader = part
                .current_leader
                .load(std::sync::atomic::Ordering::Acquire);
            if cur_leader != node_id {
                part_results.push(error_partition_result(
                    fp.partition_index,
                    codes::NOT_LEADER_OR_FOLLOWER,
                ));
                continue;
            }

            // Translate offset == -1 → high_watermark per Kafka semantics.
            let leo = part.log_end_offset();
            let hw = part.high_watermark().await;
            let target = target_offset(fp.offset, hw);

            if offset_out_of_range(target, leo) {
                part_results.push(error_partition_result(
                    fp.partition_index,
                    codes::OFFSET_OUT_OF_RANGE,
                ));
                continue;
            }

            match part.trim_to_offset(target).await {
                Ok(new_start) => {
                    part_results.push(partition_result(fp.partition_index, new_start, codes::NONE));
                }
                Err(e) => {
                    tracing::warn!(
                        topic = %topic.name, partition = fp.partition_index, error = %e,
                        "DeleteRecords: trim_to_offset failed"
                    );
                    part_results.push(error_partition_result(
                        fp.partition_index,
                        codes::UNKNOWN_SERVER_ERROR,
                    ));
                }
            }
        }

        topic_results.push(topic_result(topic.name, part_results));
    }

    let resp = delete_records_response(topic_results);
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use assert2::check;
    use crabka_protocol::owned::delete_records_request::{
        DeleteRecordsPartition, DeleteRecordsTopic,
    };
    use crabka_security::Principal;
    use std::net::SocketAddr;
    use std::sync::Arc;

    use crate::authorizer::Authorizer;
    use crate::broker::{Broker, BrokerHandle};
    use crate::test_support::{DenyAll, peer, principal};

    const VERSION: i16 = 2;

    fn request(topic: &str, partitions: &[(i32, i64)]) -> DeleteRecordsRequest {
        DeleteRecordsRequest {
            topics: vec![DeleteRecordsTopic {
                name: topic.into(),
                partitions: partitions
                    .iter()
                    .map(|(partition_index, offset)| DeleteRecordsPartition {
                        partition_index: *partition_index,
                        offset: *offset,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        }
    }

    fn encode_request(req: &DeleteRecordsRequest) -> Bytes {
        crate::test_support::encode_request(req, VERSION)
    }

    fn decode_response(bytes: &Bytes) -> DeleteRecordsResponse {
        crate::test_support::decode_response(bytes, VERSION)
    }

    fn test_context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::test_support::request_context(principal, peer, "admin-client")
    }

    async fn start_broker(authorizer: Arc<dyn Authorizer>) -> (BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|cfg| {
            cfg.audit_enabled = false;
            cfg.authorizer = authorizer;
        })
        .await
    }

    async fn drive(
        broker: &Broker,
        req: &DeleteRecordsRequest,
        principal: &Principal,
        peer: &SocketAddr,
    ) -> DeleteRecordsResponse {
        let ctx = test_context(principal, peer);
        let req_bytes = encode_request(req);
        let bytes = handle(broker, VERSION, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        decode_response(&bytes)
    }

    #[test]
    fn denied_topic_names_keeps_only_denied_decisions() {
        let acl_results = std::collections::HashMap::from([
            ("denied", AuthorizationResult::Deny),
            ("allowed", AuthorizationResult::Allow),
        ]);

        let denied = denied_topic_names(&acl_results);

        let expected = std::collections::HashSet::from(["denied".to_string()]);
        assert!(denied == expected);
    }

    #[test]
    fn offset_helpers_cover_delete_records_boundaries() {
        check!(target_offset(-1, 42) == 42);
        check!(target_offset(-2, 42) == -2);
        check!(target_offset(7, 42) == 7);

        check!(!offset_out_of_range(0, 10));
        check!(!offset_out_of_range(10, 10));
        check!(offset_out_of_range(-1, 10));
        check!(offset_out_of_range(11, 10));
    }

    #[test]
    fn response_helpers_preserve_topic_and_partition_fields() {
        let denied = error_partition_result(7, codes::TOPIC_AUTHORIZATION_FAILED);
        let expected_denied = DeleteRecordsPartitionResult {
            partition_index: 7,
            low_watermark: -1,
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(denied == expected_denied);

        let ok = partition_result(3, 44, codes::NONE);
        let expected_ok = DeleteRecordsPartitionResult {
            partition_index: 3,
            low_watermark: 44,
            error_code: codes::NONE,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(ok == expected_ok);

        let topic = topic_result("orders".into(), vec![denied]);
        let expected_topic = DeleteRecordsTopicResult {
            name: "orders".into(),
            partitions: vec![expected_denied],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(topic == expected_topic);

        let resp = delete_records_response(vec![topic]);
        let expected_resp = DeleteRecordsResponse {
            throttle_time_ms: 0,
            topics: vec![expected_topic],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(resp == expected_resp);
    }

    #[tokio::test]
    async fn handle_denied_topic_returns_topic_auth_rows() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let req = request("secret", &[(0, 3), (2, -1)]);

        let resp = drive(&broker, &req, &p, &peer).await;

        let expected = DeleteRecordsResponse {
            throttle_time_ms: 0,
            topics: vec![DeleteRecordsTopicResult {
                name: "secret".into(),
                partitions: vec![
                    DeleteRecordsPartitionResult {
                        partition_index: 0,
                        low_watermark: -1,
                        error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                        unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
                    },
                    DeleteRecordsPartitionResult {
                        partition_index: 2,
                        low_watermark: -1,
                        error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                        unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
                    },
                ],
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_unknown_partition_preserves_requested_index() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let req = request("missing", &[(4, 0)]);

        let resp = drive(&broker, &req, &p, &peer).await;

        let expected = DeleteRecordsResponse {
            throttle_time_ms: 0,
            topics: vec![DeleteRecordsTopicResult {
                name: "missing".into(),
                partitions: vec![DeleteRecordsPartitionResult {
                    partition_index: 4,
                    low_watermark: -1,
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
