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
    let denied_topics: std::collections::HashSet<String> = acl_results
        .iter()
        .filter_map(|(name, r)| {
            if *r == AuthorizationResult::Deny {
                Some((*name).to_string())
            } else {
                None
            }
        })
        .collect();

    let mut topic_results: Vec<DeleteRecordsTopicResult> = Vec::with_capacity(req.topics.len());

    for topic in req.topics {
        // Per-topic ACL check: if denied, mark every partition in the topic.
        if denied_topics.contains(&topic.name) {
            let part_results: Vec<DeleteRecordsPartitionResult> = topic
                .partitions
                .iter()
                .map(|fp| DeleteRecordsPartitionResult {
                    partition_index: fp.partition_index,
                    low_watermark: -1,
                    error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    ..Default::default()
                })
                .collect();
            topic_results.push(DeleteRecordsTopicResult {
                name: topic.name,
                partitions: part_results,
                ..Default::default()
            });
            continue;
        }

        let mut part_results: Vec<DeleteRecordsPartitionResult> =
            Vec::with_capacity(topic.partitions.len());

        for fp in topic.partitions {
            let part_opt = partitions.get(&topic.name, fp.partition_index);
            let Some(part) = part_opt else {
                part_results.push(DeleteRecordsPartitionResult {
                    partition_index: fp.partition_index,
                    low_watermark: -1,
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    ..Default::default()
                });
                continue;
            };

            let cur_leader = part
                .current_leader
                .load(std::sync::atomic::Ordering::Acquire);
            if cur_leader != node_id {
                part_results.push(DeleteRecordsPartitionResult {
                    partition_index: fp.partition_index,
                    low_watermark: -1,
                    error_code: codes::NOT_LEADER_OR_FOLLOWER,
                    ..Default::default()
                });
                continue;
            }

            // Translate offset == -1 → high_watermark per Kafka semantics.
            let leo = part.log_end_offset();
            let hw = part.high_watermark().await;
            let target = if fp.offset == -1 { hw } else { fp.offset };

            if target < 0 || target > leo {
                part_results.push(DeleteRecordsPartitionResult {
                    partition_index: fp.partition_index,
                    low_watermark: -1,
                    error_code: codes::OFFSET_OUT_OF_RANGE,
                    ..Default::default()
                });
                continue;
            }

            match part.trim_to_offset(target).await {
                Ok(new_start) => {
                    part_results.push(DeleteRecordsPartitionResult {
                        partition_index: fp.partition_index,
                        low_watermark: new_start,
                        error_code: codes::NONE,
                        ..Default::default()
                    });
                }
                Err(e) => {
                    tracing::warn!(
                        topic = %topic.name, partition = fp.partition_index, error = %e,
                        "DeleteRecords: trim_to_offset failed"
                    );
                    part_results.push(DeleteRecordsPartitionResult {
                        partition_index: fp.partition_index,
                        low_watermark: -1,
                        error_code: codes::UNKNOWN_SERVER_ERROR,
                        ..Default::default()
                    });
                }
            }
        }

        topic_results.push(DeleteRecordsTopicResult {
            name: topic.name,
            partitions: part_results,
            ..Default::default()
        });
    }

    let resp = DeleteRecordsResponse {
        topics: topic_results,
        throttle_time_ms: 0,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
