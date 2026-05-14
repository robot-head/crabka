//! `DeleteRecords` (`api_key=21`). Leader-only local segment trim. The
//! follower side picks up the new `log_start_offset` on the next Fetch
//! via the existing `OFFSET_OUT_OF_RANGE` recovery path — matching the
//! Apache Kafka model.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::delete_records_request::DeleteRecordsRequest;
use crabka_protocol::owned::delete_records_response::{
    DeleteRecordsPartitionResult, DeleteRecordsResponse, DeleteRecordsTopicResult,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let partitions = broker.partitions.clone();
    let node_id = broker.config.node_id;

    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = DeleteRecordsRequest::decode(&mut cur, version)?;

        let mut topic_results: Vec<DeleteRecordsTopicResult> =
            Vec::with_capacity(req.topics.len());

        for topic in req.topics {
            let mut part_results: Vec<DeleteRecordsPartitionResult> =
                Vec::with_capacity(topic.partitions.len());

            for fp in topic.partitions {
                let part_opt = partitions
                    .get(&(topic.name.clone(), fp.partition_index))
                    .map(|p| p.clone());
                let Some(part) = part_opt else {
                    part_results.push(DeleteRecordsPartitionResult {
                        partition_index: fp.partition_index,
                        low_watermark: -1,
                        error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                        ..Default::default()
                    });
                    continue;
                };

                let cur_leader =
                    part.current_leader.load(std::sync::atomic::Ordering::Acquire);
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
    })
}
