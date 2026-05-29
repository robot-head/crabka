//! `ListOffsets` (`api_key=2`). Resolves the EARLIEST / LATEST sentinels
//! using each partition's log. For tiered topics (KIP-405),
//! EARLIEST and by-timestamp lookups consult the
//! [`RemoteLogMetadataManager`](crabka_remote_storage::RemoteLogMetadataManager)
//! so offsets that have been deleted locally by local-retention but
//! still live in the remote tier are visible.
//!
//! Local-segment timestamp-index lookup (positive timestamps on non-tiered
//! topics) is still out of scope and returns -1 as before.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::list_offsets_request::ListOffsetsRequest;
use crabka_protocol::owned::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

const EARLIEST: i64 = -2;
const LATEST: i64 = -1;

#[allow(clippy::too_many_lines)]
pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let partitions = broker.partitions.clone();
    let controller = broker.controller.clone();
    let remote_reader = broker.remote_reader.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = ListOffsetsRequest::decode(&mut cur, version)?;

        let mut topics_out: Vec<ListOffsetsTopicResponse> = Vec::with_capacity(req.topics.len());
        for topic in req.topics {
            let mut parts_out: Vec<ListOffsetsPartitionResponse> =
                Vec::with_capacity(topic.partitions.len());
            for part in topic.partitions {
                let idx = part.partition_index;
                let mut out = ListOffsetsPartitionResponse {
                    partition_index: idx,
                    timestamp: -1,
                    ..Default::default()
                };

                let Some(p) = partitions.get(&(topic.name.clone(), idx)) else {
                    out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
                    parts_out.push(out);
                    continue;
                };

                let (local_start, local_end, remote_storage_enable) = {
                    let log = p.log.lock().expect("log mutex poisoned");
                    (
                        log.log_start_offset(),
                        log.log_end_offset(),
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

                let offset = match part.timestamp {
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
                                Err(e) => tracing::warn!(
                                    topic = %topic.name, partition = idx, error = %e,
                                    "list_offsets: remote earliest_offset failed"
                                ),
                            }
                        }
                        earliest
                    }
                    LATEST => local_end,
                    ts if ts > 0 => {
                        if let (Some(reader), Some(tid)) = (remote_reader.as_ref(), topic_id) {
                            let tp = crabka_remote_storage::TopicIdPartition::new(
                                tid,
                                topic.name.clone(),
                                idx,
                            );
                            match reader.offset_for_timestamp(&tp, ts).await {
                                Ok(Some(o)) => o,
                                Ok(None) => -1,
                                Err(e) => {
                                    tracing::warn!(
                                        topic = %topic.name, partition = idx, error = %e,
                                        "list_offsets: remote offset_for_timestamp failed"
                                    );
                                    -1
                                }
                            }
                        } else {
                            -1
                        }
                    }
                    _ => -1,
                };

                out.error_code = codes::NONE;
                out.offset = offset;
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
    })
}
