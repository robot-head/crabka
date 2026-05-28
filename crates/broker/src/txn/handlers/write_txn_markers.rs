//! `WriteTxnMarkers` (`api_key=27`). Receives a fan-out from the transaction
//! coordinator (`EndTxn`) and appends control-marker batches to each
//! locally-led partition named in the request.
//!
//! ## Flow
//!
//! For each marker entry in the request:
//! 1. Determine commit vs abort from `transaction_result`.
//! 2. For each (topic, partition) named in the marker:
//!    - If the partition is locally led (found in `broker.partitions`):
//!      build a marker batch and call `Partition::produce_batch`.
//!    - If not local: return `NOT_LEADER_OR_FOLLOWER` per-partition.
//! 3. Return a nested per-marker → per-topic → per-partition response.
//!
//! Wire format: v1 flexible (tagged fields), v2 flexible + `transaction_version`.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::Decode;
use crabka_protocol::Encode;
use crabka_protocol::owned::write_txn_markers_request::WriteTxnMarkersRequest;
use crabka_protocol::owned::write_txn_markers_response::{
    WritableTxnMarkerPartitionResult, WritableTxnMarkerResult, WritableTxnMarkerTopicResult,
    WriteTxnMarkersResponse,
};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::txn::marker::{MarkerType, build_marker_batch};

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let partitions = broker.partitions.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = WriteTxnMarkersRequest::decode(&mut cur, version)?;

        let mut marker_results: Vec<WritableTxnMarkerResult> = Vec::new();

        for marker_entry in &req.markers {
            let marker_type = if marker_entry.transaction_result {
                MarkerType::Commit
            } else {
                MarkerType::Abort
            };
            let pid = marker_entry.producer_id;
            let epoch = marker_entry.producer_epoch;

            let mut topic_results: Vec<WritableTxnMarkerTopicResult> = Vec::new();

            for topic in &marker_entry.topics {
                let mut partition_results: Vec<WritableTxnMarkerPartitionResult> = Vec::new();

                for &p in &topic.partition_indexes {
                    let error_code = match partitions
                        .get(&(topic.name.clone(), p))
                        .map(|e| e.value().clone())
                    {
                        None => {
                            tracing::debug!(
                                topic = %topic.name,
                                partition = p,
                                "WriteTxnMarkers: partition not local; returning NOT_LEADER_OR_FOLLOWER"
                            );
                            codes::NOT_LEADER_OR_FOLLOWER
                        }
                        Some(part) => {
                            let base_offset = part.log_end_offset();
                            let marker = build_marker_batch(pid, epoch, base_offset, marker_type);
                            match part.produce_batch(marker).await {
                                Ok(_) => codes::NONE,
                                Err(e) => {
                                    tracing::warn!(
                                        topic = %topic.name,
                                        partition = p,
                                        error = %e,
                                        "WriteTxnMarkers: produce_batch failed"
                                    );
                                    codes::UNKNOWN_SERVER_ERROR
                                }
                            }
                        }
                    };

                    partition_results.push(WritableTxnMarkerPartitionResult {
                        partition_index: p,
                        error_code,
                        ..Default::default()
                    });
                }

                topic_results.push(WritableTxnMarkerTopicResult {
                    name: topic.name.clone(),
                    partitions: partition_results,
                    ..Default::default()
                });
            }

            marker_results.push(WritableTxnMarkerResult {
                producer_id: pid,
                topics: topic_results,
                ..Default::default()
            });
        }

        let resp = WriteTxnMarkersResponse {
            markers: marker_results,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
