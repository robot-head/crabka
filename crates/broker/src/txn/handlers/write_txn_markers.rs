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
use crabka_ids::PartitionIndex;
use crabka_protocol::{
    Decode, Encode,
    owned::{
        write_txn_markers_request::WriteTxnMarkersRequest,
        write_txn_markers_response::{
            WritableTxnMarkerPartitionResult, WritableTxnMarkerResult,
            WritableTxnMarkerTopicResult, WriteTxnMarkersResponse,
        },
    },
};
use futures_util::future::BoxFuture;

use crate::{
    broker::Broker,
    codes,
    error::BrokerError,
    txn::marker::{MarkerType, build_marker_batch},
};

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
            // Wrap the wire `i64` into `ProducerId` for the marker builder;
            // unwrapped again below for the raw-`i64` response field.
            let pid = crabka_log::ProducerId(marker_entry.producer_id);
            let epoch = marker_entry.producer_epoch;

            let mut topic_results: Vec<WritableTxnMarkerTopicResult> = Vec::new();

            for topic in &marker_entry.topics {
                let mut partition_results: Vec<WritableTxnMarkerPartitionResult> = Vec::new();

                for &p in &topic.partition_indexes {
                    let error_code = match partitions.get(&topic.name, PartitionIndex(p)) {
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
                producer_id: pid.get(),
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

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use assert2::assert;
    use crabka_log::{Log, LogConfig};
    use crabka_protocol::{
        UnknownTaggedFields,
        owned::{
            write_txn_markers_request::{
                WritableTxnMarker, WritableTxnMarkerTopic, WriteTxnMarkersRequest,
            },
            write_txn_markers_response::WriteTxnMarkersResponse,
        },
    };

    use super::*;
    use crate::broker::{Broker, BrokerHandle};

    const VERSION: i16 = 2;

    fn open_partition(broker: &Broker, log_dir: &Path, topic: &str, partition: i32) {
        let part_dir = crate::log_dir::partition_dir(log_dir, topic, partition);
        std::fs::create_dir_all(&part_dir).expect("create partition dir");
        let log = Log::open(&part_dir, LogConfig::default()).expect("open partition log");
        let part = crate::broker::spawn_partition(
            topic.to_string(),
            PartitionIndex(partition),
            log_dir.to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
        );
        broker
            .partitions
            .insert(topic.to_string(), PartitionIndex(partition), part);
    }

    async fn start_broker() -> (BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|cfg| {
            cfg.audit_enabled = false;
        })
        .await
    }

    crate::test_support::codec_helpers!(
        WriteTxnMarkersRequest,
        WriteTxnMarkersResponse,
        version = VERSION
    );

    #[tokio::test]
    async fn handle_returns_marker_topic_and_partition_result_rows() {
        let (broker_handle, dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        open_partition(&broker, dir.path(), "orders", 1);
        let req = WriteTxnMarkersRequest {
            markers: vec![WritableTxnMarker {
                producer_id: 91,
                producer_epoch: 4,
                transaction_result: true,
                transaction_version: 1,
                topics: vec![WritableTxnMarkerTopic {
                    name: "orders".into(),
                    partition_indexes: vec![1, 2],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let req_bytes = encode_request(&req);

        let bytes = super::handle(&broker, VERSION, 123, &req_bytes)
            .await
            .expect("handle");
        let resp = decode_response(&bytes);

        let expected = WriteTxnMarkersResponse {
            markers: vec![WritableTxnMarkerResult {
                producer_id: 91,
                topics: vec![WritableTxnMarkerTopicResult {
                    name: "orders".into(),
                    partitions: vec![
                        WritableTxnMarkerPartitionResult {
                            partition_index: 1,
                            error_code: codes::NONE,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        },
                        WritableTxnMarkerPartitionResult {
                            partition_index: 2,
                            error_code: codes::NOT_LEADER_OR_FOLLOWER,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        },
                    ],
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
