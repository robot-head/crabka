//! `WriteTxnMarkers` (`api_key=27`). Receives a fan-out from the transaction
//! coordinator (`EndTxn`) and appends control-marker batches to each
//! locally-led partition named in the request.
//!
//! ## Flow
//!
//! For each marker entry in the request:
//! 1. Determine commit or abort from `transaction_result`.
//! 2. For each (topic, partition) named in the marker:
//!    - If the partition is locally led, that is, if it is in
//!      `broker.partitions`, build a marker batch and call
//!      `Partition::produce_batch`.
//!    - If it is not local, return per-partition `NOT_LEADER_OR_FOLLOWER`.
//! 3. Return a nested per-marker → per-topic → per-partition response.
//!
//! Wire format: v1 flexible with tagged fields, and v2 flexible with
//! `transaction_version`.

use std::{collections::HashMap, sync::Arc};

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
    coordinator::{
        bootstrap::OFFSETS_TOPIC,
        persistence::{Key, OffsetCommitValue, parse_key},
        unified::{
            GroupCoordinator,
            actor::{GroupActorMessage, GroupKindTag},
            classic_state::OffsetEntry,
        },
    },
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
    let group_coordinator = broker.group_coordinator.clone();
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
                            match append_marker_and_materialize(
                                &part,
                                Some(&group_coordinator),
                                &topic.name,
                                MarkerAppend {
                                    producer_id: pid,
                                    producer_epoch: epoch,
                                    marker_type,
                                    coordinator_epoch: marker_entry.coordinator_epoch,
                                    commit_stamp: None,
                                },
                            )
                            .await
                            {
                                Ok(()) => codes::NONE,
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

type CommittedOffsets = HashMap<String, Vec<((String, i32), OffsetEntry)>>;

/// Append a transaction marker and, for a committed `__consumer_offsets`
/// transaction, publish the now-visible offsets to the local group actors.
///
/// Both the inter-broker `WriteTxnMarkers` handler and `EndTxn`'s direct local
/// path must use this function. Keeping marker append and actor publication in
/// one path prevents local transactions from becoming durable in the log but
/// remaining invisible until the next coordinator replay.
pub(crate) async fn append_marker_and_materialize(
    partition: &crate::partition::Partition,
    group_coordinator: Option<&Arc<GroupCoordinator>>,
    topic: &str,
    marker: MarkerAppend,
) -> Result<(), BrokerError> {
    let MarkerAppend {
        producer_id,
        producer_epoch,
        marker_type,
        coordinator_epoch,
        commit_stamp,
    } = marker;
    let committed_offsets = if marker_type == MarkerType::Commit && topic == OFFSETS_TOPIC {
        let coordinator = group_coordinator.ok_or_else(|| {
            BrokerError::Txn(
                "cannot commit transactional offsets without a group coordinator".into(),
            )
        })?;
        (
            Some(coordinator),
            pending_offset_entries(partition, producer_id)?,
        )
    } else {
        (None, HashMap::new())
    };

    let marker = build_marker_batch(
        producer_id,
        producer_epoch,
        partition.log_end_offset(),
        marker_type,
        coordinator_epoch,
    );
    if let Some(stamp) = commit_stamp {
        if marker_type != MarkerType::Commit {
            return Err(BrokerError::Txn(
                "a transaction commit stamp cannot be attached to an abort marker".into(),
            ));
        }
        partition.produce_commit_marker(marker, stamp).await?;
    } else {
        partition.produce_batch(marker).await?;
    }

    if let (Some(coordinator), offsets) = committed_offsets {
        apply_committed_offsets(coordinator, offsets).await?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MarkerAppend {
    pub(crate) producer_id: crabka_log::ProducerId,
    pub(crate) producer_epoch: i16,
    pub(crate) marker_type: MarkerType,
    pub(crate) coordinator_epoch: i32,
    pub(crate) commit_stamp: Option<u64>,
}

fn pending_offset_entries(
    partition: &crate::partition::Partition,
    producer_id: crabka_log::ProducerId,
) -> Result<CommittedOffsets, BrokerError> {
    let log = partition.log.lock().map_err(|_| {
        BrokerError::Replication("offsets log lock poisoned while applying txn marker".into())
    })?;
    let Some(mut next) = log.pending_transaction_start(producer_id) else {
        return Ok(HashMap::new());
    };
    let end = log.log_end_offset();
    let mut offsets: CommittedOffsets = HashMap::new();
    while next < end {
        let read = log.read(next, crabka_units::mebibytes(1))?;
        if read.batches.is_empty() {
            break;
        }
        let mut advanced_to = next;
        for batch in &read.batches {
            if batch.producer_id == producer_id.get()
                && batch.attributes.is_transactional()
                && !batch.attributes.is_control_batch()
            {
                for record in &batch.records {
                    let (Some(key), Some(value)) = (&record.key, &record.value) else {
                        continue;
                    };
                    if let Key::OffsetCommit {
                        group_id,
                        topic,
                        partition,
                    } = parse_key(key)?
                    {
                        let value = OffsetCommitValue::decode_value(value)?;
                        offsets.entry(group_id).or_default().push((
                            (topic, partition),
                            OffsetEntry {
                                offset: value.offset,
                                leader_epoch: value.leader_epoch,
                                metadata: value.metadata,
                                commit_timestamp_ms: value.commit_timestamp_ms,
                            },
                        ));
                    }
                }
            }
            advanced_to =
                crabka_log::Offset(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
        }
        if advanced_to <= next {
            break;
        }
        next = advanced_to;
    }
    Ok(offsets)
}

async fn apply_committed_offsets(
    coordinator: &Arc<GroupCoordinator>,
    offsets: CommittedOffsets,
) -> Result<(), BrokerError> {
    for (group_id, entries) in offsets {
        // The factory detects and replaces a closed actor. This makes the
        // in-memory publication robust after an actor failure without
        // requiring a marker retry after the durable commit marker exists.
        let handle = coordinator.get_or_create_group(&group_id, GroupKindTag::Classic);
        let (reply, response) = tokio::sync::oneshot::channel();
        if handle
            .tx
            .send(GroupActorMessage::UpdateCommitted { entries, reply })
            .await
            .is_err()
            || response.await.is_err()
        {
            tracing::warn!(
                group = %group_id,
                "WriteTxnMarkers: could not publish committed transactional offsets"
            );
            return Err(BrokerError::Txn(format!(
                "could not publish committed transactional offsets for group {group_id}"
            )));
        }
    }
    Ok(())
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
            false,
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

    #[tokio::test]
    async fn committed_offsets_are_published_by_the_offsets_partition_marker() {
        use crabka_log::Offset;
        use crabka_protocol::records::{Attributes, Record, RecordBatch};

        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let group_id = "marker-materialization-group";
        let offsets_partition = crate::coordinator::partitioner::partition_for_group(
            &broker.controller.current_image(),
            group_id,
        );
        let part = broker
            .partitions
            .get(OFFSETS_TOPIC, PartitionIndex(offsets_partition))
            .expect("local offsets partition");
        let value = OffsetCommitValue {
            offset: Offset(42),
            leader_epoch: 3,
            metadata: "txn".into(),
            commit_timestamp_ms: 123,
        };
        part.produce_batch(RecordBatch {
            producer_id: 91,
            producer_epoch: 4,
            attributes: Attributes::default().with_transactional(true),
            records: vec![Record {
                key: Some(OffsetCommitValue::encode_key(group_id, "orders", 2)),
                value: Some(value.encode_value()),
                ..Default::default()
            }],
            ..RecordBatch::default()
        })
        .await
        .expect("append transactional offset");

        let req = WriteTxnMarkersRequest {
            markers: vec![WritableTxnMarker {
                producer_id: 91,
                producer_epoch: 4,
                transaction_result: true,
                transaction_version: 1,
                topics: vec![WritableTxnMarkerTopic {
                    name: OFFSETS_TOPIC.into(),
                    partition_indexes: vec![offsets_partition],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let response = super::handle(&broker, VERSION, 1, &encode_request(&req))
            .await
            .expect("commit marker");
        let response = decode_response(&response);
        assert!(response.markers[0].topics[0].partitions[0].error_code == codes::NONE);

        let handle = broker
            .group_coordinator
            .find(group_id)
            .expect("offset home actor");
        let (reply, result) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::FetchCommitted { reply })
            .await
            .unwrap();
        let committed = result.await.unwrap();
        let entry = committed
            .get(&("orders".to_string(), 2))
            .expect("committed offset visible");
        assert!(entry.offset == 42);
        assert!(entry.leader_epoch == 3);
        assert!(entry.metadata == "txn");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn aborted_offsets_are_not_published_by_the_offsets_partition_marker() {
        use crabka_log::Offset;
        use crabka_protocol::records::{Attributes, Record, RecordBatch};

        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let group_id = "marker-abort-group";
        let offsets_partition = crate::coordinator::partitioner::partition_for_group(
            &broker.controller.current_image(),
            group_id,
        );
        let part = broker
            .partitions
            .get(OFFSETS_TOPIC, PartitionIndex(offsets_partition))
            .expect("local offsets partition");
        let producer_id = crabka_log::ProducerId(92);
        part.produce_batch(RecordBatch {
            producer_id: producer_id.get(),
            producer_epoch: 5,
            attributes: Attributes::default().with_transactional(true),
            records: vec![Record {
                key: Some(OffsetCommitValue::encode_key(group_id, "orders", 3)),
                value: Some(
                    OffsetCommitValue {
                        offset: Offset(99),
                        leader_epoch: 4,
                        metadata: "aborted".into(),
                        commit_timestamp_ms: 456,
                    }
                    .encode_value(),
                ),
                ..Record::default()
            }],
            ..RecordBatch::default()
        })
        .await
        .expect("append transactional offset");

        append_marker_and_materialize(
            &part,
            Some(&broker.group_coordinator),
            OFFSETS_TOPIC,
            MarkerAppend {
                producer_id,
                producer_epoch: 5,
                marker_type: MarkerType::Abort,
                coordinator_epoch: 0,
                commit_stamp: None,
            },
        )
        .await
        .expect("abort marker");

        assert!(broker.group_coordinator.find(group_id).is_none());
        {
            let log = part.log.lock().expect("offsets log lock");
            assert!(log.pending_transaction_start(producer_id).is_none());
        }
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn internal_marker_path_records_supplied_commit_stamp() {
        use crabka_protocol::records::{Attributes, Record, RecordBatch};

        let (broker_handle, dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        open_partition(&broker, dir.path(), "stamped-orders", 0);
        let part = broker
            .partitions
            .get("stamped-orders", PartitionIndex(0))
            .expect("local partition");
        part.log
            .lock()
            .expect("partition log")
            .set_stamp_source(Arc::new(crabka_log::MonotonicStampSource::new(1, 1)))
            .expect("install stamp source");

        let producer_id = crabka_log::ProducerId(700);
        part.produce_batch(RecordBatch {
            producer_id: producer_id.get(),
            producer_epoch: 2,
            attributes: Attributes::default().with_transactional(true),
            records: vec![Record {
                value: Some(Bytes::from_static(b"event")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        })
        .await
        .expect("append transactional data");
        assert!(part.stamp_for_offset(crabka_log::Offset(0)).is_none());

        append_marker_and_materialize(
            &part,
            None,
            "stamped-orders",
            MarkerAppend {
                producer_id,
                producer_epoch: 2,
                marker_type: MarkerType::Commit,
                coordinator_epoch: 0,
                commit_stamp: Some(900),
            },
        )
        .await
        .expect("commit marker");

        assert!(part.stamp_for_offset(crabka_log::Offset(0)) == Some(900));
        assert!(part.stamp_for_offset(crabka_log::Offset(1)).is_none());
        broker_handle.shutdown().await;
    }
}
