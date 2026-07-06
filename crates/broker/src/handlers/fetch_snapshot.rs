//! `FetchSnapshot` (`api_key=59`, KIP-630). Serves a byte range of the
//! controller's `__cluster_metadata` snapshot to a replica catching up.
//!
//! Crabka runs a single raft log (the controller quorum) and snapshots its
//! `MetadataImage`. A replica fetches the snapshot one page at a time by
//! advancing `position`; each response carries the requested byte range
//! verbatim plus the snapshot's `(end_offset, epoch)` id and total `size`.
//!
//! The returned bytes are *unaligned*: a snapshot is many concatenated
//! record batches and a paged byte range is by design not batch-aligned,
//! so the range is shipped as `RecordsPayload::Legacy` (written verbatim)
//! rather than decoded into a single `RecordBatch`.
//!
//! Any topic other than `__cluster_metadata` / partition 0 gets
//! `INVALID_TOPIC_EXCEPTION` (17). A request whose `cluster_id` doesn't
//! match this cluster gets a top-level `INCONSISTENT_CLUSTER_ID` (104).

use bytes::Bytes;
use crabka_protocol::{
    Decode,
    owned::{
        fetch_snapshot_request::FetchSnapshotRequest,
        fetch_snapshot_response::{
            FetchSnapshotResponse, LeaderIdAndEpoch, PartitionSnapshot, SnapshotId, TopicSnapshot,
        },
    },
    records::RecordsPayload,
};
use crabka_raft::SnapshotRange;
use futures_util::future::BoxFuture;

use crate::{broker::Broker, codes, error::BrokerError};

/// The single Kafka-side topic name that represents the `KRaft` metadata
/// log. Mirrors `org.apache.kafka.common.Topic.CLUSTER_METADATA_TOPIC_NAME`.
const CLUSTER_METADATA_TOPIC: &str = "__cluster_metadata";

/// An error-carrying partition entry: every field zeroed except `index`
/// and `error_code`.
fn err_partition(index: i32, error_code: i16) -> PartitionSnapshot {
    PartitionSnapshot {
        index,
        error_code,
        snapshot_id: SnapshotId::default(),
        size: 0,
        position: 0,
        unaligned_records: RecordsPayload::default(),
        current_leader: LeaderIdAndEpoch::default(),
        ..Default::default()
    }
}

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let controller = broker.controller.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = FetchSnapshotRequest::decode(&mut cur, version)?;
        let max_bytes = req.max_bytes;
        let resolve =
            |position: i64, _max: i32| controller.read_snapshot_range(position, max_bytes);
        let local_cluster_id = controller.current_image().cluster_id();
        let resp = build_response(local_cluster_id, &req, &resolve);
        crate::handlers::encode_response(&resp, version)
    })
}

/// Build the response from a decoded request. Pure — testable without a
/// live `Broker` by passing a `resolve` closure that stands in for
/// [`crabka_raft::ControllerHandle::read_snapshot_range`].
fn build_response(
    local_cluster_id: uuid::Uuid,
    req: &FetchSnapshotRequest,
    resolve: &dyn Fn(i64, i32) -> SnapshotRange,
) -> FetchSnapshotResponse {
    if let Some(s) = req.cluster_id.as_deref()
        && s != local_cluster_id.to_string()
    {
        return FetchSnapshotResponse {
            throttle_time_ms: 0,
            error_code: codes::INCONSISTENT_CLUSTER_ID,
            topics: Vec::new(),
            node_endpoints: Vec::new(),
            ..Default::default()
        };
    }

    let topics = req
        .topics
        .iter()
        .map(|topic| {
            let is_metadata = topic.name == CLUSTER_METADATA_TOPIC;
            let partitions = topic
                .partitions
                .iter()
                .map(|part| {
                    if !is_metadata || part.partition != 0 {
                        return err_partition(part.partition, codes::INVALID_TOPIC_EXCEPTION);
                    }
                    match resolve(part.position, req.max_bytes) {
                        SnapshotRange::NoSnapshot => err_partition(0, codes::SNAPSHOT_NOT_FOUND),
                        SnapshotRange::OutOfRange => err_partition(0, codes::POSITION_OUT_OF_RANGE),
                        SnapshotRange::Slice(slice) => PartitionSnapshot {
                            index: 0,
                            error_code: codes::NONE,
                            snapshot_id: SnapshotId {
                                end_offset: slice.end_offset,
                                epoch: slice.epoch,
                                ..Default::default()
                            },
                            size: slice.total_size,
                            position: part.position,
                            unaligned_records: RecordsPayload::Legacy(slice.bytes),
                            current_leader: LeaderIdAndEpoch::default(),
                            ..Default::default()
                        },
                    }
                })
                .collect();
            TopicSnapshot {
                name: topic.name.clone(),
                partitions,
                ..Default::default()
            }
        })
        .collect();

    FetchSnapshotResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        topics,
        node_endpoints: Vec::new(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_raft::SnapshotSlice;

    use super::*;

    #[test]
    fn build_response_serves_requested_range() {
        use crabka_protocol::owned::fetch_snapshot_request::{
            FetchSnapshotRequest, PartitionSnapshot as ReqPartition, SnapshotId as ReqSnapshotId,
            TopicSnapshot as ReqTopic,
        };
        use uuid::Uuid;
        let cid = Uuid::from_u128(7);
        let req = FetchSnapshotRequest {
            replica_id: -1,
            max_bytes: 1024,
            topics: vec![ReqTopic {
                name: CLUSTER_METADATA_TOPIC.into(),
                partitions: vec![ReqPartition {
                    partition: 0,
                    current_leader_epoch: 0,
                    snapshot_id: ReqSnapshotId {
                        end_offset: 6,
                        epoch: 1,
                        ..Default::default()
                    },
                    position: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            cluster_id: None,
            ..Default::default()
        };
        let resolve = |_pos: i64, _max: i32| {
            SnapshotRange::Slice(SnapshotSlice {
                end_offset: 6,
                epoch: 1,
                total_size: 100,
                bytes: bytes::Bytes::from_static(b"abc"),
            })
        };
        let resp = build_response(cid, &req, &resolve);
        let part = &resp.topics[0].partitions[0];
        check!(resp.error_code == 0);
        check!(part.error_code == 0);
        check!(part.snapshot_id.end_offset == 6);
        check!(part.snapshot_id.epoch == 1);
        check!(part.size == 100);
        check!(part.position == 0);
        let mut buf = bytes::BytesMut::new();
        part.unaligned_records.encode_to(&mut buf).unwrap();
        assert!(&buf[..] == b"abc");
    }

    #[test]
    fn build_response_rejects_cluster_id_mismatch() {
        use crabka_protocol::owned::fetch_snapshot_request::{
            FetchSnapshotRequest, PartitionSnapshot as ReqPartition, SnapshotId as ReqSnapshotId,
            TopicSnapshot as ReqTopic,
        };
        use uuid::Uuid;
        let cid = Uuid::from_u128(7);
        let req = FetchSnapshotRequest {
            replica_id: -1,
            max_bytes: 1024,
            topics: vec![ReqTopic {
                name: CLUSTER_METADATA_TOPIC.into(),
                partitions: vec![ReqPartition {
                    partition: 0,
                    current_leader_epoch: 0,
                    snapshot_id: ReqSnapshotId {
                        end_offset: 6,
                        epoch: 1,
                        ..Default::default()
                    },
                    position: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            cluster_id: Some("different".into()),
            ..Default::default()
        };
        let resolve = |_pos: i64, _max: i32| SnapshotRange::NoSnapshot;
        let resp = build_response(cid, &req, &resolve);
        assert!(resp.error_code == codes::INCONSISTENT_CLUSTER_ID);
        assert!(resp.topics.is_empty());
    }

    #[test]
    fn build_response_position_past_end_returns_out_of_range() {
        use crabka_protocol::owned::fetch_snapshot_request::{
            FetchSnapshotRequest, PartitionSnapshot as ReqPartition, TopicSnapshot as ReqTopic,
        };
        use uuid::Uuid;
        let cid = Uuid::from_u128(7);
        let req = FetchSnapshotRequest {
            replica_id: -1,
            max_bytes: 1024,
            topics: vec![ReqTopic {
                name: CLUSTER_METADATA_TOPIC.into(),
                partitions: vec![ReqPartition {
                    partition: 0,
                    position: 9_999,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            cluster_id: None,
            ..Default::default()
        };
        let resolve = |_pos: i64, _max: i32| SnapshotRange::OutOfRange;
        let resp = build_response(cid, &req, &resolve);
        let part = &resp.topics[0].partitions[0];
        assert!(resp.error_code == codes::NONE);
        assert!(part.error_code == codes::POSITION_OUT_OF_RANGE);
    }
}
