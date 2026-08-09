//! Shard-addressed WAL RPC descriptors.

use bytes::Bytes;
use crabka_ids::PartitionIndex;
use crabka_protocol::{Decode, Encode, owned::fetch_request::FetchRequest};
use crabka_units::{ByteSize, convert::ByteSizeExt as _};

const KRAFT_METADATA_TOPIC_ID: crabka_protocol::primitives::uuid::Uuid =
    crabka_protocol::primitives::uuid::Uuid([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
const KIP_595_FETCH_VERSION: i16 = 17;
const NONE: i16 = 0;
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

/// Group discriminator for KIP-595 traffic carried by the broker listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum QuorumGroup {
    Metadata,
    DisklessWal {
        topic_id: uuid::Uuid,
        partition: PartitionIndex,
    },
}

// No `Eq`: `max_size` is a `ByteSize`, which stores `f64`. The derive was
// unused — `WalFetchRequest` is only destructured, never compared or hashed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct WalFetchRequest {
    pub(crate) group: QuorumGroup,
    pub(crate) from: crabka_raft::NodeId,
    pub(crate) fetch_offset: i64,
    pub(crate) max_size: ByteSize,
}

impl QuorumGroup {
    #[must_use]
    pub(crate) fn diskless_wal(topic_id: uuid::Uuid, partition: PartitionIndex) -> Self {
        Self::DisklessWal {
            topic_id,
            partition,
        }
    }
}

/// Decodes enough of the KIP-595 `Fetch` body to choose the target quorum
/// group.
///
/// The fixed `__cluster_metadata` topic id identifies Kafka's metadata quorum.
/// Diskless WAL shards reuse the same KIP-595 Fetch envelope, but they use the
/// data topic id and the partition as the shard address.
#[allow(dead_code)]
pub(crate) fn classify_fetch(body: &[u8]) -> Option<QuorumGroup> {
    decode_fetch(body).map(|request| request.group)
}

pub(crate) fn decode_fetch(body: &[u8]) -> Option<WalFetchRequest> {
    let mut cur = body;
    let request = FetchRequest::decode(&mut cur, KIP_595_FETCH_VERSION).ok()?;
    let topic = request.topics.first()?;
    let partition = topic.partitions.first()?;
    let group = if topic.topic_id == KRAFT_METADATA_TOPIC_ID {
        QuorumGroup::Metadata
    } else {
        QuorumGroup::diskless_wal(
            uuid::Uuid::from_bytes(topic.topic_id.0),
            PartitionIndex(partition.partition),
        )
    };
    Some(WalFetchRequest {
        group,
        from: crabka_raft::NodeId(u64::try_from(request.replica_state.replica_id).ok()?),
        fetch_offset: partition.fetch_offset,
        max_size: ByteSize::from_bytes_i64(i64::from(request.max_bytes.max(0))),
    })
}

pub(crate) fn encode_fetch_response(group: QuorumGroup, hwm: i64, records: Bytes) -> Bytes {
    encode_fetch_response_with_error(group, hwm, records, NONE)
}

pub(crate) fn encode_unknown_shard_fetch_response(group: QuorumGroup) -> Bytes {
    encode_fetch_response_with_error(group, 0, Bytes::new(), UNKNOWN_TOPIC_OR_PARTITION)
}

fn encode_fetch_response_with_error(
    group: QuorumGroup,
    hwm: i64,
    records: Bytes,
    error_code: i16,
) -> Bytes {
    use crabka_protocol::{owned::fetch_response as fetch_resp, records::RecordsPayload};

    let (topic, topic_id, partition) = match group {
        QuorumGroup::Metadata => ("__cluster_metadata".to_string(), KRAFT_METADATA_TOPIC_ID, 0),
        QuorumGroup::DisklessWal {
            topic_id,
            partition,
        } => (
            String::new(),
            crabka_protocol::primitives::uuid::Uuid(*topic_id.as_bytes()),
            partition.0,
        ),
    };
    let response = fetch_resp::FetchResponse {
        responses: vec![fetch_resp::FetchableTopicResponse {
            topic,
            topic_id,
            partitions: vec![fetch_resp::PartitionData {
                partition_index: partition,
                error_code,
                high_watermark: hwm,
                last_stable_offset: hwm,
                log_start_offset: 0,
                records: (!records.is_empty()).then_some(RecordsPayload::Raw(records)),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut out = bytes::BytesMut::new();
    let _ = response.encode(&mut out, KIP_595_FETCH_VERSION);
    out.freeze()
}

#[allow(dead_code)]
pub(crate) fn encode_fetch_for_group(
    group: QuorumGroup,
    from: crabka_raft::NodeId,
    fetch_epoch: i32,
    fetch_offset: i64,
) -> Bytes {
    use crabka_protocol::owned::fetch_request as fetch_req;

    let (topic, topic_id, partition) = match group {
        QuorumGroup::Metadata => ("__cluster_metadata".to_string(), KRAFT_METADATA_TOPIC_ID, 0),
        QuorumGroup::DisklessWal {
            topic_id,
            partition,
        } => (
            String::new(),
            crabka_protocol::primitives::uuid::Uuid(*topic_id.as_bytes()),
            partition.0,
        ),
    };
    let request = FetchRequest {
        replica_state: fetch_req::ReplicaState {
            replica_id: i32::try_from(from.0).unwrap_or(i32::MAX),
            replica_epoch: -1,
            ..Default::default()
        },
        topics: vec![fetch_req::FetchTopic {
            topic,
            topic_id,
            partitions: vec![fetch_req::FetchPartition {
                partition,
                current_leader_epoch: fetch_epoch,
                fetch_offset,
                last_fetched_epoch: fetch_epoch,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut out = bytes::BytesMut::new();
    let _ = request.encode(&mut out, KIP_595_FETCH_VERSION);
    out.freeze()
}

#[cfg(test)]
mod tests {
    use crabka_raft::NodeId;

    use super::*;

    #[test]
    fn classify_fetch_distinguishes_metadata_quorum() {
        let body = encode_fetch_for_group(QuorumGroup::Metadata, NodeId(2), 7, 11);

        assert_eq!(classify_fetch(&body), Some(QuorumGroup::Metadata));
    }

    #[test]
    fn classify_fetch_extracts_diskless_wal_shard() {
        let topic_id = uuid::Uuid::from_u128(42);
        let body = encode_fetch_for_group(
            QuorumGroup::diskless_wal(topic_id, PartitionIndex(3)),
            NodeId(2),
            7,
            11,
        );

        assert_eq!(
            classify_fetch(&body),
            Some(QuorumGroup::DisklessWal {
                topic_id,
                partition: PartitionIndex(3),
            })
        );
    }
}
