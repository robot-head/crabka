//! Client-side `OffsetForLeaderEpoch` (`api_key=23`) helper, used by the
//! consumer's KIP-320 position-validation pass. The broker, given a
//! partition's `leader_epoch`, returns the `end_offset` of that epoch —
//! the safe offset a fetcher must not have consumed past.

use crabka_protocol::owned::{
    offset_for_leader_epoch_request::{
        OffsetForLeaderEpochRequest, OffsetForLeaderPartition, OffsetForLeaderTopic,
    },
    offset_for_leader_epoch_response::OffsetForLeaderEpochResponse,
};

use crate::{connection::Connection, error::ClientError};

/// One leader-epoch end-offset answer for a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochEndOffset {
    pub partition: i32,
    /// The leader's view of the epoch (may be lower than requested if the
    /// requested epoch is unknown to the leader).
    pub leader_epoch: i32,
    /// First offset *after* the requested epoch on the leader's log, or
    /// `-1` (`UNDEFINED_OFFSET`) if the epoch is unknown.
    pub end_offset: i64,
    pub error_code: i16,
}

/// Send a single-partition `OffsetForLeaderEpoch` request. `current_leader_epoch`
/// is the epoch the caller believes the partition is in (for fencing);
/// `leader_epoch` is the epoch the caller wants the end offset of.
///
/// # Errors
/// Transport / version-negotiation failure, or a partition not present in the
/// response.
pub async fn offset_for_leader_epoch(
    conn: &Connection,
    topic: &str,
    partition: i32,
    current_leader_epoch: i32,
    leader_epoch: i32,
) -> Result<EpochEndOffset, ClientError> {
    let resp: OffsetForLeaderEpochResponse = conn
        .send(build_request(
            topic,
            partition,
            current_leader_epoch,
            leader_epoch,
        ))
        .await?;
    parse_single(&resp, topic, partition)
}

fn build_request(
    topic: &str,
    partition: i32,
    current_leader_epoch: i32,
    leader_epoch: i32,
) -> OffsetForLeaderEpochRequest {
    OffsetForLeaderEpochRequest {
        replica_id: -1,
        topics: vec![OffsetForLeaderTopic {
            topic: topic.to_string(),
            partitions: vec![OffsetForLeaderPartition {
                partition,
                current_leader_epoch,
                leader_epoch,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn parse_single(
    resp: &OffsetForLeaderEpochResponse,
    topic: &str,
    partition: i32,
) -> Result<EpochEndOffset, ClientError> {
    resp.topics
        .iter()
        .find(|t| t.topic == topic)
        .and_then(|t| t.partitions.iter().find(|p| p.partition == partition))
        .map(|p| EpochEndOffset {
            partition: p.partition,
            leader_epoch: p.leader_epoch,
            end_offset: p.end_offset,
            error_code: p.error_code,
        })
        .ok_or(ClientError::Server { error_code: -1 })
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_protocol::{
        UnknownTaggedFields,
        owned::offset_for_leader_epoch_response::{
            EpochEndOffset as WireEpochEndOffset, OffsetForLeaderEpochResponse,
            OffsetForLeaderTopicResult,
        },
    };

    use super::*;

    #[test]
    fn parse_single_extracts_partition_answer() {
        let resp = OffsetForLeaderEpochResponse {
            topics: vec![OffsetForLeaderTopicResult {
                topic: "t".into(),
                partitions: vec![WireEpochEndOffset {
                    partition: 0,
                    leader_epoch: 2,
                    end_offset: 42,
                    error_code: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let got = parse_single(&resp, "t", 0).unwrap();
        assert!(
            got == EpochEndOffset {
                partition: 0,
                leader_epoch: 2,
                end_offset: 42,
                error_code: 0
            }
        );
    }

    #[test]
    fn build_request_preserves_partition_epoch_query() {
        let req = build_request("orders", 2, 17, 11);

        assert!(
            req == OffsetForLeaderEpochRequest {
                replica_id: -1,
                topics: vec![OffsetForLeaderTopic {
                    topic: "orders".to_string(),
                    partitions: vec![OffsetForLeaderPartition {
                        partition: 2,
                        current_leader_epoch: 17,
                        leader_epoch: 11,
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn parse_single_missing_partition_is_error() {
        let resp = OffsetForLeaderEpochResponse::default();
        let err = parse_single(&resp, "t", 0).unwrap_err();
        assert!(matches!(err, ClientError::Server { error_code: -1 }));
    }
}
