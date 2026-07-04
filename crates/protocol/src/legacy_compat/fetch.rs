use crate::{
    kafka_3_6_2,
    owned::{fetch_request::FetchRequest, fetch_response::FetchResponse},
};

// ── Request: legacy → canonical ──────────────────────────────────────────────

impl From<kafka_3_6_2::owned::fetch_request::FetchRequest> for FetchRequest {
    fn from(l: kafka_3_6_2::owned::fetch_request::FetchRequest) -> Self {
        Self {
            replica_id: l.replica_id,
            max_wait_ms: l.max_wait_ms,
            min_bytes: l.min_bytes,
            max_bytes: l.max_bytes,
            isolation_level: l.isolation_level,
            session_id: l.session_id,
            session_epoch: l.session_epoch,
            topics: l.topics.into_iter().map(Into::into).collect(),
            forgotten_topics_data: l
                .forgotten_topics_data
                .into_iter()
                .map(Into::into)
                .collect(),
            rack_id: l.rack_id,
            cluster_id: l.cluster_id,
            replica_state: l.replica_state.into(),
            ..Default::default()
        }
    }
}

impl From<kafka_3_6_2::owned::fetch_request::ReplicaState>
    for crate::owned::fetch_request::ReplicaState
{
    fn from(l: kafka_3_6_2::owned::fetch_request::ReplicaState) -> Self {
        Self {
            replica_id: l.replica_id,
            replica_epoch: l.replica_epoch,
            ..Default::default()
        }
    }
}

impl From<kafka_3_6_2::owned::fetch_request::FetchTopic>
    for crate::owned::fetch_request::FetchTopic
{
    fn from(l: kafka_3_6_2::owned::fetch_request::FetchTopic) -> Self {
        Self {
            topic: l.topic,
            // topic_id (v13+) defaults to Uuid::nil()
            partitions: l.partitions.into_iter().map(Into::into).collect(),
            ..Default::default()
        }
    }
}

impl From<kafka_3_6_2::owned::fetch_request::FetchPartition>
    for crate::owned::fetch_request::FetchPartition
{
    fn from(l: kafka_3_6_2::owned::fetch_request::FetchPartition) -> Self {
        Self {
            partition: l.partition,
            current_leader_epoch: l.current_leader_epoch,
            fetch_offset: l.fetch_offset,
            last_fetched_epoch: l.last_fetched_epoch,
            log_start_offset: l.log_start_offset,
            partition_max_bytes: l.partition_max_bytes,
            // replica_directory_id (v17+ tagged) and high_watermark (v18+ tagged) default
            ..Default::default()
        }
    }
}

impl From<kafka_3_6_2::owned::fetch_request::ForgottenTopic>
    for crate::owned::fetch_request::ForgottenTopic
{
    fn from(l: kafka_3_6_2::owned::fetch_request::ForgottenTopic) -> Self {
        Self {
            topic: l.topic,
            // topic_id (v13+) defaults to Uuid::nil()
            partitions: l.partitions,
            ..Default::default()
        }
    }
}

// ── Response: canonical → legacy ─────────────────────────────────────────────

impl From<FetchResponse> for kafka_3_6_2::owned::fetch_response::FetchResponse {
    fn from(c: FetchResponse) -> Self {
        Self {
            throttle_time_ms: c.throttle_time_ms,
            error_code: c.error_code,
            session_id: c.session_id,
            responses: c.responses.into_iter().map(Into::into).collect(),
            // node_endpoints (canonical v16+ tagged field) dropped — not present in 3.6.2 schema
            ..Default::default()
        }
    }
}

impl From<crate::owned::fetch_response::FetchableTopicResponse>
    for kafka_3_6_2::owned::fetch_response::FetchableTopicResponse
{
    fn from(c: crate::owned::fetch_response::FetchableTopicResponse) -> Self {
        Self {
            topic: c.topic,
            topic_id: c.topic_id,
            partitions: c.partitions.into_iter().map(Into::into).collect(),
            ..Default::default()
        }
    }
}

impl From<crate::owned::fetch_response::PartitionData>
    for kafka_3_6_2::owned::fetch_response::PartitionData
{
    fn from(c: crate::owned::fetch_response::PartitionData) -> Self {
        Self {
            partition_index: c.partition_index,
            error_code: c.error_code,
            high_watermark: c.high_watermark,
            last_stable_offset: c.last_stable_offset,
            log_start_offset: c.log_start_offset,
            aborted_transactions: c
                .aborted_transactions
                .map(|v| v.into_iter().map(Into::into).collect()),
            preferred_read_replica: c.preferred_read_replica,
            records: c.records,
            diverging_epoch: c.diverging_epoch.into(),
            current_leader: c.current_leader.into(),
            snapshot_id: c.snapshot_id.into(),
            ..Default::default()
        }
    }
}

impl From<crate::owned::fetch_response::AbortedTransaction>
    for kafka_3_6_2::owned::fetch_response::AbortedTransaction
{
    fn from(c: crate::owned::fetch_response::AbortedTransaction) -> Self {
        Self {
            producer_id: c.producer_id,
            first_offset: c.first_offset,
            ..Default::default()
        }
    }
}

impl From<crate::owned::fetch_response::EpochEndOffset>
    for kafka_3_6_2::owned::fetch_response::EpochEndOffset
{
    fn from(c: crate::owned::fetch_response::EpochEndOffset) -> Self {
        Self {
            epoch: c.epoch,
            end_offset: c.end_offset,
            ..Default::default()
        }
    }
}

impl From<crate::owned::fetch_response::LeaderIdAndEpoch>
    for kafka_3_6_2::owned::fetch_response::LeaderIdAndEpoch
{
    fn from(c: crate::owned::fetch_response::LeaderIdAndEpoch) -> Self {
        Self {
            leader_id: c.leader_id,
            leader_epoch: c.leader_epoch,
            ..Default::default()
        }
    }
}

impl From<crate::owned::fetch_response::SnapshotId>
    for kafka_3_6_2::owned::fetch_response::SnapshotId
{
    fn from(c: crate::owned::fetch_response::SnapshotId) -> Self {
        Self {
            end_offset: c.end_offset,
            epoch: c.epoch,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;

    use super::*;
    use crate::{UnknownTaggedFields, primitives::uuid::Uuid, records::RecordsPayload};

    #[test]
    fn legacy_fetch_request_conversion_preserves_mapped_fields() {
        let mut legacy = kafka_3_6_2::owned::fetch_request::FetchRequest::populated(15);
        legacy.replica_id = 42;
        legacy.max_wait_ms = 43;
        legacy.min_bytes = 44;
        legacy.max_bytes = 45;
        legacy.isolation_level = 1;
        legacy.session_id = 46;
        legacy.session_epoch = 47;
        legacy.rack_id = "rack-a".into();
        legacy.cluster_id = Some("cluster-a".into());
        legacy.replica_state.replica_id = 48;
        legacy.replica_state.replica_epoch = 49;
        legacy.topics[0].topic = "fetch-topic".into();
        legacy.topics[0].partitions[0].partition = 3;
        legacy.topics[0].partitions[0].current_leader_epoch = 4;
        legacy.topics[0].partitions[0].fetch_offset = 5;
        legacy.topics[0].partitions[0].last_fetched_epoch = 6;
        legacy.topics[0].partitions[0].log_start_offset = 7;
        legacy.topics[0].partitions[0].partition_max_bytes = 8;
        legacy.forgotten_topics_data[0].topic = "forgotten-topic".into();
        legacy.forgotten_topics_data[0].partitions = vec![9, 10];
        let converted = FetchRequest::from(legacy);

        let expected = FetchRequest {
            replica_id: 42,
            max_wait_ms: 43,
            min_bytes: 44,
            max_bytes: 45,
            isolation_level: 1,
            session_id: 46,
            session_epoch: 47,
            topics: vec![crate::owned::fetch_request::FetchTopic {
                topic: "fetch-topic".to_string(),
                topic_id: Uuid::ZERO,
                partitions: vec![crate::owned::fetch_request::FetchPartition {
                    partition: 3,
                    current_leader_epoch: 4,
                    fetch_offset: 5,
                    last_fetched_epoch: 6,
                    log_start_offset: 7,
                    partition_max_bytes: 8,
                    replica_directory_id: Uuid::ZERO,
                    high_watermark: 9_223_372_036_854_775_807,
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }],
            forgotten_topics_data: vec![crate::owned::fetch_request::ForgottenTopic {
                topic: "forgotten-topic".to_string(),
                topic_id: Uuid::ZERO,
                partitions: vec![9, 10],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }],
            rack_id: "rack-a".to_string(),
            cluster_id: Some("cluster-a".to_string()),
            replica_state: crate::owned::fetch_request::ReplicaState {
                replica_id: 48,
                replica_epoch: 49,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            },
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        };
        assert!(converted == expected);
    }

    #[test]
    fn fetch_response_conversion_preserves_mapped_fields() {
        let mut canonical = FetchResponse::populated(18);
        canonical.throttle_time_ms = 51;
        canonical.error_code = 52;
        canonical.session_id = 53;
        canonical.responses[0].topic = "fetch-response-topic".into();
        canonical.responses[0].topic_id = Uuid([0x12; 16]);
        canonical.responses[0].partitions[0].partition_index = 54;
        canonical.responses[0].partitions[0].error_code = 55;
        canonical.responses[0].partitions[0].high_watermark = 56;
        canonical.responses[0].partitions[0].last_stable_offset = 57;
        canonical.responses[0].partitions[0].log_start_offset = 58;
        canonical.responses[0].partitions[0].preferred_read_replica = 59;
        canonical.responses[0].partitions[0].records =
            Some(RecordsPayload::Legacy(Bytes::from_static(&[1, 2, 3])));
        canonical.responses[0].partitions[0].diverging_epoch.epoch = 60;
        canonical.responses[0].partitions[0]
            .diverging_epoch
            .end_offset = 61;
        canonical.responses[0].partitions[0]
            .current_leader
            .leader_id = 62;
        canonical.responses[0].partitions[0]
            .current_leader
            .leader_epoch = 63;
        canonical.responses[0].partitions[0].snapshot_id.end_offset = 64;
        canonical.responses[0].partitions[0].snapshot_id.epoch = 65;
        canonical.responses[0].partitions[0]
            .aborted_transactions
            .as_mut()
            .expect("aborted transactions")[0]
            .producer_id = 66;
        canonical.responses[0].partitions[0]
            .aborted_transactions
            .as_mut()
            .expect("aborted transactions")[0]
            .first_offset = 67;
        let converted = kafka_3_6_2::owned::fetch_response::FetchResponse::from(canonical);

        let expected = kafka_3_6_2::owned::fetch_response::FetchResponse {
            throttle_time_ms: 51,
            error_code: 52,
            session_id: 53,
            responses: vec![kafka_3_6_2::owned::fetch_response::FetchableTopicResponse {
                topic: "fetch-response-topic".to_string(),
                topic_id: Uuid([0x12; 16]),
                partitions: vec![kafka_3_6_2::owned::fetch_response::PartitionData {
                    partition_index: 54,
                    error_code: 55,
                    high_watermark: 56,
                    last_stable_offset: 57,
                    log_start_offset: 58,
                    aborted_transactions: Some(vec![
                        kafka_3_6_2::owned::fetch_response::AbortedTransaction {
                            producer_id: 66,
                            first_offset: 67,
                            unknown_tagged_fields: UnknownTaggedFields(vec![]),
                        },
                    ]),
                    preferred_read_replica: 59,
                    records: Some(RecordsPayload::Legacy(Bytes::from_static(&[1, 2, 3]))),
                    diverging_epoch: kafka_3_6_2::owned::fetch_response::EpochEndOffset {
                        epoch: 60,
                        end_offset: 61,
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    },
                    current_leader: kafka_3_6_2::owned::fetch_response::LeaderIdAndEpoch {
                        leader_id: 62,
                        leader_epoch: 63,
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    },
                    snapshot_id: kafka_3_6_2::owned::fetch_response::SnapshotId {
                        end_offset: 64,
                        epoch: 65,
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    },
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        };
        assert!(converted == expected);
    }
}
