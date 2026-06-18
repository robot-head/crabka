use crate::kafka_3_6_2;
use crate::owned::fetch_request::FetchRequest;
use crate::owned::fetch_response::FetchResponse;

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
    use super::*;
    use crate::primitives::uuid::Uuid;
    use crate::records::RecordsPayload;
    use assert2::assert;
    use bytes::Bytes;

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
        let converted = FetchRequest::from(legacy.clone());

        assert!(converted.replica_id == legacy.replica_id);
        assert!(converted.max_wait_ms == legacy.max_wait_ms);
        assert!(converted.min_bytes == legacy.min_bytes);
        assert!(converted.max_bytes == legacy.max_bytes);
        assert!(converted.isolation_level == legacy.isolation_level);
        assert!(converted.session_id == legacy.session_id);
        assert!(converted.session_epoch == legacy.session_epoch);
        assert!(converted.rack_id == legacy.rack_id);
        assert!(converted.cluster_id == legacy.cluster_id);
        assert!(converted.topics.len() == legacy.topics.len());
        assert!(converted.forgotten_topics_data.len() == legacy.forgotten_topics_data.len());
        assert!(converted.replica_state.replica_id == legacy.replica_state.replica_id);
        assert!(converted.replica_state.replica_epoch == legacy.replica_state.replica_epoch);

        let topic = &converted.topics[0];
        let legacy_topic = &legacy.topics[0];
        assert!(topic.topic == legacy_topic.topic);
        assert!(topic.partitions.len() == legacy_topic.partitions.len());

        let partition = &topic.partitions[0];
        let legacy_partition = &legacy_topic.partitions[0];
        assert!(partition.partition == legacy_partition.partition);
        assert!(partition.current_leader_epoch == legacy_partition.current_leader_epoch);
        assert!(partition.fetch_offset == legacy_partition.fetch_offset);
        assert!(partition.last_fetched_epoch == legacy_partition.last_fetched_epoch);
        assert!(partition.log_start_offset == legacy_partition.log_start_offset);
        assert!(partition.partition_max_bytes == legacy_partition.partition_max_bytes);

        let forgotten = &converted.forgotten_topics_data[0];
        let legacy_forgotten = &legacy.forgotten_topics_data[0];
        assert!(forgotten.topic == legacy_forgotten.topic);
        assert!(forgotten.partitions == legacy_forgotten.partitions);
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
        let converted = kafka_3_6_2::owned::fetch_response::FetchResponse::from(canonical.clone());

        assert!(converted.throttle_time_ms == canonical.throttle_time_ms);
        assert!(converted.error_code == canonical.error_code);
        assert!(converted.session_id == canonical.session_id);
        assert!(converted.responses.len() == canonical.responses.len());

        let topic = &converted.responses[0];
        let canonical_topic = &canonical.responses[0];
        assert!(topic.topic == canonical_topic.topic);
        assert!(topic.topic_id == canonical_topic.topic_id);
        assert!(topic.partitions.len() == canonical_topic.partitions.len());

        let partition = &topic.partitions[0];
        let canonical_partition = &canonical_topic.partitions[0];
        assert!(partition.partition_index == canonical_partition.partition_index);
        assert!(partition.error_code == canonical_partition.error_code);
        assert!(partition.high_watermark == canonical_partition.high_watermark);
        assert!(partition.last_stable_offset == canonical_partition.last_stable_offset);
        assert!(partition.log_start_offset == canonical_partition.log_start_offset);
        assert!(partition.preferred_read_replica == canonical_partition.preferred_read_replica);
        assert!(partition.records == canonical_partition.records);
        assert!(partition.diverging_epoch.epoch == canonical_partition.diverging_epoch.epoch);
        assert!(
            partition.diverging_epoch.end_offset == canonical_partition.diverging_epoch.end_offset
        );
        assert!(partition.current_leader.leader_id == canonical_partition.current_leader.leader_id);
        assert!(
            partition.current_leader.leader_epoch
                == canonical_partition.current_leader.leader_epoch
        );
        assert!(partition.snapshot_id.end_offset == canonical_partition.snapshot_id.end_offset);
        assert!(partition.snapshot_id.epoch == canonical_partition.snapshot_id.epoch);

        let aborted = partition
            .aborted_transactions
            .as_ref()
            .expect("aborted transactions")[0]
            .clone();
        let canonical_aborted = canonical_partition
            .aborted_transactions
            .as_ref()
            .expect("canonical aborted transactions")[0]
            .clone();
        assert!(aborted.producer_id == canonical_aborted.producer_id);
        assert!(aborted.first_offset == canonical_aborted.first_offset);
    }
}
