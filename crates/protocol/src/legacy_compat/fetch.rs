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
