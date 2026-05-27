use crate::kafka_3_6_2;
use crate::owned::produce_request::ProduceRequest;
use crate::owned::produce_response::ProduceResponse;

// ── Request: legacy → canonical ──────────────────────────────────────────────

impl From<kafka_3_6_2::owned::produce_request::ProduceRequest> for ProduceRequest {
    fn from(legacy: kafka_3_6_2::owned::produce_request::ProduceRequest) -> Self {
        Self {
            transactional_id: legacy.transactional_id,
            acks: legacy.acks,
            timeout_ms: legacy.timeout_ms,
            topic_data: legacy.topic_data.into_iter().map(Into::into).collect(),
            ..Default::default()
        }
    }
}

impl From<kafka_3_6_2::owned::produce_request::TopicProduceData>
    for crate::owned::produce_request::TopicProduceData
{
    fn from(l: kafka_3_6_2::owned::produce_request::TopicProduceData) -> Self {
        Self {
            name: l.name,
            partition_data: l.partition_data.into_iter().map(Into::into).collect(),
            // topic_id (v13+) defaults to Uuid::nil()
            ..Default::default()
        }
    }
}

impl From<kafka_3_6_2::owned::produce_request::PartitionProduceData>
    for crate::owned::produce_request::PartitionProduceData
{
    fn from(l: kafka_3_6_2::owned::produce_request::PartitionProduceData) -> Self {
        Self {
            index: l.index,
            // RecordsPayload is the same type across both namespaces
            records: l.records,
            ..Default::default()
        }
    }
}

// ── Response: canonical → legacy ─────────────────────────────────────────────

impl From<ProduceResponse> for kafka_3_6_2::owned::produce_response::ProduceResponse {
    fn from(c: ProduceResponse) -> Self {
        Self {
            responses: c.responses.into_iter().map(Into::into).collect(),
            throttle_time_ms: c.throttle_time_ms,
            ..Default::default()
        }
    }
}

impl From<crate::owned::produce_response::TopicProduceResponse>
    for kafka_3_6_2::owned::produce_response::TopicProduceResponse
{
    fn from(c: crate::owned::produce_response::TopicProduceResponse) -> Self {
        Self {
            name: c.name,
            partition_responses: c.partition_responses.into_iter().map(Into::into).collect(),
            ..Default::default()
        }
    }
}

impl From<crate::owned::produce_response::PartitionProduceResponse>
    for kafka_3_6_2::owned::produce_response::PartitionProduceResponse
{
    fn from(c: crate::owned::produce_response::PartitionProduceResponse) -> Self {
        Self {
            index: c.index,
            error_code: c.error_code,
            base_offset: c.base_offset,
            log_append_time_ms: c.log_append_time_ms,
            log_start_offset: c.log_start_offset,
            record_errors: c.record_errors.into_iter().map(Into::into).collect(),
            error_message: c.error_message,
            ..Default::default() // Defaulted: current_leader (canonical-only v10+ tagged field)
        }
    }
}

impl From<crate::owned::produce_response::BatchIndexAndErrorMessage>
    for kafka_3_6_2::owned::produce_response::BatchIndexAndErrorMessage
{
    fn from(c: crate::owned::produce_response::BatchIndexAndErrorMessage) -> Self {
        Self {
            batch_index: c.batch_index,
            batch_index_error_message: c.batch_index_error_message,
            ..Default::default()
        }
    }
}
