use crate::{
    kafka_3_6_2,
    owned::{produce_request::ProduceRequest, produce_response::ProduceResponse},
};

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

#[cfg(test)]
mod tests {

    use bytes::Bytes;

    use super::*;
    use crate::{UnknownTaggedFields, records::RecordsPayload};

    #[test]
    fn legacy_produce_request_conversion_preserves_mapped_fields() {
        let mut legacy = kafka_3_6_2::owned::produce_request::ProduceRequest::populated(9);
        legacy.transactional_id = Some("txn-a".into());
        legacy.acks = 1;
        legacy.timeout_ms = 72;
        legacy.topic_data[0].name = "produce-topic".into();
        legacy.topic_data[0].partition_data[0].index = 73;
        legacy.topic_data[0].partition_data[0].records =
            Some(RecordsPayload::Legacy(Bytes::from_static(&[4, 5, 6])));
        let converted = ProduceRequest::from(legacy);

        let expected = ProduceRequest {
            transactional_id: Some("txn-a".to_string()),
            acks: 1,
            timeout_ms: 72,
            topic_data: vec![crate::owned::produce_request::TopicProduceData {
                name: "produce-topic".to_string(),
                topic_id: crate::primitives::uuid::Uuid::ZERO,
                partition_data: vec![crate::owned::produce_request::PartitionProduceData {
                    index: 73,
                    records: Some(RecordsPayload::Legacy(Bytes::from_static(&[4, 5, 6]))),
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        };
        assert2::assert!(converted == expected);
    }

    #[test]
    fn produce_response_conversion_preserves_mapped_fields() {
        let mut canonical = ProduceResponse::populated(12);
        canonical.throttle_time_ms = 81;
        canonical.responses[0].name = "produce-response-topic".into();
        canonical.responses[0].partition_responses[0].index = 82;
        canonical.responses[0].partition_responses[0].error_code = 83;
        canonical.responses[0].partition_responses[0].base_offset = 84;
        canonical.responses[0].partition_responses[0].log_append_time_ms = 85;
        canonical.responses[0].partition_responses[0].log_start_offset = 86;
        canonical.responses[0].partition_responses[0].error_message = Some("produce-error".into());
        canonical.responses[0].partition_responses[0].record_errors[0].batch_index = 87;
        canonical.responses[0].partition_responses[0].record_errors[0].batch_index_error_message =
            Some("batch-error".into());
        let converted = kafka_3_6_2::owned::produce_response::ProduceResponse::from(canonical);

        let expected = kafka_3_6_2::owned::produce_response::ProduceResponse {
            responses: vec![kafka_3_6_2::owned::produce_response::TopicProduceResponse {
                name: "produce-response-topic".to_string(),
                partition_responses: vec![
                    kafka_3_6_2::owned::produce_response::PartitionProduceResponse {
                        index: 82,
                        error_code: 83,
                        base_offset: 84,
                        log_append_time_ms: 85,
                        log_start_offset: 86,
                        record_errors: vec![
                            kafka_3_6_2::owned::produce_response::BatchIndexAndErrorMessage {
                                batch_index: 87,
                                batch_index_error_message: Some("batch-error".to_string()),
                                unknown_tagged_fields: UnknownTaggedFields(vec![]),
                            },
                        ],
                        error_message: Some("produce-error".to_string()),
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    },
                ],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }],
            throttle_time_ms: 81,
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        };
        assert2::assert!(converted == expected);
    }
}
