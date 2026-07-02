//! Adapters between the canonical Produce/Fetch types and the
//! `kafka_3_6_2`-namespaced flavors emitted from the vendored 3.6.2
//! schemas. Used by the wire-router branches for Produce v0–2 and
//! Fetch v0–3.
//!
//! ## Unknown tagged fields
//!
//! All conversions in this module drop `unknown_tagged_fields` via
//! `..Default::default()`. This is deliberate: the legacy decoders only
//! handle Produce v0–2 / Fetch v0–3, which predate the KIP-482 tagged-fields
//! mechanism. Any tagged data on either side of a bridge is unreachable
//! by the wire format at these versions, so silently dropping it is safe.

mod fetch;
mod produce;

#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::{
        UnknownTaggedFields, kafka_3_6_2,
        owned::{
            fetch_request::FetchRequest, fetch_response::FetchResponse,
            produce_request::ProduceRequest, produce_response::ProduceResponse,
        },
    };

    #[test]
    fn produce_request_legacy_to_canonical_defaults_transactional_id_to_none() {
        let legacy = kafka_3_6_2::owned::produce_request::ProduceRequest {
            acks: 1,
            timeout_ms: 30_000,
            topic_data: vec![],
            ..Default::default()
        };
        let canonical: ProduceRequest = legacy.into();
        let expected = ProduceRequest {
            transactional_id: None,
            acks: 1,
            timeout_ms: 30_000,
            topic_data: vec![],
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        };
        assert!(canonical == expected);
    }

    #[test]
    fn produce_response_canonical_to_legacy_drops_modern_only_fields() {
        let canonical = ProduceResponse {
            throttle_time_ms: 17,
            responses: vec![],
            ..Default::default()
        };
        let legacy: kafka_3_6_2::owned::produce_response::ProduceResponse = canonical.into();
        assert!(legacy.throttle_time_ms == 17);
    }

    #[test]
    fn fetch_request_legacy_to_canonical_defaults_cluster_id_and_topic_id() {
        let legacy = kafka_3_6_2::owned::fetch_request::FetchRequest {
            max_wait_ms: 500,
            min_bytes: 1,
            ..Default::default()
        };
        let canonical: FetchRequest = legacy.into();
        let expected = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 2_147_483_647,
            isolation_level: 0,
            session_id: 0,
            session_epoch: -1,
            topics: vec![],
            forgotten_topics_data: vec![],
            rack_id: String::new(),
            cluster_id: None,
            replica_state: crate::owned::fetch_request::ReplicaState {
                replica_id: -1,
                replica_epoch: -1,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            },
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        };
        assert!(canonical == expected);
    }

    #[test]
    fn fetch_response_canonical_to_legacy_preserves_top_level_fields() {
        let canonical = FetchResponse {
            throttle_time_ms: 42,
            error_code: 0,
            session_id: 0,
            responses: vec![],
            ..Default::default()
        };
        let legacy: kafka_3_6_2::owned::fetch_response::FetchResponse = canonical.into();
        assert!(legacy.throttle_time_ms == 42);
    }
}
