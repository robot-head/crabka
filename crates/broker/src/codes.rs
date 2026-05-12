//! Kafka wire-level error codes used in this MVP.
//!
//! Per-(topic, partition) response fields use these `i16` values.
//! JVM clients react to specific codes, so substituting them changes
//! client behavior — values here mirror the canonical Apache Kafka
//! table.

#![allow(dead_code)] // codes are consumed by handlers in Phase E.

pub const NONE: i16 = 0;
pub const UNKNOWN_SERVER_ERROR: i16 = 1;
pub const OFFSET_OUT_OF_RANGE: i16 = 2;
pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub const INVALID_FETCH_SIZE: i16 = 4;
pub const LEADER_NOT_AVAILABLE: i16 = 5;
pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
pub const REQUEST_TIMED_OUT: i16 = 7;
pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
pub const NOT_COORDINATOR: i16 = 16;
pub const INVALID_TOPIC_EXCEPTION: i16 = 17;
pub const UNSUPPORTED_VERSION: i16 = 35;
pub const TOPIC_ALREADY_EXISTS: i16 = 36;
pub const INVALID_PARTITIONS: i16 = 37;
pub const INVALID_REPLICATION_FACTOR: i16 = 38;
pub const NOT_CONTROLLER: i16 = 41;
pub const INVALID_REQUEST: i16 = 42;

// Phase 5 additions — group coordinator codes.
pub const ILLEGAL_GENERATION: i16 = 22;
pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
pub const UNKNOWN_MEMBER_ID: i16 = 25;
pub const REBALANCE_IN_PROGRESS: i16 = 27;
pub const MEMBER_ID_REQUIRED: i16 = 79;

// Phase 6 additions — idempotent-producer codes.
pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
pub const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
/// `INVALID_PRODUCER_EPOCH` (47) — per the canonical Apache Kafka error table.
/// Returned when the producer's epoch does not match the coordinator's current
/// epoch, OR when no transaction state exists for the given
/// (`transactional_id`, `producer_id`) pair. The Rust producer client maps
/// this code to `ProducerError::FencedProducer`.
pub const INVALID_PRODUCER_EPOCH: i16 = 47;
/// Alias used in handlers that check for an unknown (tid, pid) mapping.
/// Both cases produce error code 47 on the wire, matching Apache Kafka's
/// behavior (it uses `INVALID_PRODUCER_EPOCH` for all epoch/pid mismatches).
pub const INVALID_PRODUCER_ID_MAPPING: i16 = INVALID_PRODUCER_EPOCH;
pub const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 51;

// Phase 9 additions — transactional protocol codes.
pub const INVALID_TXN_STATE: i16 = 24;
pub const INVALID_TXN_TIMEOUT: i16 = 48;
pub const CONCURRENT_TRANSACTIONS: i16 = 49;
pub const TRANSACTION_COORDINATOR_FENCED: i16 = 50;
pub const STALE_MEMBER_EPOCH: i16 = 82;

/// Map an internal [`crate::error::BrokerError`] to a wire-level code.
/// Most internal errors map to `UNKNOWN_SERVER_ERROR`; specific variants
/// pick more meaningful codes.
#[must_use]
pub fn from_broker_error(err: &crate::error::BrokerError) -> i16 {
    use crate::error::BrokerError;
    match err {
        BrokerError::UnsupportedApi { .. } => UNSUPPORTED_VERSION,
        BrokerError::PartitionWriterDied { .. } => NOT_LEADER_OR_FOLLOWER,
        BrokerError::GroupInvalidState { .. } => REBALANCE_IN_PROGRESS,
        BrokerError::UnknownMember { .. } => UNKNOWN_MEMBER_ID,
        BrokerError::GenerationMismatch { .. } => ILLEGAL_GENERATION,
        BrokerError::ProducerEpochFenced { .. } => INVALID_PRODUCER_EPOCH,
        BrokerError::Replication(_)
        | BrokerError::Shutdown
        | BrokerError::Io(_)
        | BrokerError::Log(_)
        | BrokerError::Protocol(_)
        | BrokerError::Startup(_)
        | BrokerError::Txn(_) => UNKNOWN_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::BrokerError;

    #[test]
    fn maps_unsupported_to_35() {
        let e = BrokerError::UnsupportedApi {
            api_key: 0,
            version: 99,
        };
        assert_eq!(from_broker_error(&e), UNSUPPORTED_VERSION);
    }

    #[test]
    fn maps_writer_death_to_6() {
        let e = BrokerError::PartitionWriterDied {
            topic: "t".into(),
            partition: 0,
        };
        assert_eq!(from_broker_error(&e), NOT_LEADER_OR_FOLLOWER);
    }

    #[test]
    fn maps_group_invalid_state_to_27() {
        let e = BrokerError::GroupInvalidState {
            group_id: "g".into(),
            state: "PreparingRebalance".into(),
        };
        assert_eq!(from_broker_error(&e), REBALANCE_IN_PROGRESS);
    }

    #[test]
    fn maps_unknown_member_to_25() {
        let e = BrokerError::UnknownMember {
            group_id: "g".into(),
            member_id: "m".into(),
        };
        assert_eq!(from_broker_error(&e), UNKNOWN_MEMBER_ID);
    }

    #[test]
    fn maps_generation_mismatch_to_22() {
        let e = BrokerError::GenerationMismatch {
            group_id: "g".into(),
            current: 5,
            requested: 4,
        };
        assert_eq!(from_broker_error(&e), ILLEGAL_GENERATION);
    }

    #[test]
    fn maps_producer_epoch_fenced_to_47() {
        let e = BrokerError::ProducerEpochFenced {
            producer_id: 1000,
            current: 2,
            requested: 1,
        };
        assert_eq!(from_broker_error(&e), INVALID_PRODUCER_EPOCH);
        assert_eq!(from_broker_error(&e), 47);
    }

    #[test]
    fn txn_variant_maps_to_unknown_server_error() {
        let e = BrokerError::Txn("test".into());
        assert_eq!(from_broker_error(&e), UNKNOWN_SERVER_ERROR);
    }
}
