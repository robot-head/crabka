//! Kafka wire-level error codes used in this MVP.
//!
//! Per-(topic, partition) response fields use these `i16` values.
//! JVM clients react to specific codes, so substituting them changes
//! client behavior — values here mirror the canonical Apache Kafka
//! table.

#![allow(dead_code)] // codes are consumed by handlers as APIs are enabled.

pub const NONE: i16 = 0;
pub const UNKNOWN_SERVER_ERROR: i16 = -1;
pub const OFFSET_OUT_OF_RANGE: i16 = 1;
/// `CORRUPT_MESSAGE` (2) — the broker received a record batch whose bytes
/// are malformed or whose CRC/magic does not match any supported format.
/// Corresponds to `CORRUPT_MESSAGE` in the Apache Kafka error table.
pub const CORRUPT_MESSAGE: i16 = 2;
pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub const INVALID_FETCH_SIZE: i16 = 4;
pub const LEADER_NOT_AVAILABLE: i16 = 5;
pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
pub const REQUEST_TIMED_OUT: i16 = 7;
/// `REPLICA_NOT_AVAILABLE` (11, KIP-113) — the targeted replica is not
/// hosted on this broker. Used by `AlterReplicaLogDirs` when the client
/// names a `(topic, partition)` the broker doesn't own.
pub const REPLICA_NOT_AVAILABLE: i16 = 11;
/// `KAFKA_STORAGE_ERROR` (56, KIP-113) — a log-dir-level I/O failure
/// (open, rename, remove) or a concurrent move with a conflicting target.
pub const KAFKA_STORAGE_ERROR: i16 = 56;
/// `LOG_DIR_NOT_FOUND` (57, KIP-113) — the destination directory in an
/// `AlterReplicaLogDirs` request is not one of this broker's configured
/// `log.dirs`.
pub const LOG_DIR_NOT_FOUND: i16 = 57;
pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
/// `COORDINATOR_LOAD_IN_PROGRESS` (14, KIP-848) — the group coordinator is
/// still loading state from `__consumer_offsets`; clients should retry after
/// a brief back-off.
pub const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
pub const NOT_COORDINATOR: i16 = 16;
pub const INVALID_TOPIC_EXCEPTION: i16 = 17;
/// `ILLEGAL_SASL_STATE` (34) — request received on a SASL listener before
/// the connection has completed `SaslHandshake` + `SaslAuthenticate`, or in
/// the wrong order. The broker closes the connection after emitting it.
pub const ILLEGAL_SASL_STATE: i16 = 34;
/// `UNSUPPORTED_SASL_MECHANISM` (33) — the client requested a SASL mechanism
/// the broker does not offer on this listener.
pub const UNSUPPORTED_SASL_MECHANISM: i16 = 33;
pub const UNSUPPORTED_VERSION: i16 = 35;
pub const TOPIC_ALREADY_EXISTS: i16 = 36;
pub const INVALID_PARTITIONS: i16 = 37;
pub const INVALID_REPLICATION_FACTOR: i16 = 38;
pub const NOT_CONTROLLER: i16 = 41;
pub const INVALID_REQUEST: i16 = 42;
/// Kafka error 87. Returned when a Produce payload is structurally
/// malformed — e.g. a legacy v0/v1 `MessageSet` that fails CRC, has
/// nested compression, or otherwise can't be parsed into v2 records.
pub const INVALID_RECORD: i16 = 87;

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
pub const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;

// Phase 9 additions — transactional protocol codes.
pub const INVALID_TXN_STATE: i16 = 24;
pub const INVALID_TXN_TIMEOUT: i16 = 48;
pub const CONCURRENT_TRANSACTIONS: i16 = 49;
pub const TRANSACTION_COORDINATOR_FENCED: i16 = 50;
/// `TRANSACTION_ABORTABLE` (120, KIP-890) — the operation failed but the
/// transaction can still be aborted by the client; e.g. `AddPartitionsToTxn`
/// verify-only found a partition that is not part of the ongoing transaction.
pub const TRANSACTION_ABORTABLE: i16 = 120;

/// `FENCED_INSTANCE_ID` (82, KIP-345) — another client is currently pinned
/// to the same `group.instance.id`. The losing client must exit; the broker
/// fences it across `JoinGroup`, `SyncGroup`, `Heartbeat`, `OffsetCommit`,
/// `TxnOffsetCommit`, and `LeaveGroup`.
pub const FENCED_INSTANCE_ID: i16 = 82;

/// `STALE_MEMBER_EPOCH` (113, KIP-848) — the supplied member epoch is older
/// than the coordinator's current epoch for the consumer group member.
pub const STALE_MEMBER_EPOCH: i16 = 113;
/// `FENCED_MEMBER_EPOCH` (110, KIP-848) — the supplied member epoch is
/// newer than the coordinator's; the consumer must rejoin from epoch 0.
pub const FENCED_MEMBER_EPOCH: i16 = 110;
/// `UNSUPPORTED_ASSIGNOR` (111, KIP-848) — the requested `server_assignor`
/// is not enabled on this broker.
pub const UNSUPPORTED_ASSIGNOR: i16 = 111;
/// `UNRELEASED_INSTANCE_ID` (114, KIP-848 + KIP-345) — the static
/// `instance_id` is still bound to a live member of the group.
pub const UNRELEASED_INSTANCE_ID: i16 = 114;
/// `UNKNOWN_SUBSCRIPTION_ID` (117, KIP-848) — the consumer's persisted
/// subscription identifier was not found by the coordinator.
pub const UNKNOWN_SUBSCRIPTION_ID: i16 = 117;

/// KIP-932: an acknowledgement targeted a record no longer Acquired.
pub const INVALID_RECORD_STATE: i16 = 121;
/// KIP-932: the named share session does not exist.
pub const SHARE_SESSION_NOT_FOUND: i16 = 122;
/// KIP-932: the share session epoch did not match the broker's expectation.
pub const INVALID_SHARE_SESSION_EPOCH: i16 = 123;
/// KIP-932: the share coordinator fenced a write on a stale state epoch.
pub const FENCED_STATE_EPOCH: i16 = 124;
/// KIP-932: the per-broker share session cache is full.
pub const SHARE_SESSION_LIMIT_REACHED: i16 = 133;

// Admin handler codes.
/// `INVALID_CONFIG` (40) — a config key/value pair is invalid or unknown.
pub const INVALID_CONFIG: i16 = 40;
/// `NON_EMPTY_GROUP` (68) — group still has live members; cannot be deleted.
pub const NON_EMPTY_GROUP: i16 = 68;
/// `GROUP_ID_NOT_FOUND` (69) — no group with the given id exists.
pub const GROUP_ID_NOT_FOUND: i16 = 69;
/// `GROUP_SUBSCRIBED_TO_TOPIC` (86, KIP-496) — `OffsetDelete` refused
/// because the (live, consumer-protocol) group still subscribes to the
/// topic. The operator must stop consumers first.
pub const GROUP_SUBSCRIBED_TO_TOPIC: i16 = 86;
/// `INVALID_RESOURCE_TYPE` — alias for `INVALID_REQUEST` (42); the Kafka
/// protocol does not assign a distinct wire code for unsupported resource
/// types; `INVALID_REQUEST` is the correct response.
pub const INVALID_RESOURCE_TYPE: i16 = INVALID_REQUEST;

// `AlterUserScramCredentials` (KIP-554) result codes.
/// `CLUSTER_AUTHORIZATION_FAILED` (31) — principal lacks cluster-level
/// authorization. Used as a stand-in for ACL by the
/// `AlterUserScramCredentials` handler when the request principal is not the
/// configured super-user.
pub const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
/// `RESOURCE_NOT_FOUND` (66) — per-user error when a deletion targets a
/// credential that does not exist in the metadata image.
pub const RESOURCE_NOT_FOUND: i16 = 66;
/// `RESOURCE_NOT_FOUND_USER` (83) — per-user error returned by
/// `DescribeUserScramCredentials` when the requested user has no SCRAM
/// credentials in the metadata image. Apache Kafka uses error code 83 for
/// this case (distinct from the deletion-target-missing code 66).
pub const RESOURCE_NOT_FOUND_USER: i16 = 83;
/// `UNACCEPTABLE_CREDENTIAL` (78) — per-user error when an upsertion carries
/// invalid SCRAM parameters (iterations < 4096, empty salt, `salted_password`
/// of the wrong length, or an unknown mechanism). Canonical Apache Kafka
/// assigns code 78 to this error; note that code 74 is already taken by
/// `FENCED_LEADER_EPOCH` — `78` is correct.
pub const UNACCEPTABLE_CREDENTIAL: i16 = 78;
/// `DUPLICATE_RESOURCE` (84) — per-user error when the same
/// `(user, mechanism)` appears twice in one `AlterUserScramCredentials`
/// request (either two upsertions, two deletions, or one of each).
pub const DUPLICATE_RESOURCE: i16 = 84;

/// `INVALID_UPDATE_VERSION` (95, KIP-584) — a feature-level update in
/// `UpdateFeatures` is outside the broker's supported range, or attempts an
/// unguarded downgrade / deletion of a finalized feature.
pub const INVALID_UPDATE_VERSION: i16 = 95;

/// `FEATURE_UPDATE_FAILED` (96, KIP-584) — the cluster failed to persist a
/// validated feature update (e.g., the metadata write to Raft was rejected
/// or timed out).
pub const FEATURE_UPDATE_FAILED: i16 = 96;

// ACL authorization codes.
/// `TOPIC_AUTHORIZATION_FAILED` (29) — principal lacks permission on the topic.
pub const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
/// `GROUP_AUTHORIZATION_FAILED` (30) — principal lacks permission on the group.
pub const GROUP_AUTHORIZATION_FAILED: i16 = 30;
/// `OPERATION_NOT_ATTEMPTED` (55) — returned for partitions/resources whose
/// authorization check was short-circuited by an earlier error in the same
/// request (e.g. when a prior resource already failed with an auth error).
pub const OPERATION_NOT_ATTEMPTED: i16 = 55;

// Bulletproof EOS / acks=all codes.
/// Per-partition error returned by `acks=all` Produce when the request
/// completes without enough in-sync replicas confirming the write. The
/// record is durably on the leader's log; the producer should retry.
pub const NOT_ENOUGH_REPLICAS: i16 = 19;

/// Per-partition error returned by `acks=all` Produce when the request
/// appended successfully on the leader but the HW timeout elapsed before
/// enough in-sync replicas confirmed the write. The record is durably on
/// the leader's log but not yet on every ISR follower.
pub const NOT_ENOUGH_REPLICAS_AFTER_APPEND: i16 = 20;

/// KIP-101 fence: caller's `current_leader_epoch` is older than the
/// partition's current `leader_epoch`. Caller should re-fetch metadata
/// or call `OffsetForLeaderEpoch` to learn the truncation point.
pub const FENCED_LEADER_EPOCH: i16 = 74;

/// KIP-101: caller's `current_leader_epoch` is newer than the broker's
/// view. Metadata propagation lag — caller retries after a brief wait.
pub const UNKNOWN_LEADER_EPOCH: i16 = 75;

/// `INELIGIBLE_REPLICA` (92, KIP-903) — an `AlterPartition` proposed a new
/// ISR containing at least one ineligible replica: a broker not currently
/// registered, or one whose stamped broker epoch is stale relative to the
/// controller's registration epoch. The partition's ISR is left unchanged.
pub const INELIGIBLE_REPLICA: i16 = 92;

// Leader election codes.
pub const PREFERRED_LEADER_NOT_AVAILABLE: i16 = 80;
pub const ELIGIBLE_LEADERS_NOT_AVAILABLE: i16 = 81;
pub const ELECTION_NOT_NEEDED: i16 = 84;

// Partition reassignment codes (KIP-455).
pub const INVALID_REPLICA_ASSIGNMENT: i16 = 39;
pub const NO_REASSIGNMENT_IN_PROGRESS: i16 = 85;

// KIP-227 incremental-fetch-session codes.
/// Returned at the top level of a `FetchResponse` when the request carried
/// a non-zero `session_id` that is not present in the broker's session cache
/// (evicted, never existed, or already closed).
pub const FETCH_SESSION_ID_NOT_FOUND: i16 = 70;
/// Returned at the top level of a `FetchResponse` when the request's
/// `session_epoch` does not match the cached session's current epoch, or
/// when `session_id == 0` and `session_epoch` is neither `0` (new session)
/// nor `-1` (sessionless full fetch).
pub const INVALID_FETCH_SESSION_EPOCH: i16 = 71;

// KIP-48 delegation-token codes. Numbers from
// org.apache.kafka.common.protocol.Errors. Note the existing
// ELIGIBLE_LEADERS_NOT_AVAILABLE = 81 is incorrect (Kafka says 83);
// flagged for a separate fix.
pub const DELEGATION_TOKEN_AUTH_DISABLED: i16 = 61;
pub const DELEGATION_TOKEN_NOT_FOUND: i16 = 62;
pub const DELEGATION_TOKEN_OWNER_MISMATCH: i16 = 63;
pub const DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: i16 = 64;
pub const DELEGATION_TOKEN_AUTHORIZATION_FAILED: i16 = 65;
pub const DELEGATION_TOKEN_EXPIRED: i16 = 66;

// KIP-630 FetchSnapshot (api_key 59) codes.
/// `SNAPSHOT_NOT_FOUND` (98) — the requested `__cluster_metadata` snapshot
/// does not exist (the controller has not generated one yet).
pub const SNAPSHOT_NOT_FOUND: i16 = 98;
/// `POSITION_OUT_OF_RANGE` (99) — the requested `position` is past the end
/// of the `__cluster_metadata` snapshot.
pub const POSITION_OUT_OF_RANGE: i16 = 99;
/// `INCONSISTENT_CLUSTER_ID` (104) — the request's `cluster_id` does not
/// match this cluster's id.
pub const INCONSISTENT_CLUSTER_ID: i16 = 104;
/// `UNKNOWN_TOPIC_ID` (100) — a request referenced a topic by UUID that this
/// cluster does not know about (KIP-516).
pub const UNKNOWN_TOPIC_ID: i16 = 100;
/// `INCONSISTENT_TOPIC_ID` (103) — a request supplied a topic UUID that does
/// not match the UUID stored for the named topic (KIP-516).
pub const INCONSISTENT_TOPIC_ID: i16 = 103;
/// `FETCH_SESSION_TOPIC_ID_ERROR` (106) — a fetch session referenced a topic
/// UUID that no longer resolves (e.g. recreated mid-session) (KIP-516).
pub const FETCH_SESSION_TOPIC_ID_ERROR: i16 = 106;

/// `UNSUPPORTED_COMPRESSION_TYPE` (76) — KIP-714 `PushTelemetry` carried a
/// `compression_type` the broker can't decompress.
pub const UNSUPPORTED_COMPRESSION_TYPE: i16 = 76;

/// `THROTTLING_QUOTA_EXCEEDED` (89) — KIP-714 client pushed/fetched
/// telemetry faster than the configured interval allows.
pub const THROTTLING_QUOTA_EXCEEDED: i16 = 89;

/// `TELEMETRY_TOO_LARGE` (118) — KIP-714 `PushTelemetry` payload exceeded
/// `telemetry.max.bytes`.
pub const TELEMETRY_TOO_LARGE: i16 = 118;

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
        BrokerError::FencedLeaderEpoch { .. } => FENCED_LEADER_EPOCH,
        BrokerError::UnknownLeaderEpoch(_) => UNKNOWN_LEADER_EPOCH,
        BrokerError::Replication(_)
        | BrokerError::Shutdown
        | BrokerError::Io(_)
        | BrokerError::Log(_)
        | BrokerError::Protocol(_)
        | BrokerError::Startup(_)
        | BrokerError::Txn(_)
        | BrokerError::Share(_)
        | BrokerError::ListenerConflict { .. }
        | BrokerError::InvalidInterBrokerListener { .. }
        | BrokerError::EmptyRoles
        | BrokerError::NonControllerIsVoter { .. }
        | BrokerError::SaslListenerNoMechanisms { .. }
        | BrokerError::GssapiConfigMissing
        | BrokerError::Tls(_)
        | BrokerError::BootstrapFile { .. }
        | BrokerError::InvalidLeaderRebalanceInterval { .. }
        | BrokerError::InvalidLeaderRebalanceThreshold { .. }
        | BrokerError::ShutdownTimeout(_) => UNKNOWN_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::error::BrokerError;

    #[test]
    fn share_group_error_codes_match_kafka() {
        let cases = [
            ("INVALID_RECORD_STATE", INVALID_RECORD_STATE, 121),
            ("SHARE_SESSION_NOT_FOUND", SHARE_SESSION_NOT_FOUND, 122),
            (
                "INVALID_SHARE_SESSION_EPOCH",
                INVALID_SHARE_SESSION_EPOCH,
                123,
            ),
            ("FENCED_STATE_EPOCH", FENCED_STATE_EPOCH, 124),
            (
                "SHARE_SESSION_LIMIT_REACHED",
                SHARE_SESSION_LIMIT_REACHED,
                133,
            ),
        ];
        for (name, code, want) in cases {
            assert!(code == want, "{name}");
        }
    }

    #[test]
    fn unknown_server_error_is_negative_one() {
        assert!(UNKNOWN_SERVER_ERROR == -1);
        assert!(UNKNOWN_SERVER_ERROR < NONE);
    }

    #[test]
    fn from_broker_error_maps_variants_to_wire_codes() {
        let cases = [
            (
                BrokerError::UnsupportedApi {
                    api_key: 0,
                    version: 99,
                },
                UNSUPPORTED_VERSION, // 35
            ),
            (
                BrokerError::PartitionWriterDied {
                    topic: "t".into(),
                    partition: 0,
                },
                NOT_LEADER_OR_FOLLOWER, // 6
            ),
            (
                BrokerError::GroupInvalidState {
                    group_id: "g".into(),
                    state: "PreparingRebalance".into(),
                },
                REBALANCE_IN_PROGRESS, // 27
            ),
            (
                BrokerError::UnknownMember {
                    group_id: "g".into(),
                    member_id: "m".into(),
                },
                UNKNOWN_MEMBER_ID, // 25
            ),
            (
                BrokerError::GenerationMismatch {
                    group_id: "g".into(),
                    current: 5,
                    requested: 4,
                },
                ILLEGAL_GENERATION, // 22
            ),
            (
                BrokerError::ProducerEpochFenced {
                    producer_id: 1000,
                    current: 2,
                    requested: 1,
                },
                INVALID_PRODUCER_EPOCH, // 47
            ),
            (
                BrokerError::FencedLeaderEpoch {
                    have: 0,
                    current: 1,
                },
                FENCED_LEADER_EPOCH, // 74
            ),
            (BrokerError::UnknownLeaderEpoch(2), UNKNOWN_LEADER_EPOCH), // 75
            // Catch-all arm: internal variants map to the generic code.
            (BrokerError::Txn("test".into()), UNKNOWN_SERVER_ERROR), // -1
        ];
        for (err, want) in cases {
            assert!(from_broker_error(&err) == want, "{err:?}");
        }
        // Pin the concrete wire value for the producer-fence path: the Rust
        // producer client maps 47 to `ProducerError::FencedProducer`.
        assert!(INVALID_PRODUCER_EPOCH == 47);
    }

    #[test]
    fn not_enough_replicas_codes_have_expected_values() {
        assert!(NOT_ENOUGH_REPLICAS == 19);
        assert!(NOT_ENOUGH_REPLICAS_AFTER_APPEND == 20);
    }

    #[test]
    fn kip516_error_code_numbers_match_kafka() {
        let cases = [
            ("UNKNOWN_TOPIC_ID", super::UNKNOWN_TOPIC_ID, 100),
            ("INCONSISTENT_TOPIC_ID", super::INCONSISTENT_TOPIC_ID, 103),
            (
                "FETCH_SESSION_TOPIC_ID_ERROR",
                super::FETCH_SESSION_TOPIC_ID_ERROR,
                106,
            ),
        ];
        for (name, code, want) in cases {
            assert!(code == want, "{name}");
        }
    }
}
