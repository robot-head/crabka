//! Public catalog of the Kafka protocol APIs this broker advertises.
//!
//! Single source of truth for both the live `ApiVersions` (`api_key` 18)
//! response and the generated protocol-API reference page. The handler in
//! `handlers::api_versions` calls [`supported_apis`]; `crabka-docgen` reads
//! the same list without spawning the broker binary.

use crabka_protocol::owned::api_versions_response::ApiVersion;

macro_rules! v {
    ($mod:ident) => {
        ApiVersion {
            api_key: crabka_protocol::owned::$mod::API_KEY,
            min_version: crabka_protocol::owned::$mod::MIN_VERSION,
            max_version: crabka_protocol::owned::$mod::MAX_VERSION,
            ..Default::default()
        }
    };
}

/// The full advertised API set, mirrored from each API's generated
/// `MIN_VERSION`/`MAX_VERSION` constants. Update when adding a handler.
#[must_use]
pub fn supported_apis() -> Vec<ApiVersion> {
    let mut apis = client_facing_apis();
    apis.extend(admin_apis());
    apis
}

fn client_facing_apis() -> Vec<ApiVersion> {
    use crabka_protocol::owned;
    vec![
        v!(api_versions_request),
        ApiVersion {
            api_key: owned::produce_request::API_KEY,
            min_version: crabka_protocol::kafka_3_6_2::owned::produce_request::MIN_VERSION,
            max_version: owned::produce_request::MAX_VERSION,
            ..Default::default()
        },
        ApiVersion {
            api_key: owned::fetch_request::API_KEY,
            min_version: crabka_protocol::kafka_3_6_2::owned::fetch_request::MIN_VERSION,
            max_version: owned::fetch_request::MAX_VERSION,
            ..Default::default()
        },
        v!(list_offsets_request),
        v!(metadata_request),
        v!(find_coordinator_request),
        v!(join_group_request),
        v!(sync_group_request),
        v!(heartbeat_request),
        v!(leave_group_request),
        v!(sasl_handshake_request),
        v!(sasl_authenticate_request),
        // OffsetCommit and OffsetFetch only handle the legacy
        // single-group / name-keyed shape. v8+ (OffsetFetch) and v10+
        // (OffsetCommit) switch to topic_id / per-group arrays which
        // require a topic-id index that is not wired up. Cap the
        // advertised max so clients negotiate down to a version we can
        // serve cleanly.
        ApiVersion {
            api_key: owned::offset_commit_request::API_KEY,
            min_version: owned::offset_commit_request::MIN_VERSION,
            max_version: 9,
            ..Default::default()
        },
        ApiVersion {
            api_key: owned::offset_fetch_request::API_KEY,
            min_version: owned::offset_fetch_request::MIN_VERSION,
            max_version: 7,
            ..Default::default()
        },
    ]
}

fn admin_apis() -> Vec<ApiVersion> {
    vec![
        v!(create_topics_request),
        v!(delete_topics_request),
        v!(delete_records_request),
        v!(init_producer_id_request),
        v!(offset_for_leader_epoch_request),
        v!(add_partitions_to_txn_request),
        v!(add_offsets_to_txn_request),
        v!(end_txn_request),
        v!(write_txn_markers_request),
        v!(txn_offset_commit_request),
        v!(describe_configs_request),
        v!(alter_replica_log_dirs_request),
        v!(describe_log_dirs_request),
        v!(describe_groups_request),
        v!(list_groups_request),
        v!(alter_configs_request),
        v!(create_partitions_request),
        v!(delete_groups_request),
        v!(incremental_alter_configs_request),
        v!(alter_partition_request),
        v!(describe_cluster_request),
        v!(broker_heartbeat_request),
        // UnregisterBroker (KIP-185) — admin RPC to permanently drop a
        // broker registration from the cluster's metadata image.
        v!(unregister_broker_request),
        v!(alter_user_scram_credentials_request),
        // UpdateFeatures (api_key 57, KIP-584) — `kafka-features` admin tool
        // finalizes broker-supported features through a Raft-persisted path.
        v!(update_features_request),
        v!(describe_acls_request),
        v!(create_acls_request),
        v!(delete_acls_request),
        v!(elect_leaders_request),
        v!(alter_partition_reassignments_request),
        v!(list_partition_reassignments_request),
        // OffsetDelete (api_key 47, KIP-496): completes
        // `kafka-consumer-groups --delete-offsets` parity.
        v!(offset_delete_request),
        v!(describe_client_quotas_request),
        v!(alter_client_quotas_request),
        v!(describe_user_scram_credentials_request),
        // KIP-48: delegation-token RPCs. Conditional on the
        // broker having a master key configured is tempting, but Kafka
        // always advertises these — clients discover support at this
        // level then get DELEGATION_TOKEN_AUTH_DISABLED (61) on the
        // actual call when the broker isn't configured for tokens.
        v!(create_delegation_token_request),
        v!(renew_delegation_token_request),
        v!(expire_delegation_token_request),
        v!(describe_delegation_token_request),
        // DescribeProducers (KIP-664) — admin introspection of
        // per-(topic, partition) idempotent / transactional producer state.
        v!(describe_producers_request),
        // DescribeTransactions + ListTransactions (KIP-664) — admin
        // introspection of the TxnCoordinator's local state map.
        v!(describe_transactions_request),
        v!(list_transactions_request),
        // DescribeTopicPartitions (KIP-966) — paginated topic listing
        // used by JVM admin clients 3.7+ in place of fanned-out Metadata
        // calls for `kafka-topics --describe`.
        v!(describe_topic_partitions_request),
        // KIP-714 client-metrics push handshake. Crabka exposes its own
        // broker-side observability — these handlers return "no metrics
        // subscribed" so clients skip the push entirely. Advertising is
        // still important: clients query `ApiVersions` to learn the
        // broker supports the API at all, and absence flips them into
        // legacy-fallback paths we don't need.
        v!(get_telemetry_subscriptions_request),
        v!(push_telemetry_request),
        // ListConfigResources (KIP-1142) — typed enumeration of every
        // configurable resource (topics + brokers + client_metrics). v0
        // is the legacy ListClientMetricsResources surface (KIP-714); v1
        // adds the `resource_types` filter.
        v!(list_config_resources_request),
        // DescribeQuorum (KIP-595) — `kafka-metadata-quorum --describe`
        // admin introspection of the controller-raft quorum.
        v!(describe_quorum_request),
        // FetchSnapshot (KIP-630) — controller-snapshot byte-range fetch
        // used by replicas catching up via __cluster_metadata snapshots.
        v!(fetch_snapshot_request),
        // KIP-848 next-gen consumer group protocol.
        v!(consumer_group_heartbeat_request),
        v!(consumer_group_describe_request),
        // GetReplicaLogInfo (KIP-966) — inter-broker RPC the controller's
        // unclean recovery manager uses to read each replica's LEO + leader
        // epoch. Advertised so InterBrokerClient version negotiation succeeds.
        v!(get_replica_log_info_request),
        // KIP-853 dynamic-quorum reconfiguration — `kafka-metadata-quorum
        // --add-controller / --remove-controller` and the controller
        // auto-join path.
        v!(add_raft_voter_request),
        v!(remove_raft_voter_request),
        v!(update_raft_voter_request),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn supported_apis_is_nonempty_and_sane() {
        let apis = supported_apis();
        assert!(!apis.is_empty(), "advertised API table must not be empty");
        // ApiVersions itself (api_key 18) is always advertised.
        assert!(apis.iter().any(|a| a.api_key == 18));
        for a in &apis {
            assert!(a.min_version <= a.max_version, "api {} min>max", a.api_key);
        }
    }
}
