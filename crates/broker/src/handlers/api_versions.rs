//! `ApiVersions` (`api_key=18`). Returns the (min, max) supported version
//! range for every API key this broker handles.
//!
//! KIP-511 (v3+): the request carries `client_software_name` and
//! `client_software_version`. The broker validates both against
//! `[a-zA-Z0-9](?:[a-zA-Z0-9\-.]*[a-zA-Z0-9])?` and rejects the call with
//! `INVALID_REQUEST` if either is empty or malformed (mirrors
//! `ApiVersionsRequest.isValid` on the JVM). Accepted v3+ handshakes
//! bump a per-(name, version) Prometheus counter
//! (`crabka_broker_client_software_versions_total`) so operators can see
//! which client libraries are connecting.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::Decode;
use crabka_protocol::Encode;
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::api_versions_response::{
    ApiVersion, ApiVersionsResponse, FinalizedFeatureKey, SupportedFeatureKey,
};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

/// KIP-584 finalized-features epoch. JVM clients treat values `>= 0`
/// as authoritative feature-level state and call
/// `MetadataVersion.fromFeatureLevel(N)` for every finalized level —
/// which throws `IllegalArgumentException` on any client whose
/// `MetadataVersion` enum doesn't enumerate `N` (breaks
/// `kafka-acls`, `kafka-configs`, and every other JVM admin tool
/// that handshakes `ApiVersions` first). `-1` is the schema sentinel
/// for "unknown / no finalized features"; JVM clients fall back to
/// `MetadataVersion.UNKNOWN` and skip per-level validation. We sit
/// at `-1` until `UpdateFeatures` (`api_key` 57) lands a Raft-
/// persisted feature transition path with a real epoch.
const FINALIZED_FEATURES_EPOCH: i64 = -1;

fn supported_feature_keys() -> Vec<SupportedFeatureKey> {
    // Empty until we either (a) ship at least one feature whose
    // numeric level is in every JVM client we test against
    // (cp-kafka 3.1/6.1/7.5, apache/kafka 4.0), or (b) wire a
    // per-client-version negotiation path. Advertising
    // `metadata.version` with a max above what the connecting
    // client knows broke 19 `broker-jvm-acceptance` tests on the
    // first attempt — see `tests/api_versions_features.rs` for
    // the regression note.
    Vec::new()
}

fn finalized_feature_keys() -> Vec<FinalizedFeatureKey> {
    // No finalized features advertised until `UpdateFeatures`
    // (`api_key` 57) lands. See `FINALIZED_FEATURES_EPOCH` for
    // the rationale.
    Vec::new()
}

/// Static table mirrored from each API's generated `MIN_VERSION`/`MAX_VERSION`
/// constants. Update this when adding a handler.
fn supported_apis() -> Vec<ApiVersion> {
    use crabka_protocol::owned;
    macro_rules! v {
        ($mod:ident) => {
            ApiVersion {
                api_key: owned::$mod::API_KEY,
                min_version: owned::$mod::MIN_VERSION,
                max_version: owned::$mod::MAX_VERSION,
                ..Default::default()
            }
        };
    }
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
        // OffsetCommit and OffsetFetch: MVP only handles the legacy
        // single-group / name-keyed shape. v8+ (OffsetFetch) and v10+
        // (OffsetCommit) switch to topic_id / per-group arrays which
        // require a topic-id index this slice doesn't wire up. Cap the
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
        // Slice 51 (KIP-48): delegation-token RPCs. Conditional on the
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
        // KIP-848 next-gen consumer group protocol.
        v!(consumer_group_heartbeat_request),
        v!(consumer_group_describe_request),
    ]
}

/// KIP-511 client-information validity check. Matches the JVM
/// `ApiVersionsRequest.isValid` regex
/// `[a-zA-Z0-9](?:[a-zA-Z0-9\-.]*[a-zA-Z0-9])?`:
///
/// - non-empty
/// - first and last chars are `[a-zA-Z0-9]`
/// - interior chars are `[a-zA-Z0-9\-.]`
///
/// A single alphanumeric char is valid (the optional middle group lets the
/// first and last char coincide). Implemented as a byte scan rather than
/// a `regex` dependency — every Kafka-client name in the wild stays within
/// ASCII so we don't need full UTF-8 char-class semantics.
#[must_use]
pub(crate) fn is_valid_client_info(s: &str) -> bool {
    let bytes = s.as_bytes();
    let is_alnum = |b: u8| b.is_ascii_alphanumeric();
    let is_interior = |b: u8| b.is_ascii_alphanumeric() || b == b'-' || b == b'.';
    match bytes.len() {
        0 => false,
        1 => is_alnum(bytes[0]),
        n => {
            is_alnum(bytes[0])
                && is_alnum(bytes[n - 1])
                && bytes[1..n - 1].iter().all(|&b| is_interior(b))
        }
    }
}

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let metrics = broker.metrics.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = ApiVersionsRequest::decode(&mut cur, version)?;

        // KIP-511: validate client-info fields on v3+. The codegen
        // leaves both as empty strings on earlier versions, so the
        // check would always fire — gate it on the version range that
        // actually carries the fields. On reject, return a degraded
        // response (error code, empty api_keys); clients are expected
        // to retry with a fixed name/version or give up.
        if version >= 3
            && (!is_valid_client_info(&req.client_software_name)
                || !is_valid_client_info(&req.client_software_version))
        {
            let resp = ApiVersionsResponse {
                error_code: codes::INVALID_REQUEST,
                api_keys: Vec::new(),
                throttle_time_ms: 0,
                ..Default::default()
            };
            let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
            resp.encode(&mut buf, version)?;
            return Ok(buf.freeze());
        }

        // Accepted handshake. Bump the per-(name, version) counter on
        // v3+ only; older requests don't carry the fields.
        if version >= 3 {
            metrics.record_client_software(&req.client_software_name, &req.client_software_version);
        }

        let resp = ApiVersionsResponse {
            error_code: codes::NONE,
            api_keys: supported_apis(),
            throttle_time_ms: 0,
            // KIP-584 read-side. Both feature lists stay empty and
            // the epoch is the schema sentinel `-1` until
            // `UpdateFeatures` (api_key 57) lands. JVM admin clients
            // read this as `MetadataVersion.UNKNOWN` and skip
            // per-level validation. Populating either list ahead of
            // a real Raft-tracked epoch breaks every JVM admin tool
            // whose `MetadataVersion` enum doesn't enumerate the
            // advertised level — see `tests/api_versions_features.rs`.
            supported_features: supported_feature_keys(),
            finalized_features_epoch: FINALIZED_FEATURES_EPOCH,
            finalized_features: finalized_feature_keys(),
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── KIP-584 feature surface ────────────────────────────────────────────

    #[test]
    fn feature_surface_is_empty_with_unknown_epoch() {
        // KIP-584 read-side: until `UpdateFeatures` (api_key 57)
        // lands a Raft-persisted feature transition path, both
        // feature lists stay empty and the epoch sits at the
        // schema sentinel `-1` (consumed by JVM clients as
        // `MetadataVersion.UNKNOWN`). Populating either list
        // without a real epoch breaks JVM admin tooling — see the
        // module-level note on `FINALIZED_FEATURES_EPOCH`.
        assert!(supported_feature_keys().is_empty());
        assert!(finalized_feature_keys().is_empty());
        assert_eq!(FINALIZED_FEATURES_EPOCH, -1);
    }

    #[test]
    fn api_versions_advertises_legacy_produce_and_fetch_min() {
        let table = supported_apis();
        let produce = table.iter().find(|v| v.api_key == 0).expect("produce");
        let fetch = table.iter().find(|v| v.api_key == 1).expect("fetch");
        assert_eq!(
            produce.min_version, 0,
            "Produce min must be 0 to advertise the legacy v0-2 support"
        );
        assert_eq!(
            fetch.min_version, 0,
            "Fetch min must be 0 to advertise the legacy v0-3 support"
        );
    }

    // ── KIP-511 client-info validation ─────────────────────────────────────

    #[test]
    fn valid_client_info_accepts_typical_names() {
        for s in [
            "apache-kafka-java",
            "crabka-client-core",
            "librdkafka",
            "kafka-python",
            "node-rdkafka",
            "Sarama",
            "3.6.2",
            "0.0.0",
            "1.0.0-SNAPSHOT",
            "a", // single alnum char — allowed
            "1.2.3.4",
        ] {
            assert!(is_valid_client_info(s), "{s:?} should be valid");
        }
    }

    #[test]
    fn valid_client_info_rejects_empty() {
        assert!(!is_valid_client_info(""));
    }

    #[test]
    fn valid_client_info_rejects_leading_or_trailing_special() {
        for s in ["-leading", "trailing-", ".dotstart", "dotend.", "-only-"] {
            assert!(!is_valid_client_info(s), "{s:?} should be rejected");
        }
    }

    #[test]
    fn valid_client_info_rejects_disallowed_interior_chars() {
        for s in [
            "has space",
            "has/slash",
            "has\\backslash",
            "has;semi",
            "has@at",
            "has(paren)",
            "has\"quote",
            "café", // non-ASCII alphanumeric — KIP-511 regex is ASCII-only
        ] {
            assert!(!is_valid_client_info(s), "{s:?} should be rejected");
        }
    }
}
