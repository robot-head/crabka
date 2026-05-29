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

// KIP-584 feature surface. `supported_features` is advertised from the
// broker-wide `crate::features` table (currently `metadata.version` at a
// single conservative level). `finalized_features` + the epoch are read from
// the live metadata image: a fresh broker (no `UpdateFeatures` ever applied)
// surfaces no finalized features and the schema sentinel epoch `-1`
// ("unknown"), which JVM admin clients consume as `MetadataVersion.UNKNOWN`
// and short-circuit per-level validation. `UpdateFeatures` (api_key 57) lands
// a Raft-persisted `V1FeatureLevel` record, after which the finalized list and
// a real (`>= 0`) epoch appear here.

fn supported_feature_keys() -> Vec<SupportedFeatureKey> {
    crate::features::supported_features()
        .iter()
        .map(|f| SupportedFeatureKey {
            name: f.name.to_string(),
            min_version: f.min_version,
            max_version: f.max_version,
            ..Default::default()
        })
        .collect()
}

fn finalized_feature_keys(image: &crabka_metadata::MetadataImage) -> Vec<FinalizedFeatureKey> {
    image
        .finalized_features()
        .iter()
        .map(|(name, level)| FinalizedFeatureKey {
            name: name.clone(),
            // Kafka reports the finalized level as both the min and max
            // finalized version level.
            max_version_level: *level,
            min_version_level: *level,
            ..Default::default()
        })
        .collect()
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
    let image = broker.controller.current_image();
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
            // KIP-584 write-side. `supported_features` advertises the
            // broker's `crate::features` table; `finalized_features` + the
            // epoch are read from the live metadata image. A fresh broker
            // surfaces no finalized features and epoch `-1`
            // (`MetadataVersion.UNKNOWN` to JVM clients) until
            // `UpdateFeatures` (api_key 57) lands a `V1FeatureLevel` record.
            supported_features: supported_feature_keys(),
            finalized_features_epoch: image.finalized_features_epoch(),
            finalized_features: finalized_feature_keys(&image),
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
    fn supported_features_advertise_metadata_version() {
        let keys = supported_feature_keys();
        let mv = keys
            .iter()
            .find(|k| k.name == "metadata.version")
            .expect("metadata.version advertised");
        assert_eq!(mv.min_version, 1);
        assert_eq!(mv.max_version, crate::features::METADATA_VERSION_MAX);
    }

    #[test]
    fn fresh_image_surfaces_no_finalized_features() {
        // A fresh metadata image (no `UpdateFeatures` ever applied) has no
        // finalized features and the schema sentinel epoch `-1`, which JVM
        // clients consume as `MetadataVersion.UNKNOWN`.
        let image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        assert!(finalized_feature_keys(&image).is_empty());
        assert_eq!(image.finalized_features_epoch(), -1);
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

    #[test]
    fn api_versions_advertises_kip853_rpcs_and_describe_quorum_v2() {
        use crabka_protocol::owned;
        let table = supported_apis();
        let by_key = |k: i16| table.iter().find(|v| v.api_key == k);

        for (key, max) in [
            (80i16, owned::add_raft_voter_request::MAX_VERSION),
            (81, owned::remove_raft_voter_request::MAX_VERSION),
            (82, owned::update_raft_voter_request::MAX_VERSION),
        ] {
            let v = by_key(key).unwrap_or_else(|| panic!("api_key {key} advertised"));
            assert_eq!(v.min_version, 0);
            assert_eq!(v.max_version, max, "api_key {key} max matches codegen");
        }

        // DescribeQuorum (55) max follows its schema const — now v2 (KIP-853
        // adds VoterDirectoryId + Nodes).
        let dq = by_key(55).expect("describe_quorum advertised");
        assert_eq!(
            dq.max_version,
            owned::describe_quorum_request::MAX_VERSION,
            "DescribeQuorum max tracks the codegen const",
        );
        assert_eq!(dq.max_version, 2, "DescribeQuorum is v2 after KIP-853");
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
