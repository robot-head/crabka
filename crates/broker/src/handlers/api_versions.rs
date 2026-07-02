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
    ApiVersionsResponse, FinalizedFeatureKey, SupportedFeatureKey,
};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

// KIP-584 feature surface. `supported_features` advertises `metadata.version`
// over the full Kafka-faithful range MIN=7 (3.3-IV3) .. MAX=25 (4.0-IV3),
// sourced from the `crabka_metadata::metadata_version` table via
// `crate::features`. `finalized_features` + the epoch are read from the live
// metadata image: a fresh (unformatted) broker surfaces no finalized features
// and epoch `-1` (`MetadataVersion.UNKNOWN` to JVM clients) until a
// `V1FeatureLevel` is seeded by `crabka format --release-version` or
// `UpdateFeatures` (api_key 57) lands one.

fn supported_feature_keys() -> Vec<SupportedFeatureKey> {
    crate::features::supported_features()
        .iter()
        .map(|f| SupportedFeatureKey {
            name: f.name.to_string(),
            // Kafka's wire invariant: `SupportedVersionRange` (and the JVM
            // client's `NodeApiVersions` parser) requires `minVersion >= 1`,
            // so the ApiVersions `SupportedFeatures` advertisement clamps the
            // min to 1. Features whose registry min is 0 (e.g. `group.version`,
            // `transaction.version`, where level 0 means "disabled") are still
            // *finalizable* at 0 via `UpdateFeatures` — 0 is only inexpressible
            // on this specific wire field. Advertising min=0 here makes a
            // pre-4.0 JVM admin client throw `IllegalArgumentException` and
            // fail the whole ApiVersions handshake; real cp-kafka 4.0 advertises
            // min=1 for the same reason.
            min_version: f.min_version.max(1),
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
            api_keys: crate::api_catalog::supported_apis(),
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
    use assert2::{assert, check};
    use crabka_metadata::{FeatureLevelRecord, MetadataRecord};
    use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;

    use crate::broker::Broker;
    use crate::config::BrokerConfig;

    const API_VERSIONS_V3: i16 = 3;

    fn request(name: &str, version: &str) -> Bytes {
        let req = ApiVersionsRequest {
            client_software_name: name.into(),
            client_software_version: version.into(),
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(req.encoded_len(API_VERSIONS_V3));
        req.encode(&mut buf, API_VERSIONS_V3)
            .expect("encode ApiVersionsRequest");
        buf.freeze()
    }

    fn decode_response(version: i16, bytes: &Bytes) -> ApiVersionsResponse {
        let mut cur: &[u8] = bytes.as_ref();
        let resp =
            ApiVersionsResponse::decode(&mut cur, version).expect("decode ApiVersionsResponse");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    async fn start_broker() -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
        let handle = Broker::start(cfg).await.expect("start broker");
        (handle, dir)
    }

    async fn wait_for_leader(broker: &Broker) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if broker
                .controller
                .watch_leader()
                .borrow()
                .is_some_and(|n| n == broker.config.node_id)
            {
                return;
            }
            assert!(
                std::time::Instant::now() <= deadline,
                "broker did not become controller leader"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    // ── KIP-584 feature surface ────────────────────────────────────────────

    #[test]
    fn supported_features_advertise_metadata_version() {
        let keys = supported_feature_keys();
        let mv = keys
            .iter()
            .find(|k| k.name == "metadata.version")
            .expect("metadata.version advertised");
        assert!(mv.min_version == crate::features::METADATA_VERSION_MIN);
        assert!(mv.max_version == crate::features::METADATA_VERSION_MAX);
    }

    #[test]
    fn fresh_image_surfaces_no_finalized_features() {
        // A fresh metadata image (no `UpdateFeatures` ever applied) has no
        // finalized features and the schema sentinel epoch `-1`, which JVM
        // clients consume as `MetadataVersion.UNKNOWN`.
        let image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        assert!(finalized_feature_keys(&image).is_empty());
        assert!(image.finalized_features_epoch() == -1);
    }

    #[test]
    fn finalized_feature_keys_preserve_names_and_levels() {
        let mut image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 24,
        }));
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "group.version".into(),
            level: 1,
        }));

        let keys = finalized_feature_keys(&image);

        assert!(keys.len() == 2, "{keys:?}");
        let mv = keys
            .iter()
            .find(|k| k.name == "metadata.version")
            .expect("metadata.version finalized");
        assert!(mv.max_version_level == 24, "{keys:?}");
        assert!(mv.min_version_level == 24, "{keys:?}");
        let gv = keys
            .iter()
            .find(|k| k.name == "group.version")
            .expect("group.version finalized");
        assert!(gv.max_version_level == 1, "{keys:?}");
        assert!(gv.min_version_level == 1, "{keys:?}");
    }

    #[test]
    fn api_versions_advertises_legacy_produce_and_fetch_min() {
        let table = crate::api_catalog::supported_apis();
        let produce = table.iter().find(|v| v.api_key == 0).expect("produce");
        let fetch = table.iter().find(|v| v.api_key == 1).expect("fetch");
        assert!(
            produce.min_version == 0,
            "Produce min must be 0 to advertise the legacy v0-2 support"
        );
        assert!(
            fetch.min_version == 0,
            "Fetch min must be 0 to advertise the legacy v0-3 support"
        );
    }

    #[test]
    fn api_versions_advertises_kip853_rpcs_and_describe_quorum_v2() {
        use crabka_protocol::owned;
        let table = crate::api_catalog::supported_apis();
        let by_key = |k: i16| table.iter().find(|v| v.api_key == k);

        for (key, max) in [
            (80i16, owned::add_raft_voter_request::MAX_VERSION),
            (81, owned::remove_raft_voter_request::MAX_VERSION),
            (82, owned::update_raft_voter_request::MAX_VERSION),
        ] {
            let v = by_key(key).unwrap_or_else(|| panic!("api_key {key} advertised"));
            assert!(v.min_version == 0);
            assert!(v.max_version == max, "api_key {key} max matches codegen");
        }

        // DescribeQuorum (55) max follows its schema const — now v2 (KIP-853
        // adds VoterDirectoryId + Nodes).
        let dq = by_key(55).expect("describe_quorum advertised");
        assert!(
            dq.max_version == owned::describe_quorum_request::MAX_VERSION,
            "DescribeQuorum max tracks the codegen const"
        );
        assert!(dq.max_version == 2, "DescribeQuorum is v2 after KIP-853");
    }

    #[tokio::test]
    async fn handle_rejects_each_invalid_v3_client_info_field() {
        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();

        for (name, version) in [("", "1.0.0"), ("crabka-test", "")] {
            let req = request(name, version);
            let bytes = handle(&broker, API_VERSIONS_V3, 7, &req)
                .await
                .expect("ApiVersions handler");
            let resp = decode_response(API_VERSIONS_V3, &bytes);
            assert!(resp.error_code == codes::INVALID_REQUEST, "{resp:?}");
            assert!(resp.api_keys.is_empty(), "{resp:?}");
        }

        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_accepts_legacy_request_without_client_info() {
        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let req = ApiVersionsRequest::default();
        let mut req_bytes = BytesMut::with_capacity(req.encoded_len(0));
        req.encode(&mut req_bytes, 0)
            .expect("encode legacy ApiVersionsRequest");

        let bytes = handle(&broker, 0, 7, &req_bytes)
            .await
            .expect("ApiVersions handler");
        let resp = decode_response(0, &bytes);

        assert!(resp.error_code == codes::NONE, "{resp:?}");
        assert!(!resp.api_keys.is_empty(), "{resp:?}");

        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_accepts_valid_v3_and_surfaces_catalog_and_features() {
        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_leader(&broker).await;
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: "metadata.version".into(),
                level: 24,
            })])
            .await
            .expect("submit finalized feature");
        let image = broker.controller.current_image();
        assert!(image.finalized_features_epoch() > 0);

        let req = request("crabka-test", "1.0.0");
        let bytes = handle(&broker, API_VERSIONS_V3, 7, &req)
            .await
            .expect("ApiVersions handler");
        let resp = decode_response(API_VERSIONS_V3, &bytes);

        check!(resp.error_code == codes::NONE, "{resp:?}");
        check!(
            resp.api_keys == crate::api_catalog::supported_apis(),
            "{resp:?}"
        );
        check!(!resp.supported_features.is_empty(), "{resp:?}");
        let mv = resp
            .supported_features
            .iter()
            .find(|f| f.name == "metadata.version")
            .expect("metadata.version supported");
        check!(mv.min_version == crate::features::METADATA_VERSION_MIN);
        check!(mv.max_version == crate::features::METADATA_VERSION_MAX);
        check!(resp.finalized_features_epoch == image.finalized_features_epoch());
        let finalized_mv = resp
            .finalized_features
            .iter()
            .find(|f| f.name == "metadata.version")
            .expect("metadata.version finalized");
        assert!(finalized_mv.max_version_level == 24, "{resp:?}");
        assert!(finalized_mv.min_version_level == 24, "{resp:?}");

        broker_handle.shutdown().await;
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
