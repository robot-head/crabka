// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files.
#![allow(clippy::pedantic)]

//! KIP-584 read-side — `ApiVersions` v3+ exposes the feature surface
//! the JVM admin tooling consumes. Both feature lists stay empty and
//! the epoch sits at the schema sentinel `-1` ("unknown"), which JVM
//! admin clients consume as `MetadataVersion.UNKNOWN` and short-
//! circuit per-level validation.
//!
//! Populating either list before `UpdateFeatures` (api_key 57) lands
//! a Raft-persisted feature-transition path with a real epoch breaks
//! JVM admin tooling. The first push of this slice advertised a
//! finalized `metadata.version` entry with `finalized_features_epoch
//! = 0`; every JVM admin client whose `MetadataVersion` enum didn't
//! enumerate that level threw `IllegalArgumentException` out of
//! `MetadataVersion.fromFeatureLevel(N)` on the first handshake,
//! taking down 19 `broker-jvm-acceptance` tests (kafka-acls,
//! kafka-configs, etc.). Advertising a `supported_features` entry
//! whose `max_version` was above a connecting client's known
//! `MetadataVersion` enum hit the same wall. This test is the
//! regression guard.

#![cfg(not(target_os = "windows"))]

mod support;

use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;

#[tokio::test]
async fn v3_response_feature_surface_is_empty_with_unknown_epoch() {
    let p = support::start().await;

    let resp = p
        .client
        .send(ApiVersionsRequest {
            client_software_name: "crabka-test".into(),
            client_software_version: "0.0.0".into(),
            ..Default::default()
        })
        .await
        .expect("ApiVersions");

    assert_eq!(resp.error_code, 0, "{resp:?}");

    // KIP-584 read-side: both lists must stay empty and the epoch
    // must be the schema sentinel `-1` until UpdateFeatures lands.
    // See the module-level note for the JVM compatibility rationale.
    assert!(
        resp.supported_features.is_empty(),
        "supported_features must be empty until per-client-version negotiation lands: {:?}",
        resp.supported_features,
    );
    assert!(
        resp.finalized_features.is_empty(),
        "finalized_features must be empty until UpdateFeatures lands: {:?}",
        resp.finalized_features,
    );
    assert_eq!(
        resp.finalized_features_epoch, -1,
        "finalized_features_epoch must be -1 (`unknown`) until UpdateFeatures lands",
    );

    p.broker.shutdown().await;
}
