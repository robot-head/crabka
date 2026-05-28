// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files.
#![allow(clippy::pedantic)]

//! KIP-584 write-side surface — `ApiVersions` v3+ exposes the feature
//! surface the JVM admin tooling consumes. `supported_features` now
//! advertises `metadata.version` at a single conservative level (1 =
//! `3.0-IV1`, known to every KRaft-aware client >= 3.0), driven by the
//! broker-wide `features` table. A fresh broker has no `finalized_features`
//! and the epoch sits at the schema sentinel `-1` ("unknown"), which JVM
//! admin clients consume as `MetadataVersion.UNKNOWN` and short-circuit
//! per-level validation. Finalized features + a real (`>= 0`) epoch only
//! appear after `UpdateFeatures` (api_key 57) lands a `V1FeatureLevel`
//! record — that path is exercised in `tests/update_features.rs`.
//!
//! Advertising a `supported_features` entry whose `max_version` is above a
//! connecting client's known `MetadataVersion` enum, or a finalized
//! `metadata.version` entry with `finalized_features_epoch = 0`, breaks every
//! JVM admin client whose enum doesn't enumerate the level — it throws
//! `IllegalArgumentException` out of `MetadataVersion.fromFeatureLevel(N)` on
//! the first handshake (this took down 19 `broker-jvm-acceptance` tests
//! historically). Level 1 is the JVM-verified safe ceiling; this test guards
//! the fresh-broker surface.

#![cfg(not(target_os = "windows"))]

mod support;

use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;

#[tokio::test]
async fn v3_response_advertises_supported_metadata_version_no_finalized() {
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

    // KIP-584 write-side: supported_features advertises metadata.version at
    // the conservative level, but a fresh broker has no finalized features
    // and the epoch is the schema sentinel -1. See the module-level note for
    // the JVM compatibility rationale.
    let mv = resp
        .supported_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("metadata.version advertised in supported_features");
    assert_eq!(mv.min_version, 1, "{resp:?}");
    assert_eq!(mv.max_version, 1, "{resp:?}");
    assert!(
        resp.finalized_features.is_empty(),
        "fresh broker has no finalized features: {:?}",
        resp.finalized_features,
    );
    assert_eq!(
        resp.finalized_features_epoch, -1,
        "fresh broker epoch must be -1 until UpdateFeatures lands a record",
    );

    p.broker.shutdown().await;
}
