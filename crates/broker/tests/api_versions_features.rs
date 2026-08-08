// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files.

//! The KIP-584 write-side surface. `ApiVersions` v3 and above expose the
//! feature surface that the JVM admin tooling reads. `supported_features`
//! advertises `metadata.version` over the supported range `min = 7`
//! (`3.3-IV3`) to `max = 25` (`4.0-IV3`), from the broker-wide `features`
//! table.
//!
//! A standalone, self-bootstrapped broker behaves like a freshly formatted 4.0
//! cluster. It finalizes every registered feature at its release default,
//! which is `metadata.version = 25`, `group.version = 1`, and
//! `transaction.version = 2`. It also reports a real `finalized_features_epoch`
//! of `>= 0`. `UpdateFeatures` (`api_key` 57) then moves those levels.
//! `tests/feature_finalization.rs` exercises that path.
//!
//! Two shapes break every JVM admin client whose enum does not list the level:
//! a `supported_features` entry whose `max_version` is above the connecting
//! client's known `MetadataVersion` enum, and a finalized `metadata.version`
//! entry with `finalized_features_epoch = 0`. Such a client throws
//! `IllegalArgumentException` out of `MetadataVersion.fromFeatureLevel(N)` on
//! the first handshake. That failure once broke 19 `broker-jvm-acceptance`
//! tests.
//!
//! The advertised range `7` to `25`, that is `3.3-IV3` to `4.0-IV3`, tracks
//! Kafka's own `MetadataVersion` enum. This test guards the fresh-broker
//! surface.

use assert2::assert;
mod support;

use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;

#[tokio::test]
async fn v3_response_advertises_supported_and_bootstrapped_finalized_features() {
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

    assert!(resp.error_code == 0, "{resp:?}");

    // KIP-584 write-side: supported_features advertises metadata.version over
    // the supported range; the standalone broker self-bootstraps the release
    // defaults, so finalized_features carries metadata.version=25 and
    // group.version=1 with a real (>= 0) epoch. See the module-level note for
    // the JVM compatibility rationale.
    let mv = resp
        .supported_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("metadata.version advertised in supported_features");
    assert!(mv.min_version == 7, "{resp:?}");
    assert!(mv.max_version == 25, "{resp:?}");
    let gv = resp
        .supported_features
        .iter()
        .find(|f| f.name == "group.version")
        .expect("group.version advertised in supported_features");
    // Wire min is clamped to 1 (Kafka SupportedVersionRange requires >= 1),
    // even though the registry min is 0 (level 0 = "disabled", finalizable via
    // UpdateFeatures). Advertising min=0 here breaks pre-4.0 JVM admin clients.
    assert!(gv.min_version == 1, "{resp:?}");
    assert!(gv.max_version == 1, "{resp:?}");
    let tv = resp
        .supported_features
        .iter()
        .find(|f| f.name == "transaction.version")
        .expect("transaction.version advertised in supported_features");
    assert!(tv.min_version == 1, "{resp:?}");
    assert!(tv.max_version == 2, "{resp:?}");

    // A self-bootstrapped broker finalizes the release defaults.
    let finalized_metadata_version = resp
        .finalized_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("metadata.version finalized at bootstrap");
    assert!(
        finalized_metadata_version.max_version_level == 25,
        "{resp:?}"
    );
    let finalized_group_version = resp
        .finalized_features
        .iter()
        .find(|f| f.name == "group.version")
        .expect("group.version finalized at bootstrap");
    assert!(finalized_group_version.max_version_level == 1, "{resp:?}");
    let finalized_transaction_version = resp
        .finalized_features
        .iter()
        .find(|f| f.name == "transaction.version")
        .expect("transaction.version finalized at bootstrap");
    assert!(
        finalized_transaction_version.max_version_level == 2,
        "{resp:?}"
    );
    assert!(
        resp.finalized_features_epoch >= 0,
        "self-bootstrapped broker finalizes defaults so epoch must be >= 0: {resp:?}"
    );

    p.broker.shutdown().await;
}
