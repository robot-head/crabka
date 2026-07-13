//! KIP-584 `UpdateFeatures` (`api_key` 57) write path. Finalizing
//! `metadata.version` lands a Raft-persisted `V1FeatureLevel` record and
//! surfaces the finalized feature + a real epoch through `ApiVersions`.
//! Validation rejects unsupported features and out-of-range levels;
//! `validate_only` runs all checks without persisting.

use assert2::assert;
mod support;

use crabka_protocol::owned::{
    api_versions_request::ApiVersionsRequest,
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
};

fn metadata_version_update(level: i16) -> UpdateFeaturesRequest {
    UpdateFeaturesRequest {
        feature_updates: vec![FeatureUpdateKey {
            feature: "metadata.version".into(),
            max_version_level: level,
            upgrade_type: 1,
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn finalizes_metadata_version_and_surfaces_in_api_versions() {
    let p = support::start().await;

    let resp = p
        .client
        .send(metadata_version_update(25))
        .await
        .expect("UpdateFeatures");
    assert!(resp.error_code == 0, "{resp:?}");
    if let Some(row) = resp
        .results
        .iter()
        .find(|r| r.feature == "metadata.version")
    {
        assert!(row.error_code == 0, "{resp:?}");
    }

    // ApiVersions now surfaces the finalized feature with a real epoch.
    let av = p
        .client
        .send(ApiVersionsRequest {
            client_software_name: "crabka-test".into(),
            client_software_version: "0.0.0".into(),
            ..Default::default()
        })
        .await
        .expect("ApiVersions");
    let fin = av
        .finalized_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("metadata.version finalized");
    assert!(fin.max_version_level == 25, "{av:?}");
    assert!(av.finalized_features_epoch >= 0, "{av:?}");

    p.broker.shutdown().await;
}

/// Assert a row carries `code`, tolerant of the wire version: on
/// `UpdateFeatures` v2 the `results` array is not encoded, so the handler
/// promotes the first non-zero row error to the top-level `error_code`.
fn assert_feature_error(
    resp: &crabka_protocol::owned::update_features_response::UpdateFeaturesResponse,
    feature: &str,
    code: i16,
) {
    if let Some(row) = resp.results.iter().find(|r| r.feature == feature) {
        assert!(row.error_code == code, "per-row error: {resp:?}");
    } else {
        assert!(
            resp.error_code == code,
            "promoted top-level error: {resp:?}"
        );
    }
}

#[tokio::test]
async fn rejects_unsupported_feature() {
    let p = support::start().await;
    let mut req = metadata_version_update(1);
    req.feature_updates[0].feature = "not.a.feature".into();
    let resp = p.client.send(req).await.expect("UpdateFeatures");
    // INVALID_REQUEST (42) for an unsupported feature.
    assert_feature_error(&resp, "not.a.feature", 42);
    p.broker.shutdown().await;
}

#[tokio::test]
async fn rejects_level_above_supported_max() {
    let p = support::start().await;
    let resp = p
        .client
        .send(metadata_version_update(99))
        .await
        .expect("UpdateFeatures");
    // INVALID_UPDATE_VERSION (95) for a level above the supported max.
    assert_feature_error(&resp, "metadata.version", 95);
    p.broker.shutdown().await;
}

fn metadata_version(
    api_versions: &crabka_protocol::owned::api_versions_response::ApiVersionsResponse,
) -> i16 {
    api_versions
        .finalized_features
        .iter()
        .find(|feature| feature.name == "metadata.version")
        .expect("metadata.version finalized at bootstrap")
        .max_version_level
}

#[tokio::test]
async fn validate_only_does_not_persist() {
    let p = support::start().await;

    // A self-bootstrapped broker already finalizes metadata.version=25, so
    // emptiness no longer signals "nothing persisted". Capture the level + epoch
    // BEFORE, send a validate_only request that WOULD change metadata.version,
    // then assert neither moved — validate_only must run the checks without
    // persisting (no epoch bump, no level change).
    let api_versions = || {
        p.client.send(ApiVersionsRequest {
            client_software_name: "crabka-test".into(),
            client_software_version: "0.0.0".into(),
            ..Default::default()
        })
    };

    let before = api_versions().await.expect("ApiVersions");
    assert!(metadata_version(&before) == 25, "{before:?}");
    let epoch_before = before.finalized_features_epoch;
    assert!(epoch_before >= 0, "{before:?}");

    // Request a SAFE_DOWNGRADE to level 24 with validate_only — this would
    // change metadata.version if persisted.
    let mut req = metadata_version_update(24);
    req.feature_updates[0].upgrade_type = 2; // SAFE_DOWNGRADE
    req.validate_only = true;
    let resp = p.client.send(req).await.expect("UpdateFeatures");
    assert!(resp.error_code == 0, "{resp:?}");

    // Nothing changed: same level, same epoch.
    let after = api_versions().await.expect("ApiVersions");
    assert!(
        metadata_version(&after) == 25,
        "validate_only must not change the level: {after:?}",
    );
    assert!(
        after.finalized_features_epoch == epoch_before,
        "validate_only must not bump the epoch: {after:?}",
    );
    p.broker.shutdown().await;
}

#[tokio::test]
async fn rejects_level_below_min_floor() {
    let p = support::start().await;
    // Level 6 is below the baseline floor (METADATA_VERSION_MIN = 7); the
    // controller refuses it with INVALID_UPDATE_VERSION (95).
    let resp = p
        .client
        .send(metadata_version_update(6))
        .await
        .expect("UpdateFeatures");
    assert_feature_error(&resp, "metadata.version", 95);
    p.broker.shutdown().await;
}
