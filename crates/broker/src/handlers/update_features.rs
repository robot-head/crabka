//! `UpdateFeatures` handler (`api_key` 57, KIP-584).
//!
//! Finalizes broker-supported features (currently only `metadata.version`)
//! through a Raft-persisted `V1FeatureLevel` record. Gated by `Alter` on
//! `Cluster("kafka-cluster")`. Intercepted inline in `network::dispatch`
//! (like `AlterUserScramCredentials`) so the handler receives the
//! authenticated principal + peer for the ACL check.

use crabka_metadata::{AclOperation, FeatureLevelRecord, MetadataRecord};
use crabka_protocol::owned::update_features_request::UpdateFeaturesRequest;
use crabka_protocol::owned::update_features_response::{
    UpdatableFeatureResult, UpdateFeaturesResponse,
};
use crabka_raft::RaftError;

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;

/// True if every KIP-1022 dependency for a feature finalize is already met in
/// the target image. `deps` is the feature's `dependencies(level)` slice:
/// `(dependency_feature_name, min_finalized_level)` pairs.
fn dependencies_met(image: &crabka_metadata::MetadataImage, deps: &[(&str, i16)]) -> bool {
    deps.iter().all(|(dep, min_level)| {
        image
            .finalized_features()
            .get(*dep)
            .is_some_and(|finalized| finalized >= min_level)
    })
}

/// KIP-584 `FeatureUpdate.UpgradeType`: 1 = UPGRADE, 2 = `SAFE_DOWNGRADE`,
/// 3 = `UNSAFE_DOWNGRADE`.
fn downgrade_allowed(version: i16, allow_downgrade: bool, upgrade_type: i8) -> bool {
    if version == 0 {
        allow_downgrade
    } else {
        matches!(upgrade_type, 2 | 3)
    }
}

#[allow(clippy::too_many_lines)]
#[tracing::instrument(
    name = "handle_update_features",
    level = "info",
    skip_all,
    fields(api = "UpdateFeatures", version)
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: UpdateFeaturesRequest,
    version: i16,
    ctx: &crate::handlers::RequestContext<'_>,
) -> UpdateFeaturesResponse {
    let image = broker.controller.current_image();

    // Whole-request Cluster:Alter gate.
    let authorized = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: crabka_metadata::ResourceType::Cluster,
            resource_name: "kafka-cluster",
            operation: AclOperation::Alter,
        },
    ) == AuthorizationResult::Allow;

    if !authorized {
        return top_level_error(
            codes::CLUSTER_AUTHORIZATION_FAILED,
            "Cluster authorization failed.",
            version,
        );
    }

    if req.feature_updates.is_empty() {
        return top_level_error(
            codes::INVALID_REQUEST,
            "Can not provide empty feature updates in the request.",
            version,
        );
    }

    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut results: Vec<UpdatableFeatureResult> = Vec::new();
    let mut records: Vec<MetadataRecord> = Vec::new();

    for upd in &req.feature_updates {
        let name = upd.feature.clone();
        if !seen.insert(name.clone()) {
            results.push(row(
                name,
                codes::INVALID_REQUEST,
                "Provided feature can not be updated more than once in the request.",
            ));
            continue;
        }
        let Some(feat) = crabka_metadata::feature(&name) else {
            results.push(row(
                name,
                codes::INVALID_REQUEST,
                "Could not apply finalized feature update because the provided feature is not supported.",
            ));
            continue;
        };

        let level = upd.max_version_level;
        let current = image.finalized_features().get(&name).copied();
        let allow_dg = downgrade_allowed(version, upd.allow_downgrade, upd.upgrade_type);

        let (_min, max) = feat.supported_range();
        if level < 0 || level > max {
            results.push(row(
                name,
                codes::INVALID_UPDATE_VERSION,
                "Provided version level is not in the supported range.",
            ));
            continue;
        }
        // Per-feature downgrade-safety floor (KIP-584 unsafe downgrade): a
        // finalize below the level the live image requires is rejected even
        // with the downgrade flag set. `level == 0` (delete) is handled by the
        // tombstone path below, not the floor.
        let floor = feat.min_required_floor(&image);
        if level > 0 && level < floor {
            results.push(row(
                name,
                codes::INVALID_UPDATE_VERSION,
                "Can not downgrade the feature below the level required by existing cluster state.",
            ));
            continue;
        }
        // KIP-1022 dependencies: every dependency must already be finalized
        // at >= its required level in the target image.
        if !dependencies_met(&image, feat.dependencies(level)) {
            results.push(row(
                name,
                codes::INVALID_UPDATE_VERSION,
                "Can not finalize feature: a required dependency feature is not finalized at a high enough level.",
            ));
            continue;
        }
        if level == 0 {
            // Delete the finalized feature; only valid if it exists and a
            // downgrade is permitted.
            if current.is_none() {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    "Can not delete a finalized feature that does not exist.",
                ));
                continue;
            }
            if !allow_dg {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    "Can not delete a finalized feature without setting the downgrade flag.",
                ));
                continue;
            }
        } else if let Some(cur) = current
            && level < cur
            && !allow_dg
        {
            results.push(row(
                name,
                codes::INVALID_UPDATE_VERSION,
                "Can not downgrade a finalized feature without setting the downgrade flag.",
            ));
            continue;
        }

        // Accepted.
        records.push(MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: name.clone(),
            level,
        }));
        results.push(row(name, codes::NONE, ""));
    }

    // validate_only: never persist.
    if req.validate_only {
        return finalize(results, version);
    }

    if !records.is_empty() {
        match broker.controller.submit_change(records).await {
            Ok(()) => {}
            Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                return apply_request_wide(
                    results,
                    codes::NOT_CONTROLLER,
                    "This broker is not the active controller.",
                    version,
                );
            }
            Err(e) => {
                tracing::warn!(error = %e, "UpdateFeatures: submit_change failed");
                return apply_request_wide(
                    results,
                    codes::FEATURE_UPDATE_FAILED,
                    "Failed to persist the feature update.",
                    version,
                );
            }
        }
    }

    finalize(results, version)
}

fn row(feature: String, error_code: i16, msg: &str) -> UpdatableFeatureResult {
    UpdatableFeatureResult {
        feature,
        error_code,
        error_message: (error_code != codes::NONE).then(|| msg.to_string()),
        ..Default::default()
    }
}

fn top_level_error(code: i16, msg: &str, version: i16) -> UpdateFeaturesResponse {
    let _ = version;
    UpdateFeaturesResponse {
        throttle_time_ms: 0,
        error_code: code,
        error_message: Some(msg.to_string()),
        results: Vec::new(),
        ..Default::default()
    }
}

/// Overwrite every `ok` row with a request-wide failure code, and set the
/// top-level error too.
fn apply_request_wide(
    mut results: Vec<UpdatableFeatureResult>,
    code: i16,
    msg: &str,
    version: i16,
) -> UpdateFeaturesResponse {
    for r in results.iter_mut().filter(|r| r.error_code == codes::NONE) {
        r.error_code = code;
        r.error_message = Some(msg.to_string());
    }
    let mut resp = finalize(results, version);
    resp.error_code = code;
    resp.error_message = Some(msg.to_string());
    resp
}

/// Assemble the final response. On v2 (no `results` array on the wire) the
/// top-level `error_code` must carry the first non-zero row code so the
/// client still sees the failure.
fn finalize(results: Vec<UpdatableFeatureResult>, version: i16) -> UpdateFeaturesResponse {
    let (top_code, top_msg) = if version >= 2 {
        results
            .iter()
            .find(|r| r.error_code != codes::NONE)
            .map_or((codes::NONE, None), |r| {
                (r.error_code, r.error_message.clone())
            })
    } else {
        (codes::NONE, None)
    };
    UpdateFeaturesResponse {
        throttle_time_ms: 0,
        error_code: top_code,
        error_message: top_msg,
        results,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn downgrade_flag_v0_uses_allow_downgrade() {
        assert!(downgrade_allowed(0, true, 1));
        assert!(!downgrade_allowed(0, false, 2));
    }

    #[test]
    fn downgrade_flag_v1_uses_upgrade_type() {
        assert!(!downgrade_allowed(1, true, 1)); // UPGRADE
        assert!(downgrade_allowed(1, false, 2)); // SAFE_DOWNGRADE
        assert!(downgrade_allowed(1, false, 3)); // UNSAFE_DOWNGRADE
    }

    #[test]
    fn row_sets_message_only_on_error() {
        assert!(
            row("metadata.version".into(), codes::NONE, "x")
                .error_message
                .is_none()
        );
        assert!(
            row(
                "metadata.version".into(),
                codes::INVALID_UPDATE_VERSION,
                "bad"
            )
            .error_message
            .as_deref()
                == Some("bad")
        );
    }

    #[test]
    fn finalize_v2_promotes_first_error_to_top_level() {
        let results = vec![
            row("a".into(), codes::NONE, ""),
            row("b".into(), codes::INVALID_UPDATE_VERSION, "bad"),
        ];
        let resp = finalize(results, 2);
        assert!(resp.error_code == codes::INVALID_UPDATE_VERSION);
    }

    #[test]
    fn finalize_v1_keeps_top_level_none() {
        let results = vec![row("b".into(), codes::INVALID_UPDATE_VERSION, "bad")];
        let resp = finalize(results, 1);
        assert!(resp.error_code == codes::NONE);
    }

    #[test]
    fn metadata_version_floor_via_registry() {
        // A fresh image floors metadata.version at its supported min; the
        // registry trait path returns that floor.
        let image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let feat = crabka_metadata::feature("metadata.version").unwrap();
        assert!(feat.min_required_floor(&image) == crate::features::METADATA_VERSION_MIN);
    }

    #[test]
    fn dependencies_met_checks_finalized_levels() {
        use crabka_metadata::{FeatureLevelRecord, MetadataImage, MetadataRecord};
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        // No deps → trivially met.
        assert!(dependencies_met(&image, &[]));
        // Unmet: metadata.version not finalized at all.
        assert!(!dependencies_met(&image, &[("metadata.version", 22)]));
        // Finalize metadata.version=25 → a >=22 dependency is now met, >=26 not.
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 25,
        }));
        assert!(dependencies_met(&image, &[("metadata.version", 22)]));
        assert!(!dependencies_met(&image, &[("metadata.version", 26)]));
    }
}
