//! `UpdateFeatures` handler (`api_key` 57, KIP-584).
//!
//! This handler finalizes broker-supported features, at present only
//! `metadata.version`, through a Raft-persisted `V1FeatureLevel` record.
//! `Alter` on `Cluster("kafka-cluster")` gates it.
//!
//! `network::dispatch` intercepts the request inline, as it does for
//! `AlterUserScramCredentials`, so the handler receives the authenticated
//! principal and the peer for the ACL check.

use crabka_metadata::{AclOperation, FeatureLevelRecord, MetadataRecord};
use crabka_protocol::owned::{
    update_features_request::UpdateFeaturesRequest,
    update_features_response::{UpdatableFeatureResult, UpdateFeaturesResponse},
};
use crabka_raft::RaftError;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
};

/// True when the target image already meets every KIP-1022 dependency for a
/// feature finalize. `deps` is the feature's `dependencies(level)` slice, which
/// holds `(dependency_feature_name, min_finalized_level)` pairs.
fn dependencies_met(image: &crabka_metadata::MetadataImage, deps: &[(&str, i16)]) -> bool {
    deps.iter().all(|(dep, min_level)| {
        image
            .finalized_features()
            .get(*dep)
            .is_some_and(|finalized| finalized >= min_level)
    })
}

/// KIP-584 `FeatureUpdate.UpgradeType` wire code for a safe downgrade, which
/// loses nothing.
const UPGRADE_TYPE_SAFE_DOWNGRADE: i8 = 2;

/// KIP-584 `FeatureUpdate.UpgradeType` wire code for an unsafe downgrade. The
/// caller accepts the loss of metadata written at the higher feature level.
const UPGRADE_TYPE_UNSAFE_DOWNGRADE: i8 = 3;

/// KIP-584: a requested `max_version_level` of `0` asks to *delete* the
/// finalized feature rather than move it to another level.
const DELETE_FINALIZED_LEVEL: i16 = 0;

/// KIP-584 `FeatureUpdate.UpgradeType`: 1 is UPGRADE, 2 is `SAFE_DOWNGRADE`,
/// and 3 is `UNSAFE_DOWNGRADE`. Request v0 comes from before this field and
/// carries the boolean `allow_downgrade` flag instead.
fn downgrade_allowed(version: i16, allow_downgrade: bool, upgrade_type: i8) -> bool {
    if version == 0 {
        allow_downgrade
    } else {
        matches!(
            upgrade_type,
            UPGRADE_TYPE_SAFE_DOWNGRADE | UPGRADE_TYPE_UNSAFE_DOWNGRADE
        )
    }
}

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
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
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

    let (results, records) = validate_updates(&req, &image, version);

    // validate_only: never persist.
    if req.validate_only {
        return finalize(results, version);
    }

    // Activation must be derived from the validated row. Looking at the raw
    // request here would let a duplicate or otherwise rejected kraft.version
    // row activate the Raft feature despite its error response.
    let kraft_upgrade = image.kraft_version() == 0
        && req
            .feature_updates
            .iter()
            .zip(&results)
            .any(|(update, result)| {
                update.feature == crabka_metadata::metadata_version::KRAFT_VERSION_FEATURE
                    && result.error_code == codes::NONE
            });
    if kraft_upgrade {
        match broker.controller.finalize_kraft_version(1).await {
            Ok(crabka_raft::ReconfigOutcome::Committed) => {}
            Ok(crabka_raft::ReconfigOutcome::NotLeader { .. })
            | Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                return apply_request_wide(
                    results,
                    codes::NOT_CONTROLLER,
                    "This broker is not the active controller.",
                    version,
                );
            }
            Err(error) => {
                tracing::warn!(%error, "UpdateFeatures: kraft.version activation failed");
                return apply_request_wide(
                    results,
                    codes::FEATURE_UPDATE_FAILED,
                    "Failed to activate kraft.version.",
                    version,
                );
            }
        }
    }

    if !records.is_empty() {
        match broker.controller.submit_change(records).await {
            Ok(_) => {}
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

fn validate_updates(
    request: &UpdateFeaturesRequest,
    image: &crabka_metadata::MetadataImage,
    version: i16,
) -> (Vec<UpdatableFeatureResult>, Vec<MetadataRecord>) {
    let mut seen = std::collections::HashSet::new();
    let mut results = Vec::new();
    let mut records = Vec::new();
    for upd in &request.feature_updates {
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
        if name == crabka_metadata::metadata_version::KRAFT_VERSION_FEATURE {
            let current = i16::try_from(image.kraft_version()).unwrap_or(i16::MAX);
            if level != 1 || current > level {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    "kraft.version can only be upgraded from 0 to 1.",
                ));
            } else {
                results.push(row(name, codes::NONE, ""));
            }
            continue;
        }
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
        let floor = feat.min_required_floor(image);
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
        if !dependencies_met(image, feat.dependencies(level)) {
            results.push(row(
                name,
                codes::INVALID_UPDATE_VERSION,
                "Can not finalize feature: a required dependency feature is not finalized at a high enough level.",
            ));
            continue;
        }
        if level == DELETE_FINALIZED_LEVEL {
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
    (results, records)
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
        error_code: code,
        error_message: Some(msg.to_string()),
        ..Default::default()
    }
}

/// Overwrites every `ok` row with a request-wide failure code, and sets the
/// top-level error as well.
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

/// Assembles the final response. On v2 the wire carries no `results` array, so
/// the top-level `error_code` must carry the first non-zero row code. The
/// client then still sees the failure.
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
        error_code: top_code,
        error_message: top_msg,
        results,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use crabka_protocol::owned::update_features_request::FeatureUpdateKey;
    use crabka_security::Principal;

    use crate::{
        authorizer::Authorizer,
        broker::{Broker, BrokerHandle},
        test_support::DenyAll,
    };

    const VERSION: i16 = 1;

    fn feature_update(name: &str, level: i16, upgrade_type: i8) -> FeatureUpdateKey {
        FeatureUpdateKey {
            feature: name.into(),
            max_version_level: level,
            upgrade_type,
            ..Default::default()
        }
    }

    fn metadata_update(level: i16, upgrade_type: i8) -> FeatureUpdateKey {
        feature_update(crate::features::METADATA_VERSION, level, upgrade_type)
    }

    fn validate_only(updates: Vec<FeatureUpdateKey>) -> UpdateFeaturesRequest {
        UpdateFeaturesRequest {
            feature_updates: updates,
            validate_only: true,
            ..Default::default()
        }
    }

    fn apply_request(updates: Vec<FeatureUpdateKey>) -> UpdateFeaturesRequest {
        UpdateFeaturesRequest {
            feature_updates: updates,
            validate_only: false,
            ..Default::default()
        }
    }

    fn principal() -> Principal {
        crate::test_support::principal("admin")
    }

    fn context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::test_support::request_context(principal, peer, "update-features-client")
    }

    async fn start_broker(authorizer: Arc<dyn Authorizer>) -> (BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|cfg| {
            cfg.audit_enabled = false;
            cfg.authorizer = authorizer;
        })
        .await
    }

    async fn call_with(
        authorizer: Arc<dyn Authorizer>,
        req: UpdateFeaturesRequest,
    ) -> (UpdateFeaturesResponse, BrokerHandle, tempfile::TempDir) {
        let (broker_handle, dir) = start_broker(authorizer).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        let resp = handle(&broker, req, VERSION, &ctx).await;
        (resp, broker_handle, dir)
    }

    fn assert_ok_row(resp: &UpdateFeaturesResponse, feature: &str) {
        let row = resp
            .results
            .iter()
            .find(|row| row.feature == feature)
            .expect("feature result row");
        assert!(row.error_code == codes::NONE, "{resp:?}");
        assert!(row.error_message.is_none(), "{resp:?}");
    }

    fn assert_row_error(resp: &UpdateFeaturesResponse, feature: &str, message: &str) {
        let row = resp
            .results
            .iter()
            .find(|row| row.feature == feature)
            .expect("feature result row");
        assert!(row.error_code == codes::INVALID_UPDATE_VERSION, "{resp:?}");
        assert!(
            row.error_message
                .as_deref()
                .is_some_and(|m| m.contains(message)),
            "{resp:?}"
        );
    }

    async fn wait_for_finalized_feature(broker: &Broker, feature: &str, level: i16) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if broker.controller.current_image().finalized_feature(feature) == Some(level) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("feature level visible");
    }

    use super::*;

    #[test]
    fn downgrade_flag_v0_uses_allow_downgrade() {
        assert!(downgrade_allowed(0, true, 1));
        assert!(!downgrade_allowed(0, false, 2));
    }

    #[test]
    fn downgrade_flag_v1_uses_upgrade_type() {
        // upgrade_type: 1 = UPGRADE, 2 = SAFE_DOWNGRADE, 3 = UNSAFE_DOWNGRADE.
        let cases = [
            // (allow_downgrade, upgrade_type, want); allow_downgrade is
            // ignored at v1+ — only upgrade_type decides.
            (true, 1, false),
            (false, 2, true),
            (false, 3, true),
        ];
        for (allow_downgrade, upgrade_type, want) in cases {
            assert!(
                downgrade_allowed(1, allow_downgrade, upgrade_type) == want,
                "allow_downgrade {allow_downgrade}, upgrade_type {upgrade_type}"
            );
        }
    }

    #[test]
    fn row_sets_message_only_on_error() {
        let ok = row("metadata.version".into(), codes::NONE, "x");
        assert!(ok.feature == "metadata.version");
        assert!(ok.error_message.is_none());

        let err = row(
            "metadata.version".into(),
            codes::INVALID_UPDATE_VERSION,
            "bad",
        );
        assert!(err.feature == "metadata.version");
        assert!(err.error_message.as_deref() == Some("bad"));
    }

    #[test]
    fn top_level_error_preserves_wire_shape() {
        let resp = top_level_error(codes::INVALID_REQUEST, "bad request", VERSION);

        let expected = UpdateFeaturesResponse {
            throttle_time_ms: 0,
            error_code: codes::INVALID_REQUEST,
            error_message: Some("bad request".to_string()),
            results: vec![],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
    }

    #[test]
    fn apply_request_wide_marks_only_successful_rows_and_sets_top_level() {
        let resp = apply_request_wide(
            vec![
                row("metadata.version".into(), codes::NONE, ""),
                row("eligible.feature".into(), codes::NONE, ""),
                row(
                    "not.a.feature".into(),
                    codes::INVALID_REQUEST,
                    "bad feature",
                ),
            ],
            codes::FEATURE_UPDATE_FAILED,
            "persist failed",
            VERSION,
        );

        let expected = UpdateFeaturesResponse {
            throttle_time_ms: 0,
            error_code: codes::FEATURE_UPDATE_FAILED,
            error_message: Some("persist failed".to_string()),
            results: vec![
                UpdatableFeatureResult {
                    feature: "metadata.version".to_string(),
                    error_code: codes::FEATURE_UPDATE_FAILED,
                    error_message: Some("persist failed".to_string()),
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
                },
                UpdatableFeatureResult {
                    feature: "eligible.feature".to_string(),
                    error_code: codes::FEATURE_UPDATE_FAILED,
                    error_message: Some("persist failed".to_string()),
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
                },
                UpdatableFeatureResult {
                    feature: "not.a.feature".to_string(),
                    error_code: codes::INVALID_REQUEST,
                    error_message: Some("bad feature".to_string()),
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
                },
            ],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
    }

    #[test]
    fn finalize_v2_promotes_first_error_to_top_level() {
        let results = vec![
            row("a".into(), codes::NONE, ""),
            row("b".into(), codes::INVALID_UPDATE_VERSION, "bad"),
        ];
        let resp = finalize(results, 2);
        let expected = UpdateFeaturesResponse {
            throttle_time_ms: 0,
            error_code: codes::INVALID_UPDATE_VERSION,
            error_message: Some("bad".to_string()),
            results: vec![
                UpdatableFeatureResult {
                    feature: "a".to_string(),
                    error_code: codes::NONE,
                    error_message: None,
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
                },
                UpdatableFeatureResult {
                    feature: "b".to_string(),
                    error_code: codes::INVALID_UPDATE_VERSION,
                    error_message: Some("bad".to_string()),
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
                },
            ],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
    }

    #[test]
    fn finalize_v1_keeps_top_level_none() {
        let results = vec![row("b".into(), codes::INVALID_UPDATE_VERSION, "bad")];
        let resp = finalize(results, 1);
        let expected = UpdateFeaturesResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            error_message: None,
            results: vec![UpdatableFeatureResult {
                feature: "b".to_string(),
                error_code: codes::INVALID_UPDATE_VERSION,
                error_message: Some("bad".to_string()),
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
            }],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
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

    #[tokio::test]
    async fn handle_denies_cluster_alter_with_top_level_error() {
        let req = validate_only(vec![metadata_update(
            crate::features::METADATA_VERSION_MAX,
            1,
        )]);

        let (resp, broker_handle, _dir) = Box::pin(call_with(Arc::new(DenyAll), req)).await;

        let expected = UpdateFeaturesResponse {
            throttle_time_ms: 0,
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("Cluster authorization failed.".to_string()),
            results: vec![],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_empty_feature_updates() {
        let (resp, broker_handle, _dir) = Box::pin(call_with(
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            validate_only(Vec::new()),
        ))
        .await;

        let expected = UpdateFeaturesResponse {
            throttle_time_ms: 0,
            error_code: codes::INVALID_REQUEST,
            error_message: Some(
                "Can not provide empty feature updates in the request.".to_string(),
            ),
            results: vec![],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_accepts_validate_only_metadata_version_at_supported_max() {
        let req = validate_only(vec![metadata_update(
            crate::features::METADATA_VERSION_MAX,
            1,
        )]);

        let (resp, broker_handle, _dir) = Box::pin(call_with(
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            req,
        ))
        .await;

        let expected = UpdateFeaturesResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            error_message: None,
            results: vec![UpdatableFeatureResult {
                feature: crate::features::METADATA_VERSION.to_string(),
                error_code: codes::NONE,
                error_message: None,
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
            }],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_persists_non_validate_feature_update() {
        let version = VERSION;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        let req = apply_request(vec![metadata_update(
            crate::features::METADATA_VERSION_MAX - 1,
            2,
        )]);

        let resp = handle(&broker, req, version, &ctx).await;

        assert!(resp.error_code == codes::NONE, "{resp:?}");
        assert_ok_row(&resp, crate::features::METADATA_VERSION);
        wait_for_finalized_feature(
            &broker,
            crate::features::METADATA_VERSION,
            crate::features::METADATA_VERSION_MAX - 1,
        )
        .await;
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_reports_duplicate_feature_on_second_row_only() {
        let req = validate_only(vec![
            metadata_update(crate::features::METADATA_VERSION_MAX, 1),
            metadata_update(crate::features::METADATA_VERSION_MAX, 1),
        ]);

        let (resp, broker_handle, _dir) = Box::pin(call_with(
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            req,
        ))
        .await;

        let expected = UpdateFeaturesResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            error_message: None,
            results: vec![
                UpdatableFeatureResult {
                    feature: crate::features::METADATA_VERSION.to_string(),
                    error_code: codes::NONE,
                    error_message: None,
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
                },
                UpdatableFeatureResult {
                    feature: crate::features::METADATA_VERSION.to_string(),
                    error_code: codes::INVALID_REQUEST,
                    error_message: Some(
                        "Provided feature can not be updated more than once in the request."
                            .to_string(),
                    ),
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
                },
            ],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_negative_level_with_supported_range_message() {
        let req = validate_only(vec![metadata_update(-1, 1)]);

        let (resp, broker_handle, _dir) = Box::pin(call_with(
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            req,
        ))
        .await;

        assert_row_error(&resp, crate::features::METADATA_VERSION, "supported range");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_level_above_supported_max_with_range_message() {
        let req = validate_only(vec![metadata_update(
            crate::features::METADATA_VERSION_MAX + 1,
            1,
        )]);

        let (resp, broker_handle, _dir) = Box::pin(call_with(
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            req,
        ))
        .await;

        assert_row_error(&resp, crate::features::METADATA_VERSION, "supported range");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_accepts_floor_level_with_safe_downgrade() {
        let req = validate_only(vec![metadata_update(
            crate::features::METADATA_VERSION_MIN,
            2,
        )]);

        let (resp, broker_handle, _dir) = Box::pin(call_with(
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            req,
        ))
        .await;

        assert!(resp.error_code == codes::NONE, "{resp:?}");
        assert_ok_row(&resp, crate::features::METADATA_VERSION);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_level_below_floor_with_floor_message() {
        let req = validate_only(vec![metadata_update(
            crate::features::METADATA_VERSION_MIN - 1,
            2,
        )]);

        let (resp, broker_handle, _dir) = Box::pin(call_with(
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            req,
        ))
        .await;

        assert_row_error(
            &resp,
            crate::features::METADATA_VERSION,
            "below the level required",
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_allows_delete_zero_when_downgrade_allowed() {
        let req = validate_only(vec![metadata_update(0, 2)]);

        let (resp, broker_handle, _dir) = Box::pin(call_with(
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            req,
        ))
        .await;

        assert!(resp.error_code == codes::NONE, "{resp:?}");
        assert_ok_row(&resp, crate::features::METADATA_VERSION);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_downgrade_without_downgrade_flag() {
        let req = validate_only(vec![metadata_update(
            crate::features::METADATA_VERSION_MAX - 1,
            1,
        )]);

        let (resp, broker_handle, _dir) = Box::pin(call_with(
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            req,
        ))
        .await;

        assert_row_error(&resp, crate::features::METADATA_VERSION, "downgrade");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_delete_zero_without_downgrade_flag() {
        let req = validate_only(vec![metadata_update(0, 1)]);

        let (resp, broker_handle, _dir) = Box::pin(call_with(
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            req,
        ))
        .await;

        assert_row_error(&resp, crate::features::METADATA_VERSION, "downgrade flag");
        broker_handle.shutdown().await;
    }
}
