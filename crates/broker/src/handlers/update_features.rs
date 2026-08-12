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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpdateType {
    Upgrade,
    SafeDowngrade,
    UnsafeDowngrade,
}

fn update_type(version: i16, allow_downgrade: bool, upgrade_type: i8) -> Option<UpdateType> {
    if version == 0 {
        return Some(if allow_downgrade {
            UpdateType::SafeDowngrade
        } else {
            UpdateType::Upgrade
        });
    }
    match upgrade_type {
        1 => Some(UpdateType::Upgrade),
        UPGRADE_TYPE_SAFE_DOWNGRADE => Some(UpdateType::SafeDowngrade),
        UPGRADE_TYPE_UNSAFE_DOWNGRADE => Some(UpdateType::UnsafeDowngrade),
        _ => None,
    }
}

/// KIP-584 `FeatureUpdate.UpgradeType`: 1 is UPGRADE, 2 is `SAFE_DOWNGRADE`,
/// and 3 is `UNSAFE_DOWNGRADE`. Request v0 comes from before this field and
/// carries the boolean `allow_downgrade` flag instead.
#[cfg(test)]
fn downgrade_allowed(version: i16, allow_downgrade: bool, upgrade_type: i8) -> bool {
    update_type(version, allow_downgrade, upgrade_type)
        .is_some_and(|kind| kind != UpdateType::Upgrade)
}

fn unsupported_registered_node(
    image: &crabka_metadata::MetadataImage,
    feature: &str,
    level: i16,
) -> Option<String> {
    for broker in image.brokers() {
        if !broker
            .features
            .get(feature)
            .is_some_and(|&(min, max)| min <= level && level <= max)
        {
            return Some(format!(
                "Broker {} does not support {feature} level {level}.",
                broker.node_id
            ));
        }
    }
    for controller in image.controllers() {
        if !controller
            .features
            .get(feature)
            .is_some_and(|&(min, max)| min <= level && level <= max)
        {
            return Some(format!(
                "Controller {} does not support {feature} level {level}.",
                controller.node_id
            ));
        }
    }
    None
}

fn registered_node_without_metadata_downgrade_capability(
    image: &crabka_metadata::MetadataImage,
) -> Option<String> {
    let supports_downgrade = |features: &std::collections::BTreeMap<String, (i16, i16)>| {
        features
            .get(crabka_metadata::metadata_version::METADATA_DOWNGRADE_CAPABILITY_FEATURE)
            .is_some_and(|&(min, max)| {
                min <= crabka_metadata::metadata_version::METADATA_DOWNGRADE_CAPABILITY_LEVEL
                    && crabka_metadata::metadata_version::METADATA_DOWNGRADE_CAPABILITY_LEVEL <= max
            })
    };
    for broker in image.brokers() {
        if !supports_downgrade(&broker.features) {
            return Some(format!(
                "Broker {} does not support online metadata.version downgrade.",
                broker.node_id
            ));
        }
    }
    for controller in image.controllers() {
        if !supports_downgrade(&controller.features) {
            return Some(format!(
                "Controller {} does not support online metadata.version downgrade.",
                controller.node_id
            ));
        }
    }
    None
}

fn unregistered_controller(
    image: &crabka_metadata::MetadataImage,
) -> Option<crabka_metadata::NodeId> {
    image
        .voters()
        .iter()
        .map(|voter| voter.id)
        .find(|&id| image.controller(id).is_none())
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
    let mut metadata_version_record = None;
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
        let Some(update_type) = update_type(version, upd.allow_downgrade, upd.upgrade_type) else {
            results.push(row(
                name,
                codes::INVALID_UPDATE_VERSION,
                "The controller does not support the given upgrade type.",
            ));
            continue;
        };
        let allow_dg = update_type != UpdateType::Upgrade;

        let (_min, max) = feat.supported_range();
        if level < 0 || level > max {
            results.push(row(
                name,
                codes::INVALID_UPDATE_VERSION,
                "Provided version level is not in the supported range.",
            ));
            continue;
        }
        if let Some(cur) = current {
            if level < cur && !allow_dg {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    "Can not downgrade a finalized feature without setting the downgrade flag.",
                ));
                continue;
            }
            if level > cur && allow_dg {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    "Can not downgrade to a newer feature version.",
                ));
                continue;
            }
        }
        let mut downgrade_records = Vec::new();
        let mut projected_image = None;
        if name == crabka_metadata::metadata_version::METADATA_VERSION_FEATURE
            && current.is_some_and(|cur| level < cur)
        {
            if level < crabka_metadata::metadata_version::ONLINE_DOWNGRADE_MIN_LEVEL {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    "Online metadata.version downgrade requires 3.7-IV0 or newer.",
                ));
                continue;
            }
            if let Some(controller) = unregistered_controller(image) {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    &format!(
                        "Controller {controller} has not registered, so its metadata.version support cannot be verified."
                    ),
                ));
                continue;
            }
            if let Some(message) = registered_node_without_metadata_downgrade_capability(image) {
                results.push(row(name, codes::INVALID_UPDATE_VERSION, &message));
                continue;
            }
            downgrade_records = image.metadata_version_downgrade_records(level);
            if !downgrade_records.is_empty() && update_type != UpdateType::UnsafeDowngrade {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    "Refusing a lossy metadata.version downgrade; retry with UNSAFE_DOWNGRADE to discard incompatible metadata.",
                ));
                continue;
            }
            if !downgrade_records.is_empty() {
                let mut projected = image.clone();
                for record in &downgrade_records {
                    projected.apply(record);
                }
                projected_image = Some(projected);
            }
        }
        if let Some(message) = unsupported_registered_node(image, &name, level) {
            results.push(row(name, codes::INVALID_UPDATE_VERSION, &message));
            continue;
        }
        // Per-feature downgrade-safety floor (KIP-584 unsafe downgrade): a
        // finalize below the level the live image requires is rejected even
        // with the downgrade flag set. `level == 0` (delete) is handled by the
        // tombstone path below, not the floor.
        // Unsafe metadata.version downgrades validate against the image that
        // will exist after their explicit cleanup records apply. Computing the
        // floor from the pre-cleanup image would reject the very state removal
        // the caller authorized.
        let target_image = projected_image.as_ref().unwrap_or(image);
        let floor = feat.min_required_floor(target_image);
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
        if !dependencies_met(target_image, feat.dependencies(level)) {
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
        }

        // Accepted.
        let feature_record = MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: name.clone(),
            level,
        });
        if name == crabka_metadata::metadata_version::METADATA_VERSION_FEATURE {
            records.extend(downgrade_records);
            metadata_version_record = Some(feature_record);
        } else {
            records.push(feature_record);
        }
        results.push(row(name, codes::NONE, ""));
    }
    // KIP-1155: the metadata.version record is always emitted last, after any
    // records that remove fields unavailable at the target version.
    records.extend(metadata_version_record);
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
    fn update_type_rejects_unknown_v1_value() {
        assert!(update_type(1, false, 0).is_none());
        assert!(update_type(1, false, 4).is_none());
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

    fn image_with_directory(metadata_version: i16) -> crabka_metadata::MetadataImage {
        let mut image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let supported_features = crabka_metadata::supported_feature_ranges();
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: crabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
            level: metadata_version,
        }));
        image.apply(&MetadataRecord::V1BrokerRegistration(
            crabka_metadata::BrokerRegistrationRecord {
                node_id: crabka_metadata::NodeId(1),
                broker_epoch: 9,
                incarnation_id: uuid::Uuid::from_u128(1),
                host: "broker-1".into(),
                port: 9092,
                rack: None,
                endpoints: vec![],
                log_dirs: vec![uuid::Uuid::from_u128(0xD2)],
                features: supported_features.clone(),
            },
        ));
        image.apply(&MetadataRecord::V1Partition(
            crabka_metadata::PartitionRecord {
                topic: "orders".into(),
                partition: 0,
                leader: crabka_metadata::NodeId(1),
                replicas: vec![crabka_metadata::NodeId(1)],
                isr: vec![crabka_metadata::NodeId(1)],
                directories: vec![uuid::Uuid::from_u128(0xD1)],
                ..Default::default()
            },
        ));
        image
    }

    #[test]
    fn unsafe_metadata_downgrade_cleans_lossy_fields_before_version_record() {
        let image = image_with_directory(crate::features::METADATA_VERSION_MAX);
        let target = crabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL - 1;

        let (safe_results, safe_records) = validate_updates(
            &validate_only(vec![metadata_update(target, UPGRADE_TYPE_SAFE_DOWNGRADE)]),
            &image,
            VERSION,
        );
        assert!(safe_results[0].error_code == codes::INVALID_UPDATE_VERSION);
        assert!(
            safe_results[0]
                .error_message
                .as_deref()
                .is_some_and(|message| message.contains("lossy"))
        );
        assert!(safe_records.is_empty());

        let (unsafe_results, unsafe_records) = validate_updates(
            &validate_only(vec![metadata_update(target, UPGRADE_TYPE_UNSAFE_DOWNGRADE)]),
            &image,
            VERSION,
        );
        assert!(unsafe_results[0].error_code == codes::NONE);
        let expected = vec![
            MetadataRecord::V1BrokerRegistration(crabka_metadata::BrokerRegistrationRecord {
                node_id: crabka_metadata::NodeId(1),
                broker_epoch: 9,
                incarnation_id: uuid::Uuid::from_u128(1),
                host: "broker-1".into(),
                port: 9092,
                rack: None,
                endpoints: vec![],
                log_dirs: vec![],
                features: crabka_metadata::supported_feature_ranges(),
            }),
            MetadataRecord::V1PartitionDirAssignment(
                crabka_metadata::PartitionDirAssignmentRecord {
                    topic: "orders".into(),
                    partition: 0,
                    replica: crabka_metadata::NodeId(1),
                    directory: uuid::Uuid::nil(),
                },
            ),
            MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: crabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
                level: target,
            }),
        ];
        assert!(unsafe_records == expected);

        let mut projected = image;
        for record in &unsafe_records {
            projected.apply(record);
        }
        assert!(
            projected
                .partition("orders", 0)
                .expect("partition")
                .directories
                == vec![uuid::Uuid::nil()]
        );
        assert!(
            projected
                .broker(crabka_metadata::NodeId(1))
                .expect("broker")
                .log_dirs
                .is_empty()
        );
        assert!(projected.finalized_metadata_version() == Some(target));
    }

    #[test]
    fn safe_metadata_downgrade_preserves_representable_directory_fields() {
        let image = image_with_directory(crate::features::METADATA_VERSION_MAX);
        let target = crabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL;

        let (results, records) = validate_updates(
            &validate_only(vec![metadata_update(target, UPGRADE_TYPE_SAFE_DOWNGRADE)]),
            &image,
            VERSION,
        );

        assert!(results[0].error_code == codes::NONE);
        assert!(
            records
                == vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                    name: crabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
                    level: target,
                })]
        );
    }

    #[test]
    fn metadata_downgrade_rejects_registered_nodes_without_capability() {
        let supported = std::collections::BTreeMap::from([(
            crabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
            (
                crate::features::METADATA_VERSION_MIN,
                crate::features::METADATA_VERSION_MAX,
            ),
        )]);
        let registrations = [
            (
                MetadataRecord::V1BrokerRegistration(crabka_metadata::BrokerRegistrationRecord {
                    node_id: crabka_metadata::NodeId(2),
                    broker_epoch: 0,
                    incarnation_id: uuid::Uuid::nil(),
                    host: String::new(),
                    port: 0,
                    rack: None,
                    log_dirs: vec![],
                    endpoints: vec![],
                    features: supported.clone(),
                }),
                "Broker 2",
            ),
            (
                MetadataRecord::V1ControllerRegistration(
                    crabka_metadata::ControllerRegistrationRecord {
                        node_id: crabka_metadata::NodeId(3),
                        incarnation_id: uuid::Uuid::nil(),
                        zk_migration_ready: false,
                        endpoints: vec![],
                        features: supported,
                    },
                ),
                "Controller 3",
            ),
        ];

        for (registration, expected_node) in registrations {
            let mut image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
            image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: crabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
                level: crate::features::METADATA_VERSION_MAX,
            }));
            image.apply(&registration);
            let (results, records) = validate_updates(
                &validate_only(vec![metadata_update(
                    crabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL,
                    UPGRADE_TYPE_SAFE_DOWNGRADE,
                )]),
                &image,
                VERSION,
            );

            assert!(results[0].error_code == codes::INVALID_UPDATE_VERSION);
            assert!(
                results[0].error_message.as_deref().is_some_and(|message| {
                    message.contains(expected_node)
                        && message.contains("does not support online metadata.version downgrade")
                }),
                "{results:?}"
            );
            assert!(records.is_empty());
        }
    }

    #[test]
    fn metadata_update_checks_every_capable_registered_node_supports_target() {
        let mut supported = crabka_metadata::supported_feature_ranges();
        supported.insert(
            crabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
            (
                crabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL,
                crate::features::METADATA_VERSION_MAX,
            ),
        );
        let registrations = [
            (
                MetadataRecord::V1BrokerRegistration(crabka_metadata::BrokerRegistrationRecord {
                    node_id: crabka_metadata::NodeId(2),
                    broker_epoch: 0,
                    incarnation_id: uuid::Uuid::nil(),
                    host: String::new(),
                    port: 0,
                    rack: None,
                    log_dirs: vec![],
                    endpoints: vec![],
                    features: supported.clone(),
                }),
                "Broker 2",
            ),
            (
                MetadataRecord::V1ControllerRegistration(
                    crabka_metadata::ControllerRegistrationRecord {
                        node_id: crabka_metadata::NodeId(3),
                        incarnation_id: uuid::Uuid::nil(),
                        zk_migration_ready: false,
                        endpoints: vec![],
                        features: supported,
                    },
                ),
                "Controller 3",
            ),
        ];

        for (registration, expected_node) in registrations {
            let mut image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
            image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: crabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
                level: crate::features::METADATA_VERSION_MAX,
            }));
            image.apply(&registration);
            let (results, records) = validate_updates(
                &validate_only(vec![metadata_update(
                    crabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL - 1,
                    UPGRADE_TYPE_SAFE_DOWNGRADE,
                )]),
                &image,
                VERSION,
            );

            assert!(results[0].error_code == codes::INVALID_UPDATE_VERSION);
            assert!(
                results[0]
                    .error_message
                    .as_deref()
                    .is_some_and(|message| message.contains(expected_node)),
                "{results:?}"
            );
            assert!(records.is_empty());
        }
    }

    #[test]
    fn metadata_downgrade_rejects_unregistered_quorum_controller() {
        let mut image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: crabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
            level: crate::features::METADATA_VERSION_MAX,
        }));
        image.apply(&MetadataRecord::V1Voters(crabka_metadata::VotersRecord {
            voters: crabka_metadata::voters::VoterSet::from_voters([
                crabka_metadata::voters::Voter {
                    id: crabka_metadata::NodeId(3),
                    directory_id: uuid::Uuid::from_u128(3),
                    endpoints: vec![],
                    kraft_version: crabka_metadata::voters::KRaftVersionRange::default(),
                },
            ]),
        }));

        let (results, records) = validate_updates(
            &validate_only(vec![metadata_update(
                crabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL,
                UPGRADE_TYPE_SAFE_DOWNGRADE,
            )]),
            &image,
            VERSION,
        );

        assert!(results[0].error_code == codes::INVALID_UPDATE_VERSION);
        assert!(
            results[0]
                .error_message
                .as_deref()
                .is_some_and(|message| message.contains("Controller 3 has not registered")),
            "{results:?}"
        );
        assert!(records.is_empty());
    }

    #[test]
    fn downgrade_type_cannot_raise_a_finalized_feature() {
        let mut image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: crabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
            level: crabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL,
        }));
        let (results, records) = validate_updates(
            &validate_only(vec![metadata_update(
                crabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL + 1,
                UPGRADE_TYPE_SAFE_DOWNGRADE,
            )]),
            &image,
            VERSION,
        );

        assert!(results[0].error_code == codes::INVALID_UPDATE_VERSION);
        assert!(
            results[0]
                .error_message
                .as_deref()
                .is_some_and(|message| message.contains("newer"))
        );
        assert!(records.is_empty());
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
    async fn handle_accepts_lossless_safe_metadata_downgrade() {
        let req = validate_only(vec![metadata_update(
            crabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL,
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
    async fn handle_rejects_metadata_downgrade_below_online_floor() {
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
            "Online metadata.version downgrade",
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_online_metadata_version_deletion() {
        let req = validate_only(vec![metadata_update(0, 2)]);

        let (resp, broker_handle, _dir) = Box::pin(call_with(
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            req,
        ))
        .await;

        assert_row_error(
            &resp,
            crate::features::METADATA_VERSION,
            "Online metadata.version downgrade",
        );
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
