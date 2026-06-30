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
        error_code: code,
        error_message: Some(msg.to_string()),
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
    use crabka_protocol::owned::update_features_request::FeatureUpdateKey;
    use crabka_security::{AuthMethod, Principal};
    use std::net::SocketAddr;
    use std::sync::Arc;

    use crate::authorizer::{AuthorizationRequest, Authorizer};
    use crate::broker::{Broker, BrokerHandle};
    use crate::config::BrokerConfig;

    const VERSION: i16 = 1;

    #[derive(Debug)]
    struct DenyAll;

    impl Authorizer for DenyAll {
        fn authorize(
            &self,
            _source: &dyn crabka_authz::AclSource,
            _req: &AuthorizationRequest<'_>,
        ) -> AuthorizationResult {
            AuthorizationResult::Deny
        }
    }

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
        Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        }
    }

    fn context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::handlers::RequestContext {
            principal,
            peer,
            client_id: "update-features-client",
            sendfile_capable: false,
            connection_listener_name: "PLAINTEXT",
        }
    }

    async fn start_broker(authorizer: Arc<dyn Authorizer>) -> (BrokerHandle, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
        cfg.audit_enabled = false;
        cfg.authorizer = authorizer;
        let handle = Broker::start(cfg).await.expect("start broker");
        (handle, dir)
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

        assert!(resp.throttle_time_ms == 0);
        assert!(resp.error_code == codes::INVALID_REQUEST);
        assert!(resp.error_message.as_deref() == Some("bad request"));
        assert!(resp.results.is_empty());
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

        assert!(resp.error_code == codes::FEATURE_UPDATE_FAILED);
        assert!(resp.error_message.as_deref() == Some("persist failed"));
        assert!(resp.results.len() == 3);
        assert!(resp.results[0].feature == "metadata.version");
        assert!(resp.results[0].error_code == codes::FEATURE_UPDATE_FAILED);
        assert!(resp.results[0].error_message.as_deref() == Some("persist failed"));
        assert!(resp.results[1].feature == "eligible.feature");
        assert!(resp.results[1].error_code == codes::FEATURE_UPDATE_FAILED);
        assert!(resp.results[1].error_message.as_deref() == Some("persist failed"));
        assert!(resp.results[2].feature == "not.a.feature");
        assert!(resp.results[2].error_code == codes::INVALID_REQUEST);
        assert!(resp.results[2].error_message.as_deref() == Some("bad feature"));
    }

    #[test]
    fn finalize_v2_promotes_first_error_to_top_level() {
        let results = vec![
            row("a".into(), codes::NONE, ""),
            row("b".into(), codes::INVALID_UPDATE_VERSION, "bad"),
        ];
        let resp = finalize(results, 2);
        assert!(resp.throttle_time_ms == 0);
        assert!(resp.error_code == codes::INVALID_UPDATE_VERSION);
        assert!(resp.error_message.as_deref() == Some("bad"));
        assert!(resp.results.len() == 2);
        assert!(resp.results[0].feature == "a");
        assert!(resp.results[1].feature == "b");
    }

    #[test]
    fn finalize_v1_keeps_top_level_none() {
        let results = vec![row("b".into(), codes::INVALID_UPDATE_VERSION, "bad")];
        let resp = finalize(results, 1);
        assert!(resp.error_code == codes::NONE);
        assert!(resp.error_message.is_none());
        assert!(resp.results.len() == 1);
        assert!(resp.results[0].feature == "b");
        assert!(resp.results[0].error_message.as_deref() == Some("bad"));
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

        let (resp, broker_handle, _dir) = call_with(Arc::new(DenyAll), req).await;

        assert!(
            resp.error_code == codes::CLUSTER_AUTHORIZATION_FAILED,
            "{resp:?}"
        );
        assert!(
            resp.error_message
                .as_deref()
                .is_some_and(|m| m.contains("Cluster authorization failed")),
            "{resp:?}"
        );
        assert!(resp.results.is_empty(), "{resp:?}");
        assert!(resp.throttle_time_ms == 0);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_empty_feature_updates() {
        let (resp, broker_handle, _dir) = call_with(
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            validate_only(Vec::new()),
        )
        .await;

        assert!(resp.error_code == codes::INVALID_REQUEST, "{resp:?}");
        assert!(
            resp.error_message
                .as_deref()
                .is_some_and(|m| m.contains("empty feature updates")),
            "{resp:?}"
        );
        assert!(resp.results.is_empty(), "{resp:?}");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_accepts_validate_only_metadata_version_at_supported_max() {
        let req = validate_only(vec![metadata_update(
            crate::features::METADATA_VERSION_MAX,
            1,
        )]);

        let (resp, broker_handle, _dir) =
            call_with(Arc::new(crate::authorizer::AllowAllAuthorizer), req).await;

        assert!(resp.error_code == codes::NONE, "{resp:?}");
        assert!(resp.error_message.is_none(), "{resp:?}");
        assert!(resp.results.len() == 1, "{resp:?}");
        assert_ok_row(&resp, crate::features::METADATA_VERSION);
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

        let (resp, broker_handle, _dir) =
            call_with(Arc::new(crate::authorizer::AllowAllAuthorizer), req).await;

        assert!(resp.error_code == codes::NONE, "{resp:?}");
        assert!(resp.results.len() == 2, "{resp:?}");
        assert!(resp.results[0].feature == crate::features::METADATA_VERSION);
        assert!(resp.results[0].error_code == codes::NONE, "{resp:?}");
        assert!(resp.results[1].feature == crate::features::METADATA_VERSION);
        assert!(
            resp.results[1].error_code == codes::INVALID_REQUEST,
            "{resp:?}"
        );
        assert!(
            resp.results[1]
                .error_message
                .as_deref()
                .is_some_and(|m| m.contains("more than once")),
            "{resp:?}"
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_negative_level_with_supported_range_message() {
        let req = validate_only(vec![metadata_update(-1, 1)]);

        let (resp, broker_handle, _dir) =
            call_with(Arc::new(crate::authorizer::AllowAllAuthorizer), req).await;

        assert_row_error(&resp, crate::features::METADATA_VERSION, "supported range");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_level_above_supported_max_with_range_message() {
        let req = validate_only(vec![metadata_update(
            crate::features::METADATA_VERSION_MAX + 1,
            1,
        )]);

        let (resp, broker_handle, _dir) =
            call_with(Arc::new(crate::authorizer::AllowAllAuthorizer), req).await;

        assert_row_error(&resp, crate::features::METADATA_VERSION, "supported range");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_accepts_floor_level_with_safe_downgrade() {
        let req = validate_only(vec![metadata_update(
            crate::features::METADATA_VERSION_MIN,
            2,
        )]);

        let (resp, broker_handle, _dir) =
            call_with(Arc::new(crate::authorizer::AllowAllAuthorizer), req).await;

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

        let (resp, broker_handle, _dir) =
            call_with(Arc::new(crate::authorizer::AllowAllAuthorizer), req).await;

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

        let (resp, broker_handle, _dir) =
            call_with(Arc::new(crate::authorizer::AllowAllAuthorizer), req).await;

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

        let (resp, broker_handle, _dir) =
            call_with(Arc::new(crate::authorizer::AllowAllAuthorizer), req).await;

        assert_row_error(&resp, crate::features::METADATA_VERSION, "downgrade");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_delete_zero_without_downgrade_flag() {
        let req = validate_only(vec![metadata_update(0, 1)]);

        let (resp, broker_handle, _dir) =
            call_with(Arc::new(crate::authorizer::AllowAllAuthorizer), req).await;

        assert_row_error(&resp, crate::features::METADATA_VERSION, "downgrade flag");
        broker_handle.shutdown().await;
    }
}
