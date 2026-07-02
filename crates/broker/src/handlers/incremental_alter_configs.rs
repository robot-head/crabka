//! `IncrementalAlterConfigs` (`api_key=44`). Same target as `AlterConfigs`
//! (a single `V1TopicConfig` record per resource) but the wire request
//! carries per-key operations (SET/DELETE/APPEND/SUBTRACT). The handler
//! reads the current overrides from the metadata image, applies the ops,
//! validates the result, and submits the merged map.
//!
//! Supported operations:
//! - SET (0)    — set/replace
//! - DELETE (1) — remove
//! - APPEND (2) and SUBTRACT (3) are list-valued operations. None of our
//!   whitelisted keys are list-valued, so we reject these with
//!   `INVALID_CONFIG`.

use bytes::{Bytes, BytesMut};

use crabka_metadata::{
    AclOperation, BrokerConfigRecord, ClientMetricsConfigRecord, MetadataImage, MetadataRecord,
    NodeId, ResourceType, TopicConfigRecord,
};
use crabka_protocol::owned::incremental_alter_configs_request::{
    AlterConfigsResource, IncrementalAlterConfigsRequest,
};
use crabka_protocol::owned::incremental_alter_configs_response::{
    AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
};
use crabka_protocol::{Decode, Encode};
use crabka_raft::RaftError;

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::config_keys;
use crate::error::BrokerError;

const RESOURCE_TYPE_TOPIC: i8 = 2;
const RESOURCE_TYPE_BROKER: i8 = 4;
const RESOURCE_TYPE_CLIENT_METRICS: i8 = 16;
const OP_SET: i8 = 0;
const OP_DELETE: i8 = 1;

/// Returns `true` if `name` is a broker-scoped config key accepted by this
/// broker. We accept the two KIP-73 throttle rate keys (which we persist) plus
/// `replica.alter.log.dirs.io.max.bytes.per.second` which Kafka's
/// `kafka-reassign-partitions --verify` also clears. The third key is silently
/// accepted but not stored (we have no log-dir throttle implementation).
fn is_known_broker_config(name: &str) -> bool {
    matches!(
        name,
        crate::throttle::LEADER_THROTTLED_RATE_KEY
            | crate::throttle::FOLLOWER_THROTTLED_RATE_KEY
            | "replica.alter.log.dirs.io.max.bytes.per.second"
    )
}

/// Validates the value for a broker-scoped config key.
/// Returns `Err` if the key is unknown or the value cannot be parsed as an
/// `i64`.
fn validate_broker_config_value(name: &str, value: &str) -> Result<(), String> {
    match name {
        crate::throttle::LEADER_THROTTLED_RATE_KEY
        | crate::throttle::FOLLOWER_THROTTLED_RATE_KEY => value
            .parse::<i64>()
            .map(|_| ())
            .map_err(|e| format!("invalid rate: {e}")),
        _ => Err(format!("unknown broker config {name}")),
    }
}

#[allow(clippy::too_many_lines)]
#[tracing::instrument(
    name = "handle_incremental_alter_configs",
    level = "info",
    skip_all,
    fields(api = "IncrementalAlterConfigs", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = IncrementalAlterConfigsRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();
    let mut responses: Vec<AlterConfigsResourceResponse> = Vec::with_capacity(req.resources.len());

    for resource in req.resources {
        let mut out = AlterConfigsResourceResponse {
            resource_type: resource.resource_type,
            resource_name: resource.resource_name.clone(),
            error_code: codes::NONE,
            error_message: None,
            ..Default::default()
        };

        // ── ACL preamble ────────────────────────────────────────
        // Per-resource authorization based on resource_type.
        // Topic (2) → AlterConfigs on Topic(resource_name) → TOPIC_AUTHORIZATION_FAILED on Deny.
        // Broker (4) → AlterConfigs on Cluster("kafka-cluster") → CLUSTER_AUTHORIZATION_FAILED on Deny.
        // Other resource types are unsupported (INVALID_RESOURCE_TYPE) — checked after ACL.
        let acl_result = match resource.resource_type {
            RESOURCE_TYPE_TOPIC => broker.config.authorizer.authorize(
                &*image,
                &AuthorizationRequest {
                    principal: ctx.principal,
                    host: ctx.peer,
                    resource_type: ResourceType::Topic,
                    resource_name: &resource.resource_name,
                    operation: AclOperation::AlterConfigs,
                },
            ),
            RESOURCE_TYPE_BROKER | RESOURCE_TYPE_CLIENT_METRICS => {
                broker.config.authorizer.authorize(
                    &*image,
                    &AuthorizationRequest {
                        principal: ctx.principal,
                        host: ctx.peer,
                        resource_type: ResourceType::Cluster,
                        resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
                        operation: AclOperation::AlterConfigs,
                    },
                )
            }
            _ => {
                out.error_code = codes::INVALID_RESOURCE_TYPE;
                out.error_message = Some(format!(
                    "resource_type={} not supported",
                    resource.resource_type
                ));
                responses.push(out);
                continue;
            }
        };
        if acl_result == AuthorizationResult::Deny {
            out.error_code = match resource.resource_type {
                RESOURCE_TYPE_TOPIC => codes::TOPIC_AUTHORIZATION_FAILED,
                _ => codes::CLUSTER_AUTHORIZATION_FAILED,
            };
            responses.push(out);
            continue;
        }

        // After ACL pass: dispatch by resource type.
        let mut to_submit: Vec<MetadataRecord> = Vec::new();

        match resource.resource_type {
            RESOURCE_TYPE_TOPIC => {
                if image.topic(&resource.resource_name).is_none() {
                    out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
                    out.error_message = Some(format!("unknown topic `{}`", resource.resource_name));
                    responses.push(out);
                    continue;
                }

                // Start from the current overrides; apply each op in order.
                let mut merged = image
                    .topic_config(&resource.resource_name)
                    .cloned()
                    .unwrap_or_default();
                let mut validation_err: Option<String> = None;
                for cfg in &resource.configs {
                    match cfg.config_operation {
                        OP_SET => {
                            let value = cfg.value.clone().unwrap_or_default();
                            if let Err(reason) =
                                config_keys::validate_topic_config(&cfg.name, &value)
                            {
                                validation_err = Some(reason);
                                break;
                            }
                            merged.insert(cfg.name.clone(), value);
                        }
                        OP_DELETE => {
                            if !config_keys::is_recognized(&cfg.name) {
                                validation_err =
                                    Some(format!("unrecognized config key `{}`", cfg.name));
                                break;
                            }
                            merged.remove(&cfg.name);
                        }
                        op => {
                            validation_err = Some(format!(
                                "config_operation={op} (APPEND/SUBTRACT) not supported for key \
                                 `{}` — only SET and DELETE are honored on this broker",
                                cfg.name
                            ));
                            break;
                        }
                    }
                }
                if let Some(reason) = validation_err {
                    out.error_code = codes::INVALID_CONFIG;
                    out.error_message = Some(reason);
                    responses.push(out);
                    continue;
                }

                to_submit.push(MetadataRecord::V1TopicConfig(TopicConfigRecord {
                    topic: resource.resource_name.clone(),
                    overrides: merged,
                }));
            }
            RESOURCE_TYPE_BROKER => {
                handle_broker_scoped(&resource, &image, &mut out, &mut to_submit);
                if out.error_code != codes::NONE {
                    responses.push(out);
                    continue;
                }
            }
            RESOURCE_TYPE_CLIENT_METRICS => {
                handle_client_metrics_scoped(&resource, &image, &mut out, &mut to_submit);
                if out.error_code != codes::NONE {
                    responses.push(out);
                    continue;
                }
            }
            _ => {
                // Already handled by the ACL match above (unreachable), but be
                // explicit for exhaustiveness.
                out.error_code = codes::INVALID_RESOURCE_TYPE;
                out.error_message = Some(format!(
                    "resource_type={} not supported",
                    resource.resource_type
                ));
                responses.push(out);
                continue;
            }
        }

        if req.validate_only {
            // Validation pass already happened above (per-config loop). Nothing
            // to submit; the response already carries the per-resource result
            // (NONE if all configs validated, INVALID_CONFIG with reason on any
            // rejection). This matches Apache Kafka's --dry-run behavior.
            responses.push(out);
            continue;
        }
        match broker.controller.submit_change(to_submit).await {
            Ok(()) => {}
            Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                out.error_code = codes::NOT_CONTROLLER;
            }
            Err(e) => {
                tracing::error!(error = %e, "IncrementalAlterConfigs submit_change failed");
                out.error_code = codes::UNKNOWN_SERVER_ERROR;
            }
        }
        responses.push(out);
    }

    let resp = IncrementalAlterConfigsResponse {
        responses,
        throttle_time_ms: 0,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

fn handle_broker_scoped(
    resource: &AlterConfigsResource,
    image: &MetadataImage,
    out: &mut AlterConfigsResourceResponse,
    to_submit: &mut Vec<MetadataRecord>,
) {
    // Empty resource_name = cluster-wide default; not currently supported.
    if resource.resource_name.is_empty() {
        out.error_code = codes::INVALID_REQUEST;
        out.error_message = Some("cluster-wide broker config not supported".into());
        return;
    }
    let node_id: NodeId = if let Ok(n) = resource.resource_name.parse() {
        n
    } else {
        out.error_code = codes::INVALID_REQUEST;
        out.error_message = Some(format!("invalid broker id {:?}", resource.resource_name));
        return;
    };
    if image.broker(node_id).is_none() {
        out.error_code = codes::INVALID_REQUEST;
        out.error_message = Some(format!("unknown broker {node_id}"));
        return;
    }
    for cfg in &resource.configs {
        if !is_known_broker_config(&cfg.name) {
            out.error_code = codes::INVALID_CONFIG;
            out.error_message = Some(format!("unknown broker config {}", cfg.name));
            return; // halt processing this resource
        }
        // Keys we accept but don't persist (no implementation yet): silently
        // skip — just validate that the operation is valid.
        let persist = matches!(
            cfg.name.as_str(),
            crate::throttle::LEADER_THROTTLED_RATE_KEY
                | crate::throttle::FOLLOWER_THROTTLED_RATE_KEY
        );
        let new_value = match cfg.config_operation {
            OP_SET => {
                let v = cfg.value.clone().unwrap_or_default();
                if persist && let Err(e) = validate_broker_config_value(&cfg.name, &v) {
                    out.error_code = codes::INVALID_CONFIG;
                    out.error_message = Some(e);
                    return;
                }
                Some(v)
            }
            OP_DELETE => None,
            _ => {
                out.error_code = codes::INVALID_REQUEST;
                out.error_message = Some(format!(
                    "unsupported config_operation {}",
                    cfg.config_operation
                ));
                return;
            }
        };
        if persist {
            to_submit.push(MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id,
                config_name: cfg.name.clone(),
                config_value: new_value,
            }));
        }
    }
}

/// Merge per-key ops into a client-metrics subscription's override map and
/// stage a `V1ClientMetricsConfig` record. SET validates per KIP-714;
/// DELETE drops the override (effective value reverts to its default at
/// read time); APPEND/SUBTRACT are rejected.
fn handle_client_metrics_scoped(
    resource: &AlterConfigsResource,
    image: &MetadataImage,
    out: &mut AlterConfigsResourceResponse,
    to_submit: &mut Vec<MetadataRecord>,
) {
    if resource.resource_name.is_empty() {
        out.error_code = codes::INVALID_REQUEST;
        out.error_message = Some("client-metrics subscription name must not be empty".into());
        return;
    }
    let mut merged = image
        .client_metrics_config(&resource.resource_name)
        .cloned()
        .unwrap_or_default();
    for cfg in &resource.configs {
        match cfg.config_operation {
            OP_SET => {
                let value = cfg.value.clone().unwrap_or_default();
                if let Err(reason) = crate::client_metrics::config::validate(&cfg.name, &value) {
                    out.error_code = codes::INVALID_CONFIG;
                    out.error_message = Some(reason);
                    return;
                }
                merged.insert(cfg.name.clone(), value);
            }
            OP_DELETE => {
                if !crate::client_metrics::config::is_recognized(&cfg.name) {
                    out.error_code = codes::INVALID_CONFIG;
                    out.error_message = Some(format!("unrecognized config key `{}`", cfg.name));
                    return;
                }
                merged.remove(&cfg.name);
            }
            op => {
                out.error_code = codes::INVALID_CONFIG;
                out.error_message = Some(format!(
                    "config_operation={op} (APPEND/SUBTRACT) not supported for client-metrics key `{}`",
                    cfg.name
                ));
                return;
            }
        }
    }
    to_submit.push(MetadataRecord::V1ClientMetricsConfig(
        ClientMetricsConfigRecord {
            name: resource.resource_name.clone(),
            configs: merged,
        },
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn topic_throttle_config_value_validated() {
        // Verify ThrottledReplicas::parse rejects malformed input that
        // the validator delegates to.
        assert!(crate::throttle::ThrottledReplicas::parse("not-a-pair").is_err());
        assert!(crate::throttle::ThrottledReplicas::parse("0:bad").is_err());
    }

    #[test]
    fn broker_scoped_rate_config_accepted() {
        assert!(is_known_broker_config(
            crate::throttle::LEADER_THROTTLED_RATE_KEY
        ));
        assert!(is_known_broker_config(
            crate::throttle::FOLLOWER_THROTTLED_RATE_KEY
        ));
    }

    #[test]
    fn broker_scoped_unknown_config_rejected() {
        assert!(!is_known_broker_config("not.a.real.config"));
        assert!(validate_broker_config_value("not.a.real.config", "1024").is_err());
    }

    #[test]
    fn broker_scoped_invalid_value_rejected() {
        assert!(
            validate_broker_config_value(
                crate::throttle::LEADER_THROTTLED_RATE_KEY,
                "not-a-number"
            )
            .is_err()
        );
        assert!(
            validate_broker_config_value(crate::throttle::LEADER_THROTTLED_RATE_KEY, "1024")
                .is_ok()
        );
    }

    // ── handle_broker_scoped unit tests ─────────────────────────────────────

    use crabka_metadata::{BrokerRegistrationRecord, MetadataImage};
    use crabka_protocol::owned::incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig,
    };

    fn make_image_with_broker(node_id: NodeId) -> MetadataImage {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id,
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::nil(),
                host: "127.0.0.1".into(),
                port: 9092,
                rack: None,
                endpoints: vec![],
            },
        ));
        img
    }

    fn make_resource(name: &str, configs: Vec<AlterableConfig>) -> AlterConfigsResource {
        AlterConfigsResource {
            resource_type: RESOURCE_TYPE_BROKER,
            resource_name: name.into(),
            configs,
            ..Default::default()
        }
    }

    fn make_set_cfg(key: &str, value: &str) -> AlterableConfig {
        AlterableConfig {
            name: key.into(),
            config_operation: OP_SET,
            value: Some(value.into()),
            ..Default::default()
        }
    }

    fn make_del_cfg(key: &str) -> AlterableConfig {
        AlterableConfig {
            name: key.into(),
            config_operation: OP_DELETE,
            value: None,
            ..Default::default()
        }
    }

    #[test]
    fn broker_scoped_empty_name_returns_invalid_request() {
        let img = make_image_with_broker(1);
        let resource = make_resource("", vec![]);
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_broker_scoped(&resource, &img, &mut out, &mut to_submit);
        assert!(out.error_code == codes::INVALID_REQUEST);
        assert!(to_submit.is_empty());
    }

    #[test]
    fn broker_scoped_unknown_broker_returns_invalid_request() {
        let img = make_image_with_broker(1);
        let resource = make_resource("99", vec![]);
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_broker_scoped(&resource, &img, &mut out, &mut to_submit);
        assert!(out.error_code == codes::INVALID_REQUEST);
    }

    #[test]
    fn broker_scoped_unknown_config_key_returns_invalid_config() {
        let img = make_image_with_broker(1);
        let resource = make_resource("1", vec![make_set_cfg("some.unknown.key", "123")]);
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_broker_scoped(&resource, &img, &mut out, &mut to_submit);
        assert!(out.error_code == codes::INVALID_CONFIG);
        assert!(to_submit.is_empty());
    }

    #[test]
    fn broker_scoped_set_produces_broker_config_record() {
        let img = make_image_with_broker(1);
        let resource = make_resource(
            "1",
            vec![make_set_cfg(
                crate::throttle::LEADER_THROTTLED_RATE_KEY,
                "2048",
            )],
        );
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_broker_scoped(&resource, &img, &mut out, &mut to_submit);
        assert!(out.error_code == codes::NONE);
        assert!(to_submit.len() == 1);
        match &to_submit[0] {
            MetadataRecord::V1BrokerConfig(rec) => {
                assert!(rec.node_id == 1);
                assert!(rec.config_name == crate::throttle::LEADER_THROTTLED_RATE_KEY);
                assert!(rec.config_value == Some("2048".into()));
            }
            other => panic!("expected V1BrokerConfig, got {other:?}"),
        }
    }

    #[test]
    fn broker_scoped_delete_produces_broker_config_record_none_value() {
        let img = make_image_with_broker(1);
        let resource = make_resource(
            "1",
            vec![make_del_cfg(crate::throttle::FOLLOWER_THROTTLED_RATE_KEY)],
        );
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_broker_scoped(&resource, &img, &mut out, &mut to_submit);
        assert!(out.error_code == codes::NONE);
        assert!(to_submit.len() == 1);
        match &to_submit[0] {
            MetadataRecord::V1BrokerConfig(rec) => {
                assert!(rec.node_id == 1);
                assert!(rec.config_name == crate::throttle::FOLLOWER_THROTTLED_RATE_KEY);
                assert!(rec.config_value == None);
            }
            other => panic!("expected V1BrokerConfig, got {other:?}"),
        }
    }

    #[test]
    fn broker_scoped_invalid_rate_value_returns_invalid_config() {
        let img = make_image_with_broker(1);
        let resource = make_resource(
            "1",
            vec![make_set_cfg(
                crate::throttle::LEADER_THROTTLED_RATE_KEY,
                "not-a-number",
            )],
        );
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_broker_scoped(&resource, &img, &mut out, &mut to_submit);
        assert!(out.error_code == codes::INVALID_CONFIG);
        assert!(to_submit.is_empty());
    }

    #[test]
    fn client_metrics_set_produces_record() {
        use crabka_protocol::owned::incremental_alter_configs_request::AlterableConfig;
        let img = MetadataImage::new(uuid::Uuid::nil());
        let resource = AlterConfigsResource {
            resource_type: RESOURCE_TYPE_CLIENT_METRICS,
            resource_name: "sub-a".into(),
            configs: vec![
                AlterableConfig {
                    name: "interval.ms".into(),
                    config_operation: OP_SET,
                    value: Some("60000".into()),
                    ..Default::default()
                },
                AlterableConfig {
                    name: "metrics".into(),
                    config_operation: OP_SET,
                    value: Some("org.apache.kafka.consumer.".into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_client_metrics_scoped(&resource, &img, &mut out, &mut to_submit);
        assert!(out.error_code == codes::NONE);
        assert!(to_submit.len() == 1);
        match &to_submit[0] {
            MetadataRecord::V1ClientMetricsConfig(rec) => {
                assert!(rec.name == "sub-a");
                assert!(rec.configs.get("interval.ms").map(String::as_str) == Some("60000"));
            }
            other => panic!("expected V1ClientMetricsConfig, got {other:?}"),
        }
    }

    #[test]
    fn client_metrics_bad_interval_rejected() {
        use crabka_protocol::owned::incremental_alter_configs_request::AlterableConfig;
        let img = MetadataImage::new(uuid::Uuid::nil());
        let resource = AlterConfigsResource {
            resource_type: RESOURCE_TYPE_CLIENT_METRICS,
            resource_name: "sub-a".into(),
            configs: vec![AlterableConfig {
                name: "interval.ms".into(),
                config_operation: OP_SET,
                value: Some("5".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_client_metrics_scoped(&resource, &img, &mut out, &mut to_submit);
        assert!(out.error_code == codes::INVALID_CONFIG);
        assert!(to_submit.is_empty());
    }

    #[test]
    fn client_metrics_delete_drops_key() {
        use crabka_metadata::ClientMetricsConfigRecord;
        use crabka_protocol::owned::incremental_alter_configs_request::AlterableConfig;
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        let mut existing = std::collections::BTreeMap::new();
        existing.insert("interval.ms".to_string(), "60000".to_string());
        existing.insert("metrics".to_string(), "a.".to_string());
        img.apply(&MetadataRecord::V1ClientMetricsConfig(
            ClientMetricsConfigRecord {
                name: "sub-a".into(),
                configs: existing,
            },
        ));
        let resource = AlterConfigsResource {
            resource_type: RESOURCE_TYPE_CLIENT_METRICS,
            resource_name: "sub-a".into(),
            configs: vec![AlterableConfig {
                name: "interval.ms".into(),
                config_operation: OP_DELETE,
                value: None,
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_client_metrics_scoped(&resource, &img, &mut out, &mut to_submit);
        assert!(out.error_code == codes::NONE);
        match &to_submit[0] {
            MetadataRecord::V1ClientMetricsConfig(rec) => {
                assert!(!rec.configs.contains_key("interval.ms"));
                assert!(rec.configs.get("metrics").map(String::as_str) == Some("a."));
            }
            other => panic!("expected V1ClientMetricsConfig, got {other:?}"),
        }
    }
}
