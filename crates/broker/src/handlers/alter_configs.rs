//! `AlterConfigs` (`api_key=33`). Topic-level only. Each resource's full
//! override map (the *complete* set of non-default values for that topic)
//! is built from the request, validated against the whitelist in
//! [`crate::config_keys`], and submitted through the controller as a
//! single `V1TopicConfig` record. Replication-side propagation runs on
//! every reconcile (see `ReplicatorSupervisor::reconcile`).

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, MetadataRecord, ResourceType, TopicConfigRecord};
use crabka_protocol::owned::alter_configs_request::AlterConfigsRequest;
use crabka_protocol::owned::alter_configs_response::{
    AlterConfigsResourceResponse, AlterConfigsResponse,
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

#[allow(clippy::too_many_lines)]
#[tracing::instrument(
    name = "handle_alter_configs",
    level = "info",
    skip_all,
    fields(api = "AlterConfigs", version, req_bytes = req_bytes.len()),
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
    let req = AlterConfigsRequest::decode(&mut cur, version)?;

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
            RESOURCE_TYPE_BROKER => broker.config.authorizer.authorize(
                &*image,
                &AuthorizationRequest {
                    principal: ctx.principal,
                    host: ctx.peer,
                    resource_type: ResourceType::Cluster,
                    resource_name: "kafka-cluster",
                    operation: AclOperation::AlterConfigs,
                },
            ),
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

        // After ACL pass: only Topic resources proceed to actual config change.
        // Broker resources are authorized above but we don't currently store broker
        // configs, so fall through to the unsupported check.
        if resource.resource_type != RESOURCE_TYPE_TOPIC {
            out.error_code = codes::INVALID_RESOURCE_TYPE;
            out.error_message = Some(format!(
                "resource_type={} not supported",
                resource.resource_type
            ));
            responses.push(out);
            continue;
        }

        if image.topic(&resource.resource_name).is_none() {
            out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
            out.error_message = Some(format!("unknown topic `{}`", resource.resource_name));
            responses.push(out);
            continue;
        }

        // AlterConfigs is FULL replacement semantics per Kafka:
        // the request's `configs` list IS the new target state for
        // this resource. Validate every entry; on first invalid key
        // surface INVALID_CONFIG and skip the submit.
        let mut overrides = std::collections::BTreeMap::new();
        let mut validation_err: Option<String> = None;
        for cfg in &resource.configs {
            let value = cfg.value.clone().unwrap_or_default();
            if let Err(reason) = config_keys::validate_topic_config(&cfg.name, &value) {
                validation_err = Some(reason);
                break;
            }
            overrides.insert(cfg.name.clone(), value);
        }
        if let Some(reason) = validation_err {
            out.error_code = codes::INVALID_CONFIG;
            out.error_message = Some(reason);
            responses.push(out);
            continue;
        }

        let record = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: resource.resource_name.clone(),
            overrides,
        });
        if req.validate_only {
            // Validation pass already happened above (per-config loop). Nothing
            // to submit; the response already carries the per-resource result
            // (NONE if all configs validated, INVALID_CONFIG with reason on any
            // rejection). This matches Apache Kafka's --dry-run behavior.
            responses.push(out);
            continue;
        }
        match broker.controller.submit_change(vec![record]).await {
            Ok(()) => {}
            Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                out.error_code = codes::NOT_CONTROLLER;
            }
            Err(e) => {
                tracing::error!(error = %e, "AlterConfigs submit_change failed");
                out.error_code = codes::UNKNOWN_SERVER_ERROR;
            }
        }
        responses.push(out);
    }

    let resp = AlterConfigsResponse {
        responses,
        throttle_time_ms: 0,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
