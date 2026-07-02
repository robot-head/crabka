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
use crabka_protocol::{Decode, Encode, UnknownTaggedFields};
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
            unknown_tagged_fields: UnknownTaggedFields::default(),
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
                    resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
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
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_protocol::owned::alter_configs_request::{
        AlterConfigsRequest, AlterConfigsResource, AlterableConfig,
    };
    use crabka_security::{AuthMethod, Principal};
    use std::net::SocketAddr;
    use std::sync::Arc;

    use crate::authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer};
    use crate::config::BrokerConfig;

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

    fn encode_request(version: i16, req: &AlterConfigsRequest) -> Bytes {
        let mut buf = BytesMut::with_capacity(req.encoded_len(version));
        req.encode(&mut buf, version).expect("encode request");
        buf.freeze()
    }

    fn decode_response(version: i16, bytes: &Bytes) -> AlterConfigsResponse {
        let mut cur: &[u8] = bytes.as_ref();
        let resp = AlterConfigsResponse::decode(&mut cur, version).expect("decode response");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    fn test_context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::handlers::RequestContext {
            principal,
            peer,
            client_id: "admin-client",
            sendfile_capable: false,
            connection_listener_name: "PLAINTEXT",
        }
    }

    async fn start_broker(
        authorizer: Arc<dyn Authorizer>,
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
        cfg.authorizer = authorizer;
        let handle = Broker::start(cfg).await.expect("start broker");
        (handle, dir)
    }

    fn resource(resource_type: i8, resource_name: &str) -> AlterConfigsResource {
        AlterConfigsResource {
            resource_type,
            resource_name: resource_name.into(),
            configs: vec![AlterableConfig {
                name: "retention.ms".into(),
                value: Some("60000".into()),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    async fn drive_one(
        authorizer: Arc<dyn Authorizer>,
        resource: AlterConfigsResource,
    ) -> AlterConfigsResponse {
        let version = 2;
        let (broker_handle, _dir) = start_broker(authorizer).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req = AlterConfigsRequest {
            resources: vec![resource],
            validate_only: false,
            ..Default::default()
        };
        let req_bytes = encode_request(version, &req);

        let resp = handle(&broker, version, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(version, &resp);
        broker_handle.shutdown().await;
        resp
    }

    #[tokio::test]
    async fn handle_preserves_resource_identity_for_unsupported_type() {
        let resp = drive_one(
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            resource(77, "mystery"),
        )
        .await;

        let expected = AlterConfigsResponse {
            throttle_time_ms: 0,
            responses: vec![AlterConfigsResourceResponse {
                error_code: codes::INVALID_RESOURCE_TYPE,
                error_message: Some("resource_type=77 not supported".to_string()),
                resource_type: 77,
                resource_name: "mystery".to_string(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }

    #[tokio::test]
    async fn topic_resource_denial_uses_topic_authorization_error() {
        let resp = drive_one(Arc::new(DenyAll), resource(RESOURCE_TYPE_TOPIC, "orders")).await;

        let expected = AlterConfigsResponse {
            throttle_time_ms: 0,
            responses: vec![AlterConfigsResourceResponse {
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                error_message: None,
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: "orders".to_string(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }

    #[tokio::test]
    async fn broker_resource_denial_uses_cluster_authorization_error() {
        let resp = drive_one(Arc::new(DenyAll), resource(RESOURCE_TYPE_BROKER, "1")).await;

        let expected = AlterConfigsResponse {
            throttle_time_ms: 0,
            responses: vec![AlterConfigsResourceResponse {
                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                error_message: None,
                resource_type: RESOURCE_TYPE_BROKER,
                resource_name: "1".to_string(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }

    #[tokio::test]
    async fn authorized_broker_resource_is_reported_unsupported() {
        let resp = drive_one(
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            resource(RESOURCE_TYPE_BROKER, "1"),
        )
        .await;

        let expected = AlterConfigsResponse {
            throttle_time_ms: 0,
            responses: vec![AlterConfigsResourceResponse {
                error_code: codes::INVALID_RESOURCE_TYPE,
                error_message: Some("resource_type=4 not supported".to_string()),
                resource_type: RESOURCE_TYPE_BROKER,
                resource_name: "1".to_string(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }
}
