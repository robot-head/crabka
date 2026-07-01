//! `DeleteShareGroupOffsets` (`api_key` 92) — KIP-932. Deletes the durable
//! share-state for every initialized partition of the requested topics of an
//! *empty* share group. A non-empty group is rejected top-level with
//! `NON_EMPTY_GROUP`.
//!
//! The request carries only `topic_name` per topic (no partition list), so the
//! handler enumerates the group's initialized partitions for each topic from
//! the cached `ShareGroupStatePartitionMetadata`.
//!
//! Intercepted inline in `network::dispatch` for the per-group `Delete` ACL
//! gate (principal + peer `SocketAddr`).

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::delete_share_group_offsets_request::DeleteShareGroupOffsetsRequest;
use crabka_protocol::owned::delete_share_group_offsets_response::{
    DeleteShareGroupOffsetsResponse, DeleteShareGroupOffsetsResponseTopic,
};
use crabka_protocol::primitives::uuid::Uuid;
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::handlers::alter_share_group_offsets::group_is_empty;

#[tracing::instrument(
    name = "handle_delete_share_group_offsets",
    level = "info",
    skip_all,
    fields(api = "DeleteShareGroupOffsets", version, req_bytes = req_bytes.len()),
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
    let req = DeleteShareGroupOffsetsRequest::decode(&mut cur, version)?;

    // Feature gate: a broker with share groups disabled does not implement the RPC.
    if !broker.config.share_group.enable {
        return encode_top_level(version, codes::UNSUPPORTED_VERSION);
    }

    let image = broker.controller.current_image();
    let ng_opt = Some(broker.group_coordinator.clone());
    let gid = req.group_id;

    // ── ACL preamble ────────────────────────────────────
    // Per-group `Delete` check. On Deny → top-level `error_code = 30`.
    let acl_req = AuthorizationRequest {
        principal: ctx.principal,
        host: ctx.peer,
        resource_type: ResourceType::Group,
        resource_name: gid.as_str(),
        operation: AclOperation::Delete,
    };
    if broker.config.authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
        return encode_top_level(version, codes::GROUP_AUTHORIZATION_FAILED);
    }

    let Some(persister) = ng_opt.as_ref().and_then(|ng| ng.share_persister().cloned()) else {
        return encode_top_level(version, codes::COORDINATOR_NOT_AVAILABLE);
    };

    // Empty-group check: only an empty group may have its offsets deleted. An
    // absent actor is treated as empty.
    if !group_is_empty(ng_opt.as_ref(), &gid).await {
        return encode_top_level(version, codes::NON_EMPTY_GROUP);
    }

    let metadata = ng_opt
        .as_ref()
        .and_then(|ng| ng.share_state_partition_metadata(&gid));

    let mut responses: Vec<DeleteShareGroupOffsetsResponseTopic> =
        Vec::with_capacity(req.topics.len());

    for rt in req.topics {
        let topic_name = rt.topic_name;

        let Some(topic_id) = image.topic(&topic_name).map(|t| t.topic_id) else {
            responses.push(DeleteShareGroupOffsetsResponseTopic {
                topic_name,
                topic_id: Uuid::default(),
                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                ..Default::default()
            });
            continue;
        };

        // Enumerate the group's initialized partitions for this topic.
        let part_indices: Vec<i32> = metadata
            .as_ref()
            .and_then(|m| {
                m.initialized
                    .iter()
                    .find(|(tid, _)| *tid == topic_id)
                    .map(|(_, parts)| parts.clone())
            })
            .unwrap_or_default();

        let mut error_code = codes::NONE;
        for p in part_indices {
            match persister.delete(&gid, topic_id, p).await {
                Ok(()) => {
                    broker.share_partition_leaders.invalidate(&gid, topic_id, p);
                }
                Err(_) => error_code = codes::COORDINATOR_NOT_AVAILABLE,
            }
        }

        // KIP-932 lifecycle: drop the topic from the group's v14
        // `ShareGroupStatePartitionMetadata` so it stays absent across restart.
        // Best-effort: a send/await failure must not fail the delete.
        if error_code == codes::NONE
            && let Some(ng) = ng_opt.as_ref()
            && let Some(handle) = ng.find_share(&gid)
        {
            let (tx, rx) = tokio::sync::oneshot::channel();
            handle
                .tx
                .send(
                    crate::coordinator::unified::share::actor::ShareGroupActorMessage::DropTopicMetadata {
                        topic_id,
                        reply: tx,
                    },
                )
                .await
                .ok();
            let _ = rx.await;
        }

        responses.push(DeleteShareGroupOffsetsResponseTopic {
            topic_name,
            topic_id: Uuid(*topic_id.as_bytes()),
            error_code,
            ..Default::default()
        });
    }

    let resp = DeleteShareGroupOffsetsResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        responses,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

fn encode_top_level(version: i16, error_code: i16) -> Result<Bytes, BrokerError> {
    let resp = DeleteShareGroupOffsetsResponse {
        throttle_time_ms: 0,
        error_code,
        responses: Vec::new(),
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_protocol::UnknownTaggedFields;
    use crabka_protocol::owned::delete_share_group_offsets_request::DeleteShareGroupOffsetsRequestTopic;
    use crabka_protocol::owned::delete_share_group_offsets_response;
    use crabka_security::{AuthMethod, Principal};
    use std::net::SocketAddr;
    use std::sync::Arc;

    use crate::authorizer::{AuthorizationRequest, Authorizer};
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

    fn request(group_id: &str, topics: &[&str]) -> DeleteShareGroupOffsetsRequest {
        DeleteShareGroupOffsetsRequest {
            group_id: group_id.into(),
            topics: topics
                .iter()
                .map(|topic_name| DeleteShareGroupOffsetsRequestTopic {
                    topic_name: (*topic_name).into(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn encode_request(req: &DeleteShareGroupOffsetsRequest) -> Bytes {
        let version = delete_share_group_offsets_response::MAX_VERSION;
        let mut buf = BytesMut::with_capacity(req.encoded_len(version));
        req.encode(&mut buf, version).expect("encode request");
        buf.freeze()
    }

    fn decode_response(bytes: &Bytes) -> DeleteShareGroupOffsetsResponse {
        let version = delete_share_group_offsets_response::MAX_VERSION;
        let mut cur: &[u8] = bytes.as_ref();
        let resp =
            DeleteShareGroupOffsetsResponse::decode(&mut cur, version).expect("decode response");
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
        share_enabled: bool,
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
        cfg.authorizer = authorizer;
        cfg.share_group.enable = share_enabled;
        let handle = Broker::start(cfg).await.expect("start broker");
        (handle, dir)
    }

    fn principal() -> Principal {
        Principal {
            name: "alice".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        }
    }

    #[test]
    fn encode_top_level_preserves_error_fields() {
        let resp = encode_top_level(
            delete_share_group_offsets_response::MAX_VERSION,
            codes::UNSUPPORTED_VERSION,
        )
        .expect("encode");
        let resp = decode_response(&resp);

        let expected = DeleteShareGroupOffsetsResponse {
            throttle_time_ms: 0,
            error_code: codes::UNSUPPORTED_VERSION,
            error_message: None,
            responses: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
    }

    #[tokio::test]
    async fn handle_disabled_feature_returns_top_level_unsupported_version() {
        let version = delete_share_group_offsets_response::MAX_VERSION;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer), false).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req_bytes = encode_request(&request("g1", &["missing"]));

        let resp = handle(&broker, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        let expected = DeleteShareGroupOffsetsResponse {
            throttle_time_ms: 0,
            error_code: codes::UNSUPPORTED_VERSION,
            error_message: None,
            responses: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_denied_group_returns_top_level_authorization_failure() {
        let version = delete_share_group_offsets_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll), true).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req_bytes = encode_request(&request("g1", &["missing"]));

        let resp = handle(&broker, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        let expected = DeleteShareGroupOffsetsResponse {
            throttle_time_ms: 0,
            error_code: codes::GROUP_AUTHORIZATION_FAILED,
            error_message: None,
            responses: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_unknown_topic_preserves_topic_fields() {
        let version = delete_share_group_offsets_response::MAX_VERSION;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer), true).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req_bytes = encode_request(&request("g1", &["missing-topic"]));

        let resp = handle(&broker, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        let expected = DeleteShareGroupOffsetsResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            error_message: None,
            responses: vec![DeleteShareGroupOffsetsResponseTopic {
                topic_name: "missing-topic".into(),
                topic_id: Uuid::default(),
                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                error_message: None,
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            }],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
