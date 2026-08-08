//! `DeleteShareGroupOffsets` (`api_key` 92), from KIP-932.
//!
//! It deletes the durable share state for every initialized partition of the
//! requested topics, in an *empty* share group. A non-empty group gets a
//! top-level `NON_EMPTY_GROUP` rejection.
//!
//! The request carries only `topic_name` for each topic, and no partition
//! list. The handler therefore lists the group's initialized partitions for
//! each topic from the cached `ShareGroupStatePartitionMetadata`.
//!
//! `network::dispatch` intercepts this request inline for the per-group
//! `Delete` ACL gate, which needs the principal and the peer `SocketAddr`.

use bytes::Bytes;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode,
    owned::{
        delete_share_group_offsets_request::DeleteShareGroupOffsetsRequest,
        delete_share_group_offsets_response::{
            DeleteShareGroupOffsetsResponse, DeleteShareGroupOffsetsResponseTopic,
        },
    },
    primitives::uuid::Uuid,
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::alter_share_group_offsets::group_is_empty,
};

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
    crate::handlers::encode_response(&resp, version)
}

fn encode_top_level(version: i16, error_code: i16) -> Result<Bytes, BrokerError> {
    let resp = DeleteShareGroupOffsetsResponse {
        throttle_time_ms: 0,
        error_code,
        responses: Vec::new(),
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use crabka_protocol::{
        UnknownTaggedFields,
        owned::{
            delete_share_group_offsets_request::DeleteShareGroupOffsetsRequestTopic,
            delete_share_group_offsets_response,
        },
    };
    use crabka_security::Principal;

    use super::*;
    use crate::{authorizer::Authorizer, test_support::DenyAll};

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

    crate::test_support::wire_helpers!(
        DeleteShareGroupOffsetsRequest,
        DeleteShareGroupOffsetsResponse,
        version = delete_share_group_offsets_response::MAX_VERSION,
        client_id = "admin-client"
    );

    async fn start_broker(
        authorizer: Arc<dyn Authorizer>,
        share_enabled: bool,
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|cfg| {
            cfg.authorizer = authorizer;
            cfg.share_group.enable = share_enabled;
        })
        .await
    }

    fn principal() -> Principal {
        crate::test_support::principal("alice")
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
    async fn handle_error_scenarios_preserve_expected_rows() {
        type Case<'a> = (
            &'a str,
            Arc<dyn Authorizer>,
            bool,
            Vec<&'a str>,
            DeleteShareGroupOffsetsResponse,
        );
        let version = delete_share_group_offsets_response::MAX_VERSION;
        let cases: Vec<Case<'_>> = vec![
            (
                "disabled feature returns top-level unsupported version",
                Arc::new(crate::authorizer::AllowAllAuthorizer),
                false,
                vec!["missing"],
                DeleteShareGroupOffsetsResponse {
                    throttle_time_ms: 0,
                    error_code: codes::UNSUPPORTED_VERSION,
                    error_message: None,
                    responses: Vec::new(),
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
            ),
            (
                "denied group returns top-level authorization failure",
                Arc::new(DenyAll),
                true,
                vec!["missing"],
                DeleteShareGroupOffsetsResponse {
                    throttle_time_ms: 0,
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    error_message: None,
                    responses: Vec::new(),
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
            ),
            (
                "unknown topic preserves topic fields",
                Arc::new(crate::authorizer::AllowAllAuthorizer),
                true,
                vec!["missing-topic"],
                DeleteShareGroupOffsetsResponse {
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
                },
            ),
        ];
        for (case, authorizer, share_enabled, topics, expected) in cases {
            let (broker_handle, _dir) = start_broker(authorizer, share_enabled).await;
            let broker = broker_handle.broker_arc_for_test();
            let principal = principal();
            let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
            let ctx = test_context(&principal, &peer);
            let req_bytes = encode_request(&request("g1", &topics));

            let resp = handle(&broker, version, 1, &req_bytes, &ctx)
                .await
                .expect("handle");
            let resp = decode_response(&resp);

            assert!(resp == expected, "case: {case}");
            broker_handle.shutdown().await;
        }
    }
}
