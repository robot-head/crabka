//! `AlterShareGroupOffsets` (`api_key` 91) — KIP-932. Resets the
//! share-partition start offset (SPSO) for the requested partitions of an
//! *empty* share group, bumping the state epoch and re-initializing the
//! persister state. A non-empty group is rejected top-level with
//! `NON_EMPTY_GROUP`.
//!
//! Intercepted inline in `network::dispatch` for the per-group `Alter` ACL
//! gate (principal + peer `SocketAddr`).

use bytes::{Bytes, BytesMut};
use tokio::sync::oneshot;

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::alter_share_group_offsets_request::AlterShareGroupOffsetsRequest;
use crabka_protocol::owned::alter_share_group_offsets_response::{
    AlterShareGroupOffsetsResponse, AlterShareGroupOffsetsResponsePartition,
    AlterShareGroupOffsetsResponseTopic,
};
use crabka_protocol::primitives::uuid::Uuid;
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::unified::share::actor::ShareGroupActorMessage;
use crate::error::BrokerError;

#[tracing::instrument(
    name = "handle_alter_share_group_offsets",
    level = "info",
    skip_all,
    fields(api = "AlterShareGroupOffsets", version, req_bytes = req_bytes.len()),
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
    let req = AlterShareGroupOffsetsRequest::decode(&mut cur, version)?;

    // Feature gate: a broker with share groups disabled does not implement the RPC.
    if !broker.config.share_group.enable {
        return encode_top_level(version, codes::UNSUPPORTED_VERSION);
    }

    let image = broker.controller.current_image();
    let ng_opt = Some(broker.group_coordinator.clone());
    let gid = req.group_id;

    // ── ACL preamble ────────────────────────────────────
    // Per-group `Alter` check. On Deny → top-level `error_code = 30`.
    let acl_req = AuthorizationRequest {
        principal: ctx.principal,
        host: ctx.peer,
        resource_type: ResourceType::Group,
        resource_name: gid.as_str(),
        operation: AclOperation::Alter,
    };
    if broker.config.authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
        return encode_top_level(version, codes::GROUP_AUTHORIZATION_FAILED);
    }

    let Some(persister) = ng_opt.as_ref().and_then(|ng| ng.share_persister().cloned()) else {
        return encode_top_level(version, codes::COORDINATOR_NOT_AVAILABLE);
    };

    // Empty-group check: only an empty group may have its offsets reset. An
    // absent actor is treated as empty.
    if !group_is_empty(ng_opt.as_ref(), &gid).await {
        return encode_top_level(version, codes::NON_EMPTY_GROUP);
    }

    let mut responses: Vec<AlterShareGroupOffsetsResponseTopic> =
        Vec::with_capacity(req.topics.len());

    for rt in req.topics {
        let topic_name = rt.topic_name;
        let topic_id = image.topic(&topic_name).map(|t| t.topic_id);

        let mut partitions: Vec<AlterShareGroupOffsetsResponsePartition> =
            Vec::with_capacity(rt.partitions.len());

        for rp in rt.partitions {
            let Some(topic_id) = topic_id else {
                partitions.push(AlterShareGroupOffsetsResponsePartition {
                    partition_index: rp.partition_index,
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    ..Default::default()
                });
                continue;
            };

            // Bump the state epoch off the current durable value, then
            // re-initialize at the requested start offset. On success drop the
            // local acquisition-state cell so the next ShareFetch re-reads the
            // new SPSO.
            let error_code = match reset_partition(
                &persister,
                &gid,
                topic_id,
                rp.partition_index,
                rp.start_offset,
            )
            .await
            {
                Ok(()) => {
                    broker
                        .share_partition_leaders
                        .invalidate(&gid, topic_id, rp.partition_index);
                    codes::NONE
                }
                Err(()) => codes::COORDINATOR_NOT_AVAILABLE,
            };

            partitions.push(AlterShareGroupOffsetsResponsePartition {
                partition_index: rp.partition_index,
                error_code,
                ..Default::default()
            });
        }

        responses.push(AlterShareGroupOffsetsResponseTopic {
            topic_name,
            topic_id: topic_id.map_or_else(Uuid::default, |id| Uuid(*id.as_bytes())),
            partitions,
            ..Default::default()
        });
    }

    let resp = AlterShareGroupOffsetsResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        responses,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

/// Read the current durable state epoch for `(group, topic_id, partition)`,
/// then re-initialize the share state at `start_offset` with epoch+1. `Err(())`
/// on any persister failure (mapped to `COORDINATOR_NOT_AVAILABLE`).
async fn reset_partition(
    persister: &crate::share_coordinator::persister_client::SharePersister,
    gid: &str,
    topic_id: uuid::Uuid,
    partition: i32,
    start_offset: i64,
) -> Result<(), ()> {
    let cur_epoch = persister
        .read_state(gid, topic_id, partition)
        .await
        .map_err(|_| ())?
        .map_or(0, |s| s.state_epoch);
    persister
        .initialize(gid, topic_id, partition, cur_epoch + 1, start_offset)
        .await
        .map_err(|_| ())
}

fn encode_top_level(version: i16, error_code: i16) -> Result<Bytes, BrokerError> {
    let resp = AlterShareGroupOffsetsResponse {
        throttle_time_ms: 0,
        error_code,
        responses: Vec::new(),
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

/// Returns `true` when the share group has no live members (or no actor at
/// all). Drives the empty-group gate for offset reset/delete.
pub(crate) async fn group_is_empty(
    ng: Option<&std::sync::Arc<crate::coordinator::unified::GroupCoordinator>>,
    gid: &str,
) -> bool {
    let Some(handle) = ng.and_then(|ng| ng.find_share(gid)) else {
        return true;
    };
    let (tx, rx) = oneshot::channel();
    if handle
        .tx
        .send(ShareGroupActorMessage::Describe { reply: tx })
        .await
        .is_err()
    {
        // Actor gone → no live members.
        return true;
    }
    match rx.await {
        Ok(view) => view.members.is_empty(),
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_protocol::owned::alter_share_group_offsets_request::{
        AlterShareGroupOffsetsRequestPartition, AlterShareGroupOffsetsRequestTopic,
    };
    use crabka_protocol::owned::alter_share_group_offsets_response;
    use crabka_protocol::owned::share_group_heartbeat_request::ShareGroupHeartbeatRequest;
    use crabka_security::{AuthMethod, Principal};
    use std::net::SocketAddr;
    use std::sync::Arc;

    use crate::authorizer::{AuthorizationRequest, Authorizer};
    use crate::config::BrokerConfig;
    use crate::coordinator::unified::share::actor::ShareGroupActorMessage;

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

    fn request(
        group_id: &str,
        topic_name: &str,
        partitions: &[i32],
    ) -> AlterShareGroupOffsetsRequest {
        AlterShareGroupOffsetsRequest {
            group_id: group_id.into(),
            topics: vec![AlterShareGroupOffsetsRequestTopic {
                topic_name: topic_name.into(),
                partitions: partitions
                    .iter()
                    .map(|partition_index| AlterShareGroupOffsetsRequestPartition {
                        partition_index: *partition_index,
                        start_offset: 42,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn encode_request(req: &AlterShareGroupOffsetsRequest) -> Bytes {
        let version = alter_share_group_offsets_response::MAX_VERSION;
        let mut buf = BytesMut::with_capacity(req.encoded_len(version));
        req.encode(&mut buf, version).expect("encode request");
        buf.freeze()
    }

    fn decode_response(bytes: &Bytes) -> AlterShareGroupOffsetsResponse {
        let version = alter_share_group_offsets_response::MAX_VERSION;
        let mut cur: &[u8] = bytes.as_ref();
        let resp =
            AlterShareGroupOffsetsResponse::decode(&mut cur, version).expect("decode response");
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
            alter_share_group_offsets_response::MAX_VERSION,
            codes::UNSUPPORTED_VERSION,
        )
        .expect("encode");
        let resp = decode_response(&resp);

        assert!(resp.throttle_time_ms == 0);
        assert!(resp.error_code == codes::UNSUPPORTED_VERSION);
        assert!(resp.responses.is_empty());
    }

    #[tokio::test]
    async fn handle_disabled_feature_returns_top_level_unsupported_version() {
        let version = alter_share_group_offsets_response::MAX_VERSION;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer), false).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req_bytes = encode_request(&request("g1", "missing", &[0]));

        let resp = handle(&broker, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        assert!(resp.throttle_time_ms == 0);
        assert!(resp.error_code == codes::UNSUPPORTED_VERSION);
        assert!(resp.responses.is_empty());
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_denied_group_returns_top_level_authorization_failure() {
        let version = alter_share_group_offsets_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll), true).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req_bytes = encode_request(&request("g1", "missing", &[0]));

        let resp = handle(&broker, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        assert!(resp.throttle_time_ms == 0);
        assert!(resp.error_code == codes::GROUP_AUTHORIZATION_FAILED);
        assert!(resp.responses.is_empty());
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_unknown_topic_preserves_topic_and_partition_fields() {
        let version = alter_share_group_offsets_response::MAX_VERSION;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer), true).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req_bytes = encode_request(&request("g1", "missing-topic", &[3, 5]));

        let resp = handle(&broker, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        assert!(resp.throttle_time_ms == 0);
        assert!(resp.error_code == codes::NONE);
        assert!(resp.responses.len() == 1, "{resp:?}");
        let topic = &resp.responses[0];
        assert!(topic.topic_name == "missing-topic");
        assert!(topic.topic_id == Uuid::default());
        assert!(topic.partitions.len() == 2, "{topic:?}");
        assert!(topic.partitions[0].partition_index == 3);
        assert!(topic.partitions[0].error_code == codes::UNKNOWN_TOPIC_OR_PARTITION);
        assert!(topic.partitions[1].partition_index == 5);
        assert!(topic.partitions[1].error_code == codes::UNKNOWN_TOPIC_OR_PARTITION);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn group_is_empty_distinguishes_absent_and_live_share_groups() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer), true).await;
        let broker = broker_handle.broker_arc_for_test();
        let coordinator = broker.group_coordinator.clone();

        assert!(group_is_empty(Some(&coordinator), "absent").await);

        coordinator.mark_share("busy");
        let actor = coordinator.get_or_create_share("busy");
        let (tx, rx) = tokio::sync::oneshot::channel();
        actor
            .tx
            .send(ShareGroupActorMessage::Heartbeat {
                request: ShareGroupHeartbeatRequest {
                    group_id: "busy".into(),
                    member_id: "member-1".into(),
                    member_epoch: 0,
                    subscribed_topic_names: Some(Vec::new()),
                    ..Default::default()
                },
                client_host: "127.0.0.1".into(),
                reply: tx,
            })
            .await
            .expect("send heartbeat");
        let resp = rx.await.expect("heartbeat response");
        assert!(resp.error_code == codes::NONE, "{resp:?}");

        assert!(!group_is_empty(Some(&coordinator), "busy").await);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn reset_partition_bumps_existing_state_epoch_and_start_offset() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer), true).await;
        let broker = broker_handle.broker_arc_for_test();
        let persister = broker
            .group_coordinator
            .share_persister()
            .cloned()
            .expect("share persister");
        let topic_id = uuid::Uuid::from_u128(0xABCD);

        persister
            .initialize("g-reset", topic_id, 0, 4, 10)
            .await
            .expect("seed share state");
        reset_partition(&persister, "g-reset", topic_id, 0, 33)
            .await
            .expect("reset partition");
        let state = persister
            .read_state("g-reset", topic_id, 0)
            .await
            .expect("read state")
            .expect("state present");

        assert!(state.state_epoch == 5);
        assert!(state.start_offset == 33);
        broker_handle.shutdown().await;
    }
}
