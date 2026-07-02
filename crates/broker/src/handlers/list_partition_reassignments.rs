//! `ListPartitionReassignments` (`api_key` 46, KIP-455).

#![allow(clippy::cast_possible_truncation, clippy::unused_async)]

use bytes::Bytes;
use crabka_metadata::{PartitionRecord, ResourceType};
use crabka_protocol::Encode;
use crabka_protocol::owned::list_partition_reassignments_request::ListPartitionReassignmentsRequest;
use crabka_protocol::owned::list_partition_reassignments_response::{
    ListPartitionReassignmentsResponse, OngoingPartitionReassignment, OngoingTopicReassignment,
};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes::{CLUSTER_AUTHORIZATION_FAILED, NONE};

#[tracing::instrument(
    name = "handle_list_partition_reassignments",
    level = "info",
    skip_all,
    fields(api = "ListPartitionReassignments"),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: ListPartitionReassignmentsRequest,
    ctx: &crate::handlers::RequestContext<'_>,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let image = broker.controller.current_image();
    let allow = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Cluster,
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            operation: crabka_metadata::AclOperation::Describe,
        },
    );
    if matches!(allow, AuthorizationResult::Deny) {
        let resp = ListPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("list-reassignment denied".into()),
            topics: vec![],
            ..Default::default()
        };
        return encode_response(&resp, api_version);
    }

    let in_flight: Vec<&PartitionRecord> = match &req.topics {
        None => image.reassignments_in_flight().collect(),
        Some(filter) => {
            let mut acc = Vec::new();
            for t in filter {
                let want_all = t.partition_indexes.is_empty();
                for pr in image.partitions_of(&t.name) {
                    if pr.adding_replicas.is_empty() && pr.removing_replicas.is_empty() {
                        continue;
                    }
                    if want_all || t.partition_indexes.contains(&pr.partition) {
                        acc.push(pr);
                    }
                }
            }
            acc
        }
    };

    // Group by topic name using BTreeMap for stable alphabetical ordering.
    let mut by_topic: std::collections::BTreeMap<String, Vec<OngoingPartitionReassignment>> =
        std::collections::BTreeMap::new();
    for pr in in_flight {
        by_topic
            .entry(pr.topic.clone())
            .or_default()
            .push(OngoingPartitionReassignment {
                partition_index: pr.partition,
                replicas: pr.replicas.iter().map(|n| *n as i32).collect(),
                adding_replicas: pr.adding_replicas.iter().map(|n| *n as i32).collect(),
                removing_replicas: pr.removing_replicas.iter().map(|n| *n as i32).collect(),
                ..Default::default()
            });
    }
    let topics: Vec<OngoingTopicReassignment> = by_topic
        .into_iter()
        .map(|(name, partitions)| OngoingTopicReassignment {
            name,
            partitions,
            ..Default::default()
        })
        .collect();
    let resp = ListPartitionReassignmentsResponse {
        throttle_time_ms: 0,
        error_code: NONE,
        error_message: None,
        topics,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let mut body = Vec::new();
    resp.encode(&mut body, api_version).map_err(|e| {
        crate::error::BrokerError::Replication(format!("encode ListPartitionReassignments: {e}"))
    })?;
    Ok(Bytes::from(body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_metadata::{MetadataRecord, TopicRecord};
    use crabka_protocol::Decode;
    use crabka_protocol::owned::list_partition_reassignments_request::ListPartitionReassignmentsTopics;
    use crabka_security::{AuthMethod, Principal};
    use std::net::SocketAddr;
    use std::sync::Arc;
    use uuid::Uuid;

    use crate::authorizer::Authorizer;
    use crate::broker::{Broker, BrokerHandle};
    use crate::config::BrokerConfig;

    const VERSION: i16 = crabka_protocol::owned::list_partition_reassignments_response::MAX_VERSION;

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

    fn decode_response(bytes: &Bytes) -> ListPartitionReassignmentsResponse {
        let mut cur: &[u8] = bytes.as_ref();
        let resp = ListPartitionReassignmentsResponse::decode(&mut cur, VERSION).expect("decode");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    fn principal(name: &str) -> Principal {
        Principal {
            name: name.into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        }
    }

    fn peer() -> SocketAddr {
        "127.0.0.1:9092".parse().unwrap()
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

    async fn start_broker(authorizer: Arc<dyn Authorizer>) -> (BrokerHandle, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
        cfg.audit_enabled = false;
        cfg.authorizer = authorizer;
        let handle = Broker::start(cfg).await.expect("start broker");
        (handle, dir)
    }

    async fn seed_reassignments(handle: &BrokerHandle) {
        handle
            .broker_arc_for_test()
            .controller
            .submit_change(vec![
                MetadataRecord::V1Topic(TopicRecord {
                    name: "orders-add".into(),
                    topic_id: Uuid::from_u128(1),
                    partitions: 1,
                    replication_factor: 2,
                }),
                MetadataRecord::V1Partition(PartitionRecord {
                    topic: "orders-add".into(),
                    partition: 0,
                    leader: 1,
                    replicas: vec![1, 2, 3],
                    isr: vec![1, 2],
                    leader_epoch: 4,
                    adding_replicas: vec![3],
                    removing_replicas: vec![],
                    directories: vec![Uuid::nil(), Uuid::nil(), Uuid::nil()],
                    partition_epoch: 8,
                }),
            ])
            .await
            .expect("seed reassignments");
    }

    #[tokio::test]
    async fn denied_response_preserves_error_fields() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        let bytes = handle(
            &broker,
            ListPartitionReassignmentsRequest::default(),
            &ctx,
            VERSION,
        )
        .await
        .expect("handle");
        let resp = decode_response(&bytes);

        let expected = ListPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("list-reassignment denied".to_string()),
            topics: vec![],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn filtered_success_response_preserves_topic_and_partition_fields() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_reassignments(&broker_handle).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        let bytes = handle(
            &broker,
            ListPartitionReassignmentsRequest {
                topics: Some(vec![ListPartitionReassignmentsTopics {
                    name: "orders-add".into(),
                    partition_indexes: vec![],
                    ..Default::default()
                }]),
                ..Default::default()
            },
            &ctx,
            VERSION,
        )
        .await
        .expect("handle");
        let resp = decode_response(&bytes);

        let expected = ListPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            topics: vec![OngoingTopicReassignment {
                name: "orders-add".to_string(),
                partitions: vec![OngoingPartitionReassignment {
                    partition_index: 0,
                    replicas: vec![1, 2, 3],
                    adding_replicas: vec![3],
                    removing_replicas: vec![],
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected, "{resp:?}");
        broker_handle.shutdown().await;
    }
}
