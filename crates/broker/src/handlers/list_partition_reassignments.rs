//! `ListPartitionReassignments` (`api_key` 46, KIP-455).

#![allow(clippy::cast_possible_truncation, clippy::unused_async)]

use bytes::Bytes;
use crabka_metadata::{PartitionRecord, ResourceType};
use crabka_protocol::{
    Encode,
    owned::{
        list_partition_reassignments_request::ListPartitionReassignmentsRequest,
        list_partition_reassignments_response::{
            ListPartitionReassignmentsResponse, OngoingPartitionReassignment,
            OngoingTopicReassignment,
        },
    },
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes::{CLUSTER_AUTHORIZATION_FAILED, NONE},
};

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
                replicas: pr.replicas.iter().map(|n| n.0 as i32).collect(),
                adding_replicas: pr.adding_replicas.iter().map(|n| n.0 as i32).collect(),
                removing_replicas: pr.removing_replicas.iter().map(|n| n.0 as i32).collect(),
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
    use std::sync::Arc;

    use assert2::assert;
    use crabka_metadata::{MetadataRecord, NodeId, TopicRecord};
    use crabka_protocol::owned::list_partition_reassignments_request::ListPartitionReassignmentsTopics;
    use uuid::Uuid;

    use super::*;
    use crate::{
        broker::BrokerHandle,
        test_support::{DenyAll, peer, principal},
    };

    const VERSION: i16 = crabka_protocol::owned::list_partition_reassignments_response::MAX_VERSION;

    crate::test_support::response_helpers!(
        ListPartitionReassignmentsResponse,
        version = VERSION,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

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
                    leader: NodeId(1),
                    replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
                    isr: vec![NodeId(1), NodeId(2)],
                    leader_epoch: crabka_metadata::LeaderEpoch(4),
                    adding_replicas: vec![NodeId(3)],
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
