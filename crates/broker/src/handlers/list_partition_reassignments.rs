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
use crate::codes::CLUSTER_AUTHORIZATION_FAILED;

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
            resource_name: "kafka-cluster",
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
        error_code: 0,
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
