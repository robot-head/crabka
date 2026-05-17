//! Thin typed wrappers over `crabka_client_core::Client` for the
//! three RPCs the ingester needs each tick. Returning typed responses
//! keeps the `Ingester` free of `crabka_protocol` imports.

use crabka_client_core::{Client, ClientError};
use crabka_protocol::owned::describe_cluster_request::DescribeClusterRequest;
use crabka_protocol::owned::describe_cluster_response::DescribeClusterResponse;
use crabka_protocol::owned::list_partition_reassignments_request::ListPartitionReassignmentsRequest;
use crabka_protocol::owned::list_partition_reassignments_response::ListPartitionReassignmentsResponse;
use crabka_protocol::owned::metadata_request::MetadataRequest;
use crabka_protocol::owned::metadata_response::MetadataResponse;

pub async fn fetch_metadata(client: &Client) -> Result<MetadataResponse, ClientError> {
    // v12: flexible, topic_id-aware. allow_auto_topic_creation = false.
    let req = MetadataRequest {
        topics: None,
        allow_auto_topic_creation: false,
        ..Default::default()
    };
    client.send(req).await
}

pub async fn fetch_describe_cluster(
    client: &Client,
) -> Result<DescribeClusterResponse, ClientError> {
    client.send(DescribeClusterRequest::default()).await
}

pub async fn fetch_list_reassignments(
    client: &Client,
) -> Result<ListPartitionReassignmentsResponse, ClientError> {
    // topics = None -> all in-flight reassignments.
    client
        .send(ListPartitionReassignmentsRequest::default())
        .await
}
