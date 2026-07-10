//! Thin typed wrappers over `crabka_client_core::Client` for the
//! three RPCs the ingester needs each tick. Returning typed responses
//! keeps the `Ingester` free of `crabka_protocol` imports.

use crabka_client_core::{Client, ClientError};
use crabka_protocol::owned::{
    describe_cluster_request::DescribeClusterRequest,
    describe_cluster_response::DescribeClusterResponse,
    list_partition_reassignments_request::ListPartitionReassignmentsRequest,
    list_partition_reassignments_response::ListPartitionReassignmentsResponse,
    metadata_request::MetadataRequest, metadata_response::MetadataResponse,
};

pub async fn fetch_metadata(client: &Client) -> Result<MetadataResponse, ClientError> {
    client.send(metadata_request()).await
}

fn metadata_request() -> MetadataRequest {
    MetadataRequest {
        topics: None,
        allow_auto_topic_creation: false,
        ..Default::default()
    }
}

pub async fn fetch_describe_cluster(
    client: &Client,
) -> Result<DescribeClusterResponse, ClientError> {
    client.send(describe_cluster_request()).await
}

// cargo-mutants: this request's desired shape is exactly the generated default.
#[cfg_attr(test, mutants::skip)]
fn describe_cluster_request() -> DescribeClusterRequest {
    DescribeClusterRequest::default()
}

pub async fn fetch_list_reassignments(
    client: &Client,
) -> Result<ListPartitionReassignmentsResponse, ClientError> {
    client.send(list_reassignments_request()).await
}

// cargo-mutants: this request's desired shape is exactly the generated default.
#[cfg_attr(test, mutants::skip)]
fn list_reassignments_request() -> ListPartitionReassignmentsRequest {
    ListPartitionReassignmentsRequest::default()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::check;

    use super::*;

    #[test]
    fn metadata_request_fetches_all_topics_without_auto_creation() {
        let req = metadata_request();
        assert_eq!((req.topics, req.allow_auto_topic_creation), (None, false));
    }

    #[test]
    fn admin_request_builders_use_default_request_shapes() {
        assert_eq!(
            describe_cluster_request(),
            DescribeClusterRequest::default()
        );
        assert_eq!(
            list_reassignments_request(),
            ListPartitionReassignmentsRequest::default()
        );
    }

    #[tokio::test]
    async fn fetch_wrappers_propagate_send_errors() {
        let client = Client::builder()
            .bootstrap("127.0.0.1:1")
            .client_id("rebalancer-admin-client-test")
            .connect_timeout(Duration::from_millis(50))
            .request_timeout(Duration::from_millis(50))
            .build()
            .await
            .expect("client build does not connect");

        check!(fetch_metadata(&client).await.is_err());
        check!(fetch_describe_cluster(&client).await.is_err());
        check!(fetch_list_reassignments(&client).await.is_err());
    }
}
