//! `DeleteShareGroupState` (`api_key=86`). The handler tombstones the durable
//! share state for each `(group, topic, partition)` and drops the in-memory
//! entry. The handler gates on local leadership of the target
//! `__share_group_state` partition.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        delete_share_group_state_request::DeleteShareGroupStateRequest,
        delete_share_group_state_response::{
            DeleteShareGroupStateResponse, DeleteStateResult, PartitionResult,
        },
    },
};
use futures_util::future::BoxFuture;

use crate::{
    broker::Broker, codes, error::BrokerError, share_coordinator::coordinator::ShareCoordinator,
};

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let coordinator = Arc::clone(&broker.share_coordinator);
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = DeleteShareGroupStateRequest::decode(&mut cur, version)?;
        handle_request(coordinator, version, req).await
    })
}

async fn handle_request(
    coordinator: Arc<ShareCoordinator>,
    version: i16,
    req: DeleteShareGroupStateRequest,
) -> Result<Bytes, BrokerError> {
    let group_id = req.group_id;

    let mut results: Vec<DeleteStateResult> = Vec::with_capacity(req.topics.len());
    for topic in req.topics {
        let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
        let mut partitions: Vec<PartitionResult> = Vec::with_capacity(topic.partitions.len());
        for pd in topic.partitions {
            let state_partition =
                coordinator.state_partition_for(&group_id, &topic_id, pd.partition);
            let error_code = if coordinator.is_leader(state_partition).await {
                match coordinator.delete(&group_id, topic_id, pd.partition).await {
                    Ok(()) => codes::NONE,
                    Err(code) => code,
                }
            } else {
                codes::NOT_COORDINATOR
            };
            partitions.push(PartitionResult {
                partition: pd.partition,
                error_code,
                ..Default::default()
            });
        }
        results.push(DeleteStateResult {
            topic_id: topic.topic_id,
            partitions,
            ..Default::default()
        });
    }

    let resp = DeleteShareGroupStateResponse {
        results,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_protocol::{
        UnknownTaggedFields,
        owned::{
            delete_share_group_state_request::{DeleteStateData, PartitionData},
            delete_share_group_state_response::DeleteShareGroupStateResponse,
        },
        primitives::uuid::Uuid as ProtoUuid,
    };

    use super::*;

    fn decode(bytes: &Bytes) -> DeleteShareGroupStateResponse {
        let mut cur: &[u8] = bytes.as_ref();
        let resp =
            DeleteShareGroupStateResponse::decode(&mut cur, super::super::test_support::VERSION)
                .expect("decode response");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    #[tokio::test]
    async fn returns_topic_partition_and_error_for_unled_partition() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let coordinator = super::super::test_support::coordinator(dir.path());
        let topic_id = ProtoUuid([31; 16]);
        let req = DeleteShareGroupStateRequest {
            group_id: "share-group".into(),
            topics: vec![DeleteStateData {
                topic_id,
                partitions: vec![PartitionData {
                    partition: 7,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let bytes = handle_request(coordinator, super::super::test_support::VERSION, req)
            .await
            .expect("handle request");
        let resp = decode(&bytes);

        let expected = DeleteShareGroupStateResponse {
            results: vec![DeleteStateResult {
                topic_id,
                partitions: vec![PartitionResult {
                    partition: 7,
                    error_code: codes::NOT_COORDINATOR,
                    error_message: None,
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
    }
}
