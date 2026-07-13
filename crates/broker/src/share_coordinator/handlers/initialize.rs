//! `InitializeShareGroupState` (`api_key=83`). Seeds the durable share state
//! for each `(group, topic, partition)` at the requested `state_epoch` /
//! `start_offset`. Gates every partition on local leadership of its
//! `__share_group_state` partition.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use crabka_log::Offset;
use crabka_protocol::{
    Decode, Encode,
    owned::{
        initialize_share_group_state_request::InitializeShareGroupStateRequest,
        initialize_share_group_state_response::{
            InitializeShareGroupStateResponse, InitializeStateResult, PartitionResult,
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
        let req = InitializeShareGroupStateRequest::decode(&mut cur, version)?;
        handle_request(coordinator, version, req).await
    })
}

// cargo-mutants: equivalent — `error_message: None` is exactly the derived
// `Default` for the `Option<String>` field, so deleting the field is a no-op.
#[cfg_attr(test, mutants::skip)]
async fn handle_request(
    coordinator: Arc<ShareCoordinator>,
    version: i16,
    req: InitializeShareGroupStateRequest,
) -> Result<Bytes, BrokerError> {
    let group_id = req.group_id;

    let mut results: Vec<InitializeStateResult> = Vec::with_capacity(req.topics.len());
    for topic in req.topics {
        let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
        let mut partitions: Vec<PartitionResult> = Vec::with_capacity(topic.partitions.len());
        for pd in topic.partitions {
            let state_partition =
                coordinator.state_partition_for(&group_id, &topic_id, pd.partition);
            let error_code = if coordinator.is_leader(state_partition).await {
                match coordinator
                    .initialize(
                        &group_id,
                        topic_id,
                        pd.partition,
                        pd.state_epoch,
                        Offset(pd.start_offset),
                    )
                    .await
                {
                    Ok(()) => codes::NONE,
                    Err(code) => code,
                }
            } else {
                codes::NOT_COORDINATOR
            };
            partitions.push(PartitionResult {
                partition: pd.partition,
                error_code,
                error_message: None,
                ..Default::default()
            });
        }
        results.push(InitializeStateResult {
            topic_id: topic.topic_id,
            partitions,
            ..Default::default()
        });
    }

    let resp = InitializeShareGroupStateResponse {
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
            initialize_share_group_state_request::{InitializeStateData, PartitionData},
            initialize_share_group_state_response::InitializeShareGroupStateResponse,
        },
        primitives::uuid::Uuid as ProtoUuid,
    };

    use super::*;

    fn decode(bytes: &Bytes) -> InitializeShareGroupStateResponse {
        let mut cur: &[u8] = bytes.as_ref();
        let resp = InitializeShareGroupStateResponse::decode(
            &mut cur,
            super::super::test_support::VERSION,
        )
        .expect("decode response");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    #[tokio::test]
    async fn returns_topic_partition_and_error_for_unled_partition() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let coordinator = super::super::test_support::coordinator(dir.path());
        let topic_id = ProtoUuid([32; 16]);
        let req = InitializeShareGroupStateRequest {
            group_id: "share-group".into(),
            topics: vec![InitializeStateData {
                topic_id,
                partitions: vec![PartitionData {
                    partition: 5,
                    state_epoch: 11,
                    start_offset: 23,
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

        let expected = InitializeShareGroupStateResponse {
            results: vec![InitializeStateResult {
                topic_id,
                partitions: vec![PartitionResult {
                    partition: 5,
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
