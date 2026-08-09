//! `AllocateProducerIds` (`api_key=67`). Reserves one durable, cluster-wide
//! producer-ID block for a registered broker.

use bytes::Bytes;
use crabka_metadata::NodeId;
use crabka_protocol::{
    Decode,
    owned::{
        allocate_producer_ids_request::AllocateProducerIdsRequest,
        allocate_producer_ids_response::AllocateProducerIdsResponse,
    },
};

use crate::{
    broker::Broker,
    codes,
    error::BrokerError,
    producer_id_manager::{ProducerIdAllocationError, allocate_block},
};

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> futures_util::future::BoxFuture<'static, Result<Bytes, BrokerError>> {
    let controller = broker.controller.clone();
    let req_bytes = req_bytes.to_vec();
    Box::pin(async move {
        let mut input: &[u8] = &req_bytes;
        let request = AllocateProducerIdsRequest::decode(&mut input, version)?;
        let result = match u64::try_from(request.broker_id) {
            Ok(broker_id) => {
                allocate_block(&controller, NodeId(broker_id), request.broker_epoch).await
            }
            Err(_) => Err(ProducerIdAllocationError::BrokerNotRegistered(NodeId(
                u64::MAX,
            ))),
        };

        let response = match result {
            Ok(block) => AllocateProducerIdsResponse {
                error_code: codes::NONE,
                producer_id_start: block.first,
                producer_id_len: block.len,
                ..Default::default()
            },
            Err(error) => {
                let error_code = match error {
                    ProducerIdAllocationError::BrokerNotRegistered(_) => {
                        codes::BROKER_ID_NOT_REGISTERED
                    }
                    ProducerIdAllocationError::StaleBrokerEpoch { .. } => codes::STALE_BROKER_EPOCH,
                    ProducerIdAllocationError::Exhausted
                    | ProducerIdAllocationError::Controller(_) => codes::UNKNOWN_SERVER_ERROR,
                };
                tracing::warn!(%error, "AllocateProducerIds failed");
                AllocateProducerIdsResponse {
                    error_code,
                    producer_id_start: -1,
                    producer_id_len: 0,
                    ..Default::default()
                }
            }
        };
        crate::handlers::encode_response(&response, version)
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    crate::test_support::codec_helpers!(
        AllocateProducerIdsRequest,
        AllocateProducerIdsResponse,
        version = 0
    );

    #[tokio::test]
    async fn allocates_consecutive_durable_blocks_and_fences_stale_epochs() {
        let (broker_handle, _dir) = crate::test_support::start_broker_with(|config| {
            config.audit_enabled = false;
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let broker_id = i32::try_from(broker.config.node_id.0).unwrap();
        let broker_epoch = broker
            .controller
            .current_image()
            .broker_epoch(broker.config.node_id)
            .expect("registered broker epoch");
        let request = |epoch| AllocateProducerIdsRequest {
            broker_id,
            broker_epoch: epoch,
            ..Default::default()
        };

        let first = decode_response(
            &handle(&broker, 0, 1, &encode_request(&request(broker_epoch)))
                .await
                .unwrap(),
        );
        let second = decode_response(
            &handle(&broker, 0, 2, &encode_request(&request(broker_epoch)))
                .await
                .unwrap(),
        );
        assert!(first.error_code == codes::NONE);
        assert!(first.producer_id_start == 0);
        assert!(first.producer_id_len == 1_000);
        assert!(second.producer_id_start == 1_000);

        let stale = decode_response(
            &handle(&broker, 0, 3, &encode_request(&request(broker_epoch - 1)))
                .await
                .unwrap(),
        );
        assert!(stale.error_code == codes::STALE_BROKER_EPOCH);
        assert!(stale.producer_id_start == -1);
        broker_handle.shutdown().await;
    }
}
