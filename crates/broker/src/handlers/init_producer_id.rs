//! `InitProducerId` (`api_key=22`). Hands out `(producer_id, producer_epoch)`
//! to a producer. Transactional ids are rejected — slice 9 will add
//! transaction-id-to-pid binding.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::init_producer_id_request::InitProducerIdRequest;
use crabka_protocol::owned::init_producer_id_response::InitProducerIdResponse;
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let producer_ids = broker.producer_ids.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = InitProducerIdRequest::decode(&mut cur, version)?;

        // Reject transactional ids — slice 9 will add the txn id manager.
        let is_transactional = req.transactional_id.as_ref().is_some_and(|t| !t.is_empty());
        let resp = if is_transactional {
            InitProducerIdResponse {
                error_code: codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                throttle_time_ms: 0,
                producer_id: -1,
                producer_epoch: -1,
                ..Default::default()
            }
        } else {
            let (pid, epoch) = producer_ids.allocate();
            InitProducerIdResponse {
                error_code: codes::NONE,
                throttle_time_ms: 0,
                producer_id: pid,
                producer_epoch: epoch,
                ..Default::default()
            }
        };

        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
