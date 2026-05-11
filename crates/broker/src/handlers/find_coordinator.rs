//! `FindCoordinator` (`api_key=10`). MVP has no group/transaction
//! coordinator runtime, so every lookup returns
//! `COORDINATOR_NOT_AVAILABLE` (15). Both the legacy single-coordinator
//! fields (v0-v3) and the `coordinators: Vec<Coordinator>` array (v4+)
//! are populated so clients on either side of the version split see the
//! same answer.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::find_coordinator_request::FindCoordinatorRequest;
use crabka_protocol::owned::find_coordinator_response::{Coordinator, FindCoordinatorResponse};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    _broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = FindCoordinatorRequest::decode(&mut cur, version)?;

        // For v4+, requests carry coordinator_keys. For v0-v3, the single
        // `key` field is what the client cares about — populate the legacy
        // top-level error fields and also emit a single Coordinator entry
        // for that key so the encode path is uniform.
        let keys: Vec<String> = if req.coordinator_keys.is_empty() {
            vec![req.key.clone()]
        } else {
            req.coordinator_keys.clone()
        };

        let coordinators: Vec<Coordinator> = keys
            .into_iter()
            .map(|k| Coordinator {
                key: k,
                node_id: -1,
                host: String::new(),
                port: -1,
                error_code: codes::COORDINATOR_NOT_AVAILABLE,
                error_message: None,
                ..Default::default()
            })
            .collect();

        let resp = FindCoordinatorResponse {
            throttle_time_ms: 0,
            error_code: codes::COORDINATOR_NOT_AVAILABLE,
            error_message: None,
            node_id: -1,
            host: String::new(),
            port: -1,
            coordinators,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
