//! Connect-RPC handlers — thin adapters: proto in, `GatewayRecord` to the
//! core, `RecordOutcome` back to proto.

use std::sync::Arc;

use axum::Extension;
use connectrpc_axum::message::{ConnectError, ConnectRequest, ConnectResponse};

use crate::pb;
use crate::state::AppState;

/// Convert a wire [`pb::Record`] into the transport-agnostic [`GatewayRecord`].
pub(crate) fn to_gateway_record(r: crate::pb::Record) -> crate::types::GatewayRecord {
    crate::types::GatewayRecord {
        topic: r.topic,
        key: r.key.map(bytes::Bytes::from),
        value: bytes::Bytes::from(r.value),
        headers: r
            .headers
            .into_iter()
            .map(|(k, v)| (k, bytes::Bytes::from(v)))
            .collect(),
        partition: r.partition,
        timestamp_ms: r.timestamp_ms,
        idempotency_key: r.idempotency_key,
    }
}

pub async fn send(
    Extension(state): Extension<Arc<AppState>>,
    req: ConnectRequest<pb::SendRequest>,
) -> Result<ConnectResponse<pb::SendResponse>, ConnectError> {
    let msg = req.0;
    // NOTE (P0–P2): `msg.acks` is accepted on the wire but not yet honored —
    // every record is produced with acks=all, which the dedup/EOS path
    // requires anyway. Per-acks handling on the plain path is deferred.
    let mut results = Vec::with_capacity(msg.records.len());
    for r in msg.records {
        let rec = crate::handlers::to_gateway_record(r);
        let result = match state.produce.produce(rec).await {
            Ok(o) => pb::RecordResult {
                partition: o.partition,
                offset: o.offset,
                deduplicated: o.deduplicated,
                error: None,
            },
            Err(e) => pb::RecordResult {
                partition: -1,
                offset: -1,
                deduplicated: false,
                error: Some(pb::ErrorInfo {
                    code: 1,
                    message: e.to_string(),
                    retriable: false,
                }),
            },
        };
        results.push(result);
    }
    Ok(ConnectResponse::new(pb::SendResponse { results }))
}
