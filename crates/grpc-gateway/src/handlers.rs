//! Connect-RPC handlers — thin adapters: proto in, `GatewayRecord` to the
//! core, `RecordOutcome` back to proto.

use std::sync::Arc;

use axum::Extension;
use bytes::Bytes;
use connectrpc_axum::message::{ConnectError, ConnectRequest, ConnectResponse};

use crate::pb;
use crate::state::AppState;
use crate::types::GatewayRecord;

pub async fn send(
    Extension(state): Extension<Arc<AppState>>,
    req: ConnectRequest<pb::SendRequest>,
) -> Result<ConnectResponse<pb::SendResponse>, ConnectError> {
    let msg = req.0;
    let mut results = Vec::with_capacity(msg.records.len());
    for r in msg.records {
        let rec = GatewayRecord {
            topic: r.topic,
            key: r.key.map(Bytes::from),
            value: Bytes::from(r.value),
            headers: r
                .headers
                .into_iter()
                .map(|(k, v)| (k, Bytes::from(v)))
                .collect(),
            partition: r.partition,
            timestamp_ms: r.timestamp_ms,
            idempotency_key: r.idempotency_key,
        };
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
