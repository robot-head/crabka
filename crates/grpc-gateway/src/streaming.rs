//! Streaming Connect handlers — bidirectional `SendStream` (produce) and
//! `Subscribe` (consume). The per-handler logic lives in a `*_inner` function
//! returning a plain `Stream` (unit-testable); the public handler is a thin
//! wrapper into `ConnectResponse::new(StreamBody::new(inner))`.

use std::pin::Pin;
use std::sync::Arc;

use axum::Extension;
use connectrpc_axum::message::{
    ConnectError, ConnectRequest, ConnectResponse, StreamBody, Streaming,
};
use futures_util::{Stream, StreamExt};

use crate::handlers::to_gateway_record;
use crate::pb;
use crate::state::AppState;

/// Produce every record in each inbound `SendRequest`, emitting one `SendAck`
/// (with a per-record `RecordResult` vector) per request.
pub fn send_stream_inner(
    mut inbound: Streaming<pb::SendRequest>,
    state: Arc<AppState>,
) -> impl Stream<Item = Result<pb::SendAck, ConnectError>> {
    async_stream::stream! {
        while let Some(item) = inbound.next().await {
            let send_req = match item {
                Ok(r) => r,
                Err(e) => { yield Err(e); break; }
            };
            let mut results = Vec::with_capacity(send_req.records.len());
            for r in send_req.records {
                let rec = to_gateway_record(r);
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
                        error: Some(pb::ErrorInfo { code: 1, message: e.to_string(), retriable: false }),
                    },
                };
                results.push(result);
            }
            yield Ok(pb::SendAck { results });
        }
    }
}

/// Bidi `SendStream` Connect handler.
pub async fn send_stream(
    Extension(state): Extension<Arc<AppState>>,
    req: ConnectRequest<Streaming<pb::SendRequest>>,
) -> Result<
    ConnectResponse<
        StreamBody<Pin<Box<dyn Stream<Item = Result<pb::SendAck, ConnectError>> + Send>>>,
    >,
    ConnectError,
> {
    Ok(ConnectResponse::new(StreamBody::new(Box::pin(
        send_stream_inner(req.0, state),
    ))))
}
