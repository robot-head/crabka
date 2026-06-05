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

use crate::consume::ConsumeSession;
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
                    Err(e) => crate::handlers::error_result(&e),
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

/// Join a consumer group on the caller's behalf and stream records. The first
/// frame MUST be `Start`; subsequent `Ack` frames drive offset commits
/// (at-least-once). The subscription ends when the control stream closes or
/// errors.
///
/// Commit semantics: on `Ack`, the session commits its *current* consumed
/// positions for all assigned partitions (the `Ack`'s `topic`/`partition`/
/// `offset` fields are currently advisory — per-offset commit is a follow-up,
/// pending an offset-specific commit API on the consumer). With `auto_commit`,
/// the session commits after each non-empty poll (at enqueue, slightly weaker
/// than on-receipt). For strict at-least-once, the caller should ack
/// synchronously per received batch.
pub fn subscribe_inner(
    mut frames: Streaming<pb::SubscribeFrame>,
    state: Arc<AppState>,
) -> impl Stream<Item = Result<pb::Inbound, ConnectError>> {
    async_stream::stream! {
        // First frame must be Start.
        let start = match frames.next().await {
            Some(Ok(pb::SubscribeFrame { frame: Some(pb::subscribe_frame::Frame::Start(s)) })) => s,
            Some(Ok(_)) => { yield Err(ConnectError::new_invalid_argument("first Subscribe frame must be Start")); return; }
            Some(Err(e)) => { yield Err(e); return; }
            None => return,
        };

        let client_id = format!("{}-sub", state.config.client_id);
        let mut session = match ConsumeSession::new(&state.config.bootstrap, &start.group_id, &client_id, start.topics).await {
            Ok(s) => s,
            Err(e) => { yield Err(ConnectError::new_internal(e.to_string())); return; }
        };
        let auto_commit = start.auto_commit;

        loop {
            // BORROW NOTE: do NOT call session.commit() inside a select! arm —
            // session.poll(..) holds a &mut borrow across the select. Instead set
            // flags inside the select and commit AFTER it resolves.
            let mut commit = false;
            let mut stop = false;
            let mut to_emit: Vec<pb::Inbound> = Vec::new();
            tokio::select! {
                frame = frames.next() => {
                    match frame {
                        Some(Ok(pb::SubscribeFrame { frame: Some(pb::subscribe_frame::Frame::Ack(_)) })) => commit = true,
                        Some(Ok(_)) => {}
                        Some(Err(e)) => { yield Err(e); stop = true; }
                        None => stop = true,
                    }
                }
                batch = session.poll(std::time::Duration::from_millis(500)) => {
                    match batch {
                        Ok(records) => {
                            for r in records {
                                to_emit.push(pb::Inbound {
                                    topic: r.topic,
                                    partition: r.partition,
                                    offset: r.offset,
                                    key: r.key.map(|b| b.to_vec()),
                                    value: r.value.map(|b| b.to_vec()).unwrap_or_default(),
                                    headers: std::collections::HashMap::new(),
                                    timestamp_ms: r.timestamp,
                                });
                            }
                            if !to_emit.is_empty() && auto_commit { commit = true; }
                        }
                        Err(e) => { yield Err(ConnectError::new_internal(e.to_string())); stop = true; }
                    }
                }
            }
            for msg in to_emit {
                yield Ok(msg);
            }
            if commit
                && let Err(e) = session.commit().await
            {
                yield Err(ConnectError::new_internal(e.to_string()));
                break;
            }
            if stop { break; }
        }
    }
}

/// Bidi `Subscribe` Connect handler.
pub async fn subscribe(
    Extension(state): Extension<Arc<AppState>>,
    req: ConnectRequest<Streaming<pb::SubscribeFrame>>,
) -> Result<
    ConnectResponse<
        StreamBody<Pin<Box<dyn Stream<Item = Result<pb::Inbound, ConnectError>> + Send>>>,
    >,
    ConnectError,
> {
    Ok(ConnectResponse::new(StreamBody::new(Box::pin(
        subscribe_inner(req.0, state),
    ))))
}
