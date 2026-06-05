//! Internal gateway→gateway forwarding: the owner-routing client plus the
//! `/internal/v1/forward` endpoint that receives a forwarded record and
//! produces it LOCALLY (the receiver is the partition's owner).
//!
//! Transport is plain JSON over HTTP on the gateway's own listener (TLS is a
//! later phase). This INTERNAL protocol is deliberately separate from the
//! public Connect `Send` API so the two evolve independently.

use std::sync::Arc;

use axum::routing::post;
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};

use crate::error::GatewayError;
use crate::state::AppState;
use crate::types::{GatewayRecord, RecordOutcome};

/// Wire form of a forwarded record (bytes as JSON arrays — no extra deps).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardRecord {
    pub topic: String,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub partition: Option<i32>,
    pub timestamp_ms: Option<i64>,
    pub idempotency_key: Option<String>,
}

impl ForwardRecord {
    fn from_record(r: &GatewayRecord) -> Self {
        Self {
            topic: r.topic.clone(),
            key: r.key.as_ref().map(|b| b.to_vec()),
            value: r.value.to_vec(),
            headers: r
                .headers
                .iter()
                .map(|(k, v)| (k.clone(), v.to_vec()))
                .collect(),
            partition: r.partition,
            timestamp_ms: r.timestamp_ms,
            idempotency_key: r.idempotency_key.clone(),
        }
    }

    fn into_record(self) -> GatewayRecord {
        GatewayRecord {
            topic: self.topic,
            key: self.key.map(bytes::Bytes::from),
            value: bytes::Bytes::from(self.value),
            headers: self
                .headers
                .into_iter()
                .map(|(k, v)| (k, bytes::Bytes::from(v)))
                .collect(),
            partition: self.partition,
            timestamp_ms: self.timestamp_ms,
            idempotency_key: self.idempotency_key,
        }
    }
}

/// Wire form of a forward result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardResult {
    pub partition: i32,
    pub offset: i64,
    pub deduplicated: bool,
    /// Present when the owner could not produce; `retriable` ⇒ the origin maps
    /// it back to `Unavailable` and retries / re-resolves.
    pub error: Option<ForwardError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardError {
    pub message: String,
    pub retriable: bool,
}

/// reqwest client that forwards a record to the owning replica.
pub struct Forwarder {
    http: reqwest::Client,
}

impl Forwarder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::new(),
        }
    }

    /// POST the record to `owner_addr`'s internal forward endpoint. Transport
    /// failures and owner-`retriable` errors become `Unavailable` so the origin
    /// retries / re-resolves to the (possibly new) owner.
    pub async fn forward(
        &self,
        owner_addr: &str,
        rec: &GatewayRecord,
    ) -> Result<RecordOutcome, GatewayError> {
        let url = format!("http://{owner_addr}/internal/v1/forward");
        let body = ForwardRecord::from_record(rec);
        let resp = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|_| GatewayError::Unavailable)?;
        if !resp.status().is_success() {
            return Err(GatewayError::Unavailable);
        }
        let result: ForwardResult = resp
            .json()
            .await
            .map_err(|e| GatewayError::Forward(format!("decode forward result: {e}")))?;
        match result.error {
            None => Ok(RecordOutcome {
                partition: result.partition,
                offset: result.offset,
                deduplicated: result.deduplicated,
            }),
            Some(e) if e.retriable => Err(GatewayError::Unavailable),
            Some(e) => Err(GatewayError::Forward(e.message)),
        }
    }
}

impl Default for Forwarder {
    fn default() -> Self {
        Self::new()
    }
}

/// The `/internal/v1/forward` route. Mount alongside the Connect + health
/// routers on the gateway listener.
#[must_use = "router must be merged into the application"]
pub fn forward_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/internal/v1/forward", post(forward_handler))
        .layer(Extension(state))
}

/// Receiver side: produce LOCALLY (no further forwarding — this replica owns
/// the partition; `produce_local` returns `Unavailable` if it just lost it,
/// which the origin retries). Never re-forwards, so there are no forward loops.
async fn forward_handler(
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<ForwardRecord>,
) -> Json<ForwardResult> {
    let rec = req.into_record();
    match state.produce.produce_local(rec).await {
        Ok(o) => Json(ForwardResult {
            partition: o.partition,
            offset: o.offset,
            deduplicated: o.deduplicated,
            error: None,
        }),
        Err(e) => {
            let retriable = matches!(e, GatewayError::Unavailable);
            Json(ForwardResult {
                partition: -1,
                offset: -1,
                deduplicated: false,
                error: Some(ForwardError {
                    message: e.to_string(),
                    retriable,
                }),
            })
        }
    }
}
