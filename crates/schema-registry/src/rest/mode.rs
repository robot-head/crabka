//! `/mode` endpoints (global + per-subject).

use axum::extract::{Path, State};
use axum::response::Response;
use serde::Deserialize;

use crate::error::SrError;
use crate::rest::{AppState, response::ok_json};

#[derive(Deserialize)]
struct PutMode {
    mode: String,
}

/// GET /mode -> {"mode": "<M>"}
// axum requires async handlers even when the body is synchronous.
#[allow(clippy::unused_async)]
pub async fn get_global(State(st): State<AppState>) -> Response {
    let m = st.store.store.read().global_mode().to_string();
    ok_json(&serde_json::json!({ "mode": m }))
}

/// PUT /mode {"mode":"READONLY"} -> {"mode":"READONLY"}
pub async fn put_global(State(st): State<AppState>, body: String) -> Result<Response, SrError> {
    let req: PutMode =
        serde_json::from_str(&body).map_err(|e| SrError::InvalidMode(e.to_string()))?;
    st.store.set_global_mode(req.mode.clone()).await?;
    Ok(ok_json(&serde_json::json!({ "mode": req.mode })))
}

/// GET /mode/{subject} -> {"mode": "<M>"} | 404 if no override
pub async fn get_subject(
    State(st): State<AppState>,
    Path(subject): Path<String>,
) -> Result<Response, SrError> {
    let m = st
        .store
        .store
        .read()
        .subject_mode(&subject)
        .map(str::to_string)
        .ok_or_else(|| SrError::SubjectNotFound(subject.clone()))?;
    Ok(ok_json(&serde_json::json!({ "mode": m })))
}

/// PUT /mode/{subject} {"mode":"IMPORT"} -> {"mode":"IMPORT"}
pub async fn put_subject(
    State(st): State<AppState>,
    Path(subject): Path<String>,
    body: String,
) -> Result<Response, SrError> {
    let req: PutMode =
        serde_json::from_str(&body).map_err(|e| SrError::InvalidMode(e.to_string()))?;
    st.store
        .set_subject_mode(&subject, req.mode.clone())
        .await?;
    Ok(ok_json(&serde_json::json!({ "mode": req.mode })))
}

/// DELETE /mode/{subject} -> {"mode": "<prior>"} (clears the override)
pub async fn delete_subject(
    State(st): State<AppState>,
    Path(subject): Path<String>,
) -> Result<Response, SrError> {
    let prior = st
        .store
        .store
        .read()
        .subject_mode(&subject)
        .map(str::to_string)
        .ok_or_else(|| SrError::SubjectNotFound(subject.clone()))?;
    st.store.clear_subject_mode(&subject).await?;
    Ok(ok_json(&serde_json::json!({ "mode": prior })))
}
