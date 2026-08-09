//! `/mode` endpoints (global + per-subject).

use axum::{
    extract::{Path, State},
    response::Response,
};
use serde::Deserialize;

use crate::{
    error::SrError,
    rest::{AppState, response::ok_json},
};

#[derive(Deserialize)]
struct PutMode {
    mode: String,
}

/// `GET /mode -> {"mode": "<M>"}`
// axum requires async handlers even when the body is synchronous.
#[must_use]
pub fn get_global(State(st): State<AppState>) -> Response {
    let m = st.store.store.read().global_mode().to_string();
    ok_json(&serde_json::json!({ "mode": m }))
}

/// PUT /mode {"mode":"READONLY"} -> {"mode":"READONLY"}
#[tracing::instrument(level = "info", name = "sr.set_global_mode", skip_all, fields(mode = tracing::field::Empty), err)]
/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
pub async fn put_global(State(st): State<AppState>, body: String) -> Result<Response, SrError> {
    let req: PutMode =
        serde_json::from_str(&body).map_err(|e| SrError::InvalidMode(e.to_string()))?;
    tracing::Span::current().record("mode", req.mode.as_str());
    st.store.set_global_mode(req.mode.clone()).await?;
    Ok(ok_json(&serde_json::json!({ "mode": req.mode })))
}

/// `GET /mode/{subject} -> {"mode": "<M>"}`, or 404 when there is no override.
#[tracing::instrument(level = "debug", name = "sr.get_subject_mode", skip_all, fields(subject = %subject), err)]
/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
pub fn get_subject(
    State(st): State<AppState>,
    Path(subject): Path<String>,
) -> Result<Response, SrError> {
    let m = st
        .store
        .store
        .read()
        .subject_mode(&subject)
        .map(str::to_string)
        .ok_or_else(|| SrError::SubjectModeNotConfigured(subject.clone()))?;
    Ok(ok_json(&serde_json::json!({ "mode": m })))
}

/// PUT /mode/{subject} {"mode":"IMPORT"} -> {"mode":"IMPORT"}
#[tracing::instrument(level = "info", name = "sr.set_subject_mode", skip_all, fields(subject = %subject, mode = tracing::field::Empty), err)]
/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
pub async fn put_subject(
    State(st): State<AppState>,
    Path(subject): Path<String>,
    body: String,
) -> Result<Response, SrError> {
    let req: PutMode =
        serde_json::from_str(&body).map_err(|e| SrError::InvalidMode(e.to_string()))?;
    tracing::Span::current().record("mode", req.mode.as_str());
    st.store
        .set_subject_mode(&subject, req.mode.clone())
        .await?;
    Ok(ok_json(&serde_json::json!({ "mode": req.mode })))
}

/// `DELETE /mode/{subject} -> {"mode": "<prior>"}` clears the override.
#[tracing::instrument(level = "info", name = "sr.clear_subject_mode", skip_all, fields(subject = %subject), err)]
/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
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
        .ok_or_else(|| SrError::SubjectModeNotConfigured(subject.clone()))?;
    st.store.clear_subject_mode(&subject).await?;
    Ok(ok_json(&serde_json::json!({ "mode": prior })))
}
