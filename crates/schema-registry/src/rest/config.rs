//! `/config` endpoints. Stored and replayed; compatibility enforcement lives
//! with the format validators.

use axum::extract::{Path, State};
use axum::response::Response;
use serde::Deserialize;

use crate::error::SrError;
use crate::rest::{AppState, response::ok_json};

const LEVELS: &[&str] = &[
    "NONE",
    "BACKWARD",
    "BACKWARD_TRANSITIVE",
    "FORWARD",
    "FORWARD_TRANSITIVE",
    "FULL",
    "FULL_TRANSITIVE",
];

#[derive(Deserialize)]
struct PutConfig {
    compatibility: String,
}

fn validate(level: &str) -> Result<(), SrError> {
    if LEVELS.contains(&level) {
        Ok(())
    } else {
        Err(SrError::InvalidCompatibilityLevel(level.to_string()))
    }
}

/// GET /config
// axum requires async handlers even when the body is synchronous.
#[allow(clippy::unused_async)]
pub async fn get_global(State(st): State<AppState>) -> Response {
    let lvl = st.store.store.read().global_compat().to_string();
    ok_json(&serde_json::json!({ "compatibilityLevel": lvl }))
}

/// PUT /config
pub async fn put_global(State(st): State<AppState>, body: String) -> Result<Response, SrError> {
    let req: PutConfig = serde_json::from_str(&body)
        .map_err(|e| SrError::InvalidCompatibilityLevel(e.to_string()))?;
    validate(&req.compatibility)?;
    st.store
        .set_global_compat(req.compatibility.clone())
        .await?;
    Ok(ok_json(
        &serde_json::json!({ "compatibility": req.compatibility }),
    ))
}

/// GET /config/{subject}
// axum requires async handlers even when the body is synchronous.
#[allow(clippy::unused_async)]
pub async fn get_subject(
    State(st): State<AppState>,
    Path(subject): Path<String>,
) -> Result<Response, SrError> {
    let lvl = st
        .store
        .store
        .read()
        .subject_compat(&subject)
        .map(str::to_string)
        .ok_or_else(|| SrError::SubjectNotFound(subject.clone()))?;
    Ok(ok_json(&serde_json::json!({ "compatibilityLevel": lvl })))
}

/// PUT /config/{subject}
pub async fn put_subject(
    State(st): State<AppState>,
    Path(subject): Path<String>,
    body: String,
) -> Result<Response, SrError> {
    let req: PutConfig = serde_json::from_str(&body)
        .map_err(|e| SrError::InvalidCompatibilityLevel(e.to_string()))?;
    validate(&req.compatibility)?;
    st.store
        .set_subject_compat(&subject, req.compatibility.clone())
        .await?;
    Ok(ok_json(
        &serde_json::json!({ "compatibility": req.compatibility }),
    ))
}
