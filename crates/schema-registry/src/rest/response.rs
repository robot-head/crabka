//! Success-response helper. It serialises with `serde_json` and sets the
//! Confluent vendor content-type. axum's `Json` would force
//! `application/json`.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::error::CONTENT_TYPE;

/// 200 OK with a JSON body and the vendor content-type.
pub fn ok_json<T: Serialize>(value: &T) -> Response {
    match serde_json::to_string(value) {
        Ok(body) => (StatusCode::OK, [("content-type", CONTENT_TYPE)], body).into_response(),
        Err(e) => crate::error::SrError::Backend(e.to_string()).into_response(),
    }
}

/// Raw 200 with the vendor content-type. This serves the `/schema` raw-text
/// endpoint, which returns the schema string verbatim. Confluent still uses the
/// vendor content-type there.
#[must_use]
pub fn ok_raw(body: String) -> Response {
    (StatusCode::OK, [("content-type", CONTENT_TYPE)], body).into_response()
}

#[cfg(test)]
mod tests {
    use axum::response::IntoResponse;

    use super::*;

    #[tokio::test]
    async fn ok_json_sets_vendor_content_type() {
        let resp = ok_json(&serde_json::json!({"id": 7})).into_response();
        let content_type = resp.headers()["content-type"].clone();
        let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        assert2::assert!(content_type.to_str().unwrap() == crate::error::CONTENT_TYPE);
        assert2::assert!(body.as_ref() == br#"{"id":7}"#.as_slice());
    }
}
