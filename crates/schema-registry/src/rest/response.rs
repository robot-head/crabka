//! Success-response helper: serialises with `serde_json` and sets the Confluent
//! vendor content-type (axum's `Json` would force `application/json`).

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

/// Raw 200 with the vendor content-type (for the `/schema` raw-text endpoint,
/// which returns the schema string verbatim — still vendor content-type per Confluent).
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
        assert_eq!(resp.headers()["content-type"], crate::error::CONTENT_TYPE);
        let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        assert_eq!(&body[..], br#"{"id":7}"#);
    }
}
