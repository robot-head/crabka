//! Write-forwarding middleware: a secondary proxies mutating REST to the
//! elected primary; reads + primary-side writes pass through. A forwarded
//! request carries `X-Forwarded-For-Registry` so the primary never re-forwards.

use axum::{
    body::Body,
    extract::{Request, State},
    http::{HeaderValue, Method, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use crabka_units::prelude::*;
use tokio::sync::watch;

use crate::election::PrimaryState;

pub const FORWARD_HEADER: &str = "x-forwarded-for-registry";

#[derive(Clone)]
pub struct ForwardState {
    pub primary: watch::Receiver<PrimaryState>,
    pub http: reqwest::Client,
    pub node_id: String,
    /// Largest forwarded request body this node buffers before replaying it.
    pub forward_max_body: ByteSize,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Decision {
    PassThrough,
    Forward(String),
    Unavailable,
    Retriable,
}

/// Decide what to do with a request, given the method, whether it already
/// carries the forward header, and the current primary state.
pub(crate) fn decide(method: &Method, already_forwarded: bool, state: &PrimaryState) -> Decision {
    let mutating = matches!(
        method,
        &Method::POST | &Method::PUT | &Method::DELETE | &Method::PATCH
    );
    if !mutating {
        return Decision::PassThrough;
    }
    if state.is_primary {
        return Decision::PassThrough;
    }
    if already_forwarded {
        // forwarded to a non-primary (stale primary / race) → ask caller to retry
        return Decision::Retriable;
    }
    match &state.primary_url {
        Some(url) => Decision::Forward(url.clone()),
        None => Decision::Unavailable,
    }
}

/// axum `from_fn_with_state` middleware.
pub async fn forward_layer(State(fwd): State<ForwardState>, req: Request, next: Next) -> Response {
    let already = req.headers().contains_key(FORWARD_HEADER);
    let method = req.method().clone();
    let state = fwd.primary.borrow().clone();
    match decide(&method, already, &state) {
        Decision::PassThrough => next.run(req).await,
        Decision::Unavailable => {
            (StatusCode::SERVICE_UNAVAILABLE, "no primary elected").into_response()
        }
        Decision::Retriable => {
            (StatusCode::SERVICE_UNAVAILABLE, "not primary; retry").into_response()
        }
        Decision::Forward(primary_url) => proxy(&fwd, &primary_url, req).await,
    }
}

async fn proxy(fwd: &ForwardState, primary_url: &str, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let path_q = parts.uri.path_and_query().map_or("", |p| p.as_str());
    let url = format!("{primary_url}{path_q}");
    // `to_bytes` takes a raw `usize` cap.
    let Ok(bytes) = axum::body::to_bytes(body, fwd.forward_max_body.bytes_usize()).await else {
        return (StatusCode::BAD_REQUEST, "body read failed").into_response();
    };
    let method = reqwest::Method::from_bytes(parts.method.as_str().as_bytes())
        .unwrap_or(reqwest::Method::POST);
    let mut rb = fwd.http.request(method, &url).body(bytes.to_vec());
    if let Some(ct) = parts.headers.get(header::CONTENT_TYPE) {
        rb = rb.header(header::CONTENT_TYPE, ct);
    }
    // SECURITY: do NOT forward the caller's `Authorization` header. The ingress
    // node already authenticated AND authorized this request; the primary trusts
    // the forward via `FORWARD_HEADER` (both `auth_layer` and `authz_layer` skip
    // for it). Forwarding the credential would leak it over the inter-node hop and
    // could not work for mTLS anyway (a client cert can't be carried on this
    // server-to-server `reqwest` call).
    rb = rb.header(FORWARD_HEADER, &fwd.node_id);
    match rb.send().await {
        Ok(resp) => {
            let status =
                StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let ct = resp
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| HeaderValue::from_bytes(v.as_bytes()).ok());
            let body = resp.bytes().await.unwrap_or_default();
            let mut out = Response::new(Body::from(body));
            *out.status_mut() = status;
            if let Some(ct) = ct {
                out.headers_mut().insert(header::CONTENT_TYPE, ct);
            }
            out
        }
        Err(e) => (StatusCode::BAD_GATEWAY, format!("forward failed: {e}")).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::election::PrimaryState;

    fn primary(url: &str) -> PrimaryState {
        PrimaryState {
            is_primary: true,
            primary_url: Some(url.into()),
        }
    }
    fn secondary(url: &str) -> PrimaryState {
        PrimaryState {
            is_primary: false,
            primary_url: Some(url.into()),
        }
    }

    #[test]
    fn forwarding_decision_cases() {
        let no_primary = PrimaryState {
            is_primary: false,
            primary_url: None,
        };
        for (_name, method, forwarded, state, expected) in [
            (
                "read_secondary",
                Method::GET,
                false,
                secondary("http://p:8081"),
                Decision::PassThrough,
            ),
            (
                "write_primary",
                Method::POST,
                false,
                primary("http://me:8081"),
                Decision::PassThrough,
            ),
            (
                "write_secondary",
                Method::POST,
                false,
                secondary("http://p:8081"),
                Decision::Forward("http://p:8081".into()),
            ),
            (
                "secondary_without_primary",
                Method::DELETE,
                false,
                no_primary,
                Decision::Unavailable,
            ),
            (
                "forwarded_secondary",
                Method::POST,
                true,
                secondary("http://p:8081"),
                Decision::Retriable,
            ),
            (
                "forwarded_primary",
                Method::POST,
                true,
                primary("http://me:8081"),
                Decision::PassThrough,
            ),
        ] {
            assert2::assert!(decide(&method, forwarded, &state) == expected);
        }
    }

    #[tokio::test]
    async fn forwarding_body_limit_uses_configured_value() {
        let (_primary_tx, primary) = watch::channel(secondary("http://unused"));
        let fwd = ForwardState {
            primary,
            http: reqwest::Client::new(),
            node_id: "secondary".into(),
            forward_max_body: bytes(3),
        };
        let request = Request::builder()
            .method(Method::POST)
            .uri("/")
            .body(Body::from("four"))
            .unwrap();

        let response = proxy(&fwd, "http://unused", request).await;

        assert2::assert!(response.status() == StatusCode::BAD_REQUEST);
    }
}
