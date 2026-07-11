//! Per-request bearer-token → Principal resolution.
//!
//! Provides an axum middleware ([`resolve_principal`]) that resolves the
//! caller's [`Principal`] from an `Authorization: Bearer <token>` header via
//! an [`OAuthBearerValidator`] extension, falling through gracefully when the
//! extension or header is absent.  A connection-level mTLS principal injected
//! by `serve_tls` is the _base_ identity; a valid bearer header **overrides**
//! it for per-request authz.

use std::{
    net::SocketAddr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{extract::Request, middleware::Next, response::Response};
use crabka_security::{AuthMethod, OAuthBearerValidator, Principal};

/// Extension wrapper around [`OAuthBearerValidator`] placed on the
/// [`axum::Router`] so the [`resolve_principal`] middleware can reach it.
///
/// Wrap the validator in `Arc` to make it cheap to clone out of extensions.
///
/// ```rust,ignore
/// router.layer(axum::Extension(BearerValidator(Arc::new(validator))))
/// ```
#[derive(Debug, Clone)]
pub struct BearerValidator(pub Arc<OAuthBearerValidator>);

/// Return the Anonymous principal used when no authenticated identity is
/// available.
#[must_use]
pub fn anonymous() -> Principal {
    Principal {
        name: "ANONYMOUS".to_string(),
        auth_method: AuthMethod::Anonymous,
        groups: vec![],
    }
}

/// Axum middleware that attempts bearer-token validation and, on success,
/// inserts the resolved [`Principal`] into request extensions (overriding any
/// mTLS principal from `serve_tls`). Falls through on any of:
///
/// - No [`BearerValidator`] extension (validator not configured).
/// - No `Authorization: Bearer <token>` header present.
/// - Token validation failure (logs at `debug` level; does NOT short-circuit
///   the request — authz gating is a separate concern done in handlers).
///
/// When none of the above apply AND validation succeeds, the new principal
/// is inserted; the mTLS principal (if any) is replaced.
pub async fn resolve_principal(mut req: Request, next: Next) -> Response {
    if let Some(BearerValidator(validator)) = req.extensions().get::<BearerValidator>().cloned()
        && let Some(token) = bearer_token(&req)
    {
        let now = now_ms();
        match validator.validate(&token, now).await {
            Ok(outcome) => {
                req.extensions_mut().insert(outcome.principal);
            }
            Err(e) => {
                tracing::debug!(
                    error = %e,
                    "bearer token invalid; falling through to connection principal"
                );
            }
        }
    }
    next.run(req).await
}

/// Extract the bearer token string from the `Authorization` header, if
/// present and well-formed (`Authorization: Bearer <token>`).
pub fn bearer_token(req: &Request) -> Option<String> {
    let value = req.headers().get(axum::http::header::AUTHORIZATION)?;
    let s = value.to_str().ok()?;
    let token = s.strip_prefix("Bearer ")?;
    if token.is_empty() {
        None
    } else {
        Some(token.to_string())
    }
}

/// Current time as Unix epoch milliseconds, used to pass `now_ms` to
/// [`OAuthBearerValidator::validate`].
///
/// Falls back to `0` on `SystemTime` clock anomalies (pre-epoch, overflow);
/// in practice this means no token will ever validate on those machines —
/// an acceptable safe-fail.
#[must_use]
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// Return the peer [`SocketAddr`] from request extensions, or the default
/// `0.0.0.0:0` placeholder when absent (e.g. in plaintext test paths where
/// no peer was injected).
#[must_use]
pub fn peer_or_default(req: &Request) -> SocketAddr {
    req.extensions()
        .get::<SocketAddr>()
        .copied()
        .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 0)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        Router,
        body::Body,
        http::{Request, Response, StatusCode},
        middleware::from_fn,
        routing::get,
    };
    use crabka_security::{OAuthBearerValidator, Principal, UnsecuredJwsValidator};
    use tower::ServiceExt;

    use super::*;

    // Pre-computed unsecured JWS tokens (alg=none, base64url, no padding):
    //   header  = {"alg":"none"}
    //   payload = {"sub": "<user>", "exp": <exp_secs>}
    //
    // Generated via:
    //   python3 -c "import base64, json; ..."
    //
    // alice, exp=9999999999 (year ~2286, effectively never expires)
    const TOKEN_ALICE: &str =
        "eyJhbGciOiJub25lIn0.eyJzdWIiOiAiYWxpY2UiLCAiZXhwIjogOTk5OTk5OTk5OX0.";
    // bob, exp=1 (epoch+1s, long expired)
    const TOKEN_BOB_EXPIRED: &str = "eyJhbGciOiJub25lIn0.eyJzdWIiOiAiYm9iIiwgImV4cCI6IDF9.";
    // bearer-user, exp=9999999999
    const TOKEN_BEARER_USER: &str =
        "eyJhbGciOiJub25lIn0.eyJzdWIiOiAiYmVhcmVyLXVzZXIiLCAiZXhwIjogOTk5OTk5OTk5OX0.";

    async fn principal_echo(req: Request<Body>) -> Response<Body> {
        let name = req
            .extensions()
            .get::<Principal>()
            .map_or_else(|| "none".to_string(), |p| p.name.clone());
        Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(name))
            .unwrap()
    }

    fn test_router() -> Router {
        let validator = OAuthBearerValidator::Unsecured(UnsecuredJwsValidator::default());
        let bearer = BearerValidator(Arc::new(validator));
        Router::new()
            .route("/", get(principal_echo))
            .layer(from_fn(resolve_principal))
            .layer(axum::Extension(bearer))
    }

    #[tokio::test]
    async fn bearer_resolution_cases() {
        for (_name, token, expected) in [
            ("absent", None, b"none".as_slice()),
            ("valid", Some(TOKEN_ALICE), b"alice".as_slice()),
            ("expired", Some(TOKEN_BOB_EXPIRED), b"none".as_slice()),
        ] {
            let mut builder = Request::builder().uri("/");
            if let Some(token) = token {
                builder = builder.header("Authorization", format!("Bearer {token}"));
            }
            let resp = test_router()
                .oneshot(builder.body(Body::empty()).unwrap())
                .await
                .unwrap();
            let status = resp.status();
            let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
            assert2::assert!(status == StatusCode::OK);
            assert2::assert!(body.as_ref() == expected);
        }
    }

    /// Valid bearer overrides a pre-existing mTLS principal in extensions.
    #[tokio::test]
    async fn bearer_overrides_mtls_principal() {
        async fn inject_mtls_principal(mut req: Request<Body>, next: Next) -> Response<Body> {
            req.extensions_mut().insert(Principal {
                name: "mtls-user".to_string(),
                auth_method: crabka_security::AuthMethod::MTls,
                groups: vec![],
            });
            next.run(req).await
        }

        async fn principal_echo(req: Request<Body>) -> Response<Body> {
            let name = req
                .extensions()
                .get::<Principal>()
                .map_or_else(|| "none".to_string(), |p| p.name.clone());
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::from(name))
                .unwrap()
        }

        let validator = OAuthBearerValidator::Unsecured(UnsecuredJwsValidator::default());
        let bearer = BearerValidator(Arc::new(validator));

        let app = Router::new()
            .route("/", get(principal_echo))
            .layer(from_fn(resolve_principal))
            .layer(from_fn(inject_mtls_principal))
            .layer(axum::Extension(bearer));

        let req = Request::builder()
            .uri("/")
            .header("Authorization", format!("Bearer {TOKEN_BEARER_USER}"))
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        // bearer-user should override mtls-user
        assert2::assert!(&body[..] == b"bearer-user");
    }
}
