//! Authentication middleware: resolve a `crabka_security::Principal` from each
//! request (mTLS → Bearer → Basic → Anonymous) into request extensions; `401`
//! on a bad credential or a missing one when `require_auth`. Reuses
//! `crabka_security` validators; only `BasicAuthStore` is local. Models
//! `grpc-gateway/src/authz/auth_layer.rs`.
pub mod basic;

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use crabka_security::{AuthMethod, OAuthBearerValidator, Principal};

use basic::BasicAuthStore;

/// An mTLS-authenticated principal inserted by the TLS accept loop (Task 4);
/// `auth_layer` consumes it as the highest-precedence source.
#[derive(Clone)]
pub struct MtlsPrincipal(pub Principal);

/// Per-request authentication state shared by [`auth_layer`]. Cheaply cloned
/// (the stores live behind `Arc`).
#[derive(Clone)]
pub struct AuthState {
    /// HTTP Basic credential store; `None` disables Basic.
    pub basic: Option<Arc<BasicAuthStore>>,
    /// Bearer (OAuth) token validator; `None` disables Bearer.
    pub bearer: Option<Arc<OAuthBearerValidator>>,
    /// Reject anonymous (credential-less) requests with `401`.
    pub require_auth: bool,
    /// Realm advertised in the `WWW-Authenticate: Basic realm="…"` header.
    pub realm: String,
}

/// The outcome of [`resolve`]: an authenticated principal, or a `401`.
#[derive(Debug, PartialEq, Eq)]
pub enum AuthDecision {
    /// Request carries (or is permitted) this identity.
    Authn(Principal),
    /// Reject with `401 Unauthorized`.
    Unauthorized,
}

/// The Anonymous principal used when no credentials are presented and
/// `require_auth` is off.
#[must_use]
pub fn anonymous() -> Principal {
    Principal {
        name: "ANONYMOUS".to_string(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    }
}

/// Resolve the request's principal. `now_ms` is passed for Bearer expiry.
///
/// Precedence: mTLS → Bearer → Basic → Anonymous. A presented-but-bad
/// credential (or a presented credential whose scheme is not configured) is
/// always [`AuthDecision::Unauthorized`], even when `require_auth` is off.
pub async fn resolve(
    headers: &HeaderMap,
    mtls: Option<Principal>,
    st: &AuthState,
    now_ms: i64,
) -> AuthDecision {
    if let Some(p) = mtls {
        return AuthDecision::Authn(p);
    }
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    if let Some(token) = auth.and_then(|s| s.strip_prefix("Bearer ")) {
        if let Some(validator) = &st.bearer {
            return match validator.validate(token, now_ms).await {
                Ok(outcome) => AuthDecision::Authn(outcome.principal),
                Err(_) => AuthDecision::Unauthorized,
            };
        }
        return AuthDecision::Unauthorized; // bearer presented but not configured
    }
    if let Some(b64) = auth.and_then(|s| s.strip_prefix("Basic ")) {
        let Some(store) = &st.basic else {
            return AuthDecision::Unauthorized;
        };
        let Ok(raw) = base64::engine::general_purpose::STANDARD.decode(b64) else {
            return AuthDecision::Unauthorized;
        };
        let Ok(text) = std::str::from_utf8(&raw) else {
            return AuthDecision::Unauthorized;
        };
        let Some((user, pass)) = text.split_once(':') else {
            return AuthDecision::Unauthorized;
        };
        if store.verify(user, pass) {
            return AuthDecision::Authn(Principal {
                name: user.to_string(),
                auth_method: AuthMethod::SaslPlain,
                groups: Vec::new(),
            });
        }
        return AuthDecision::Unauthorized;
    }
    // no credentials
    if st.require_auth {
        AuthDecision::Unauthorized
    } else {
        AuthDecision::Authn(anonymous())
    }
}

/// `from_fn_with_state` middleware: resolve the principal, insert it into
/// request extensions on success, or return `401` (with a
/// `WWW-Authenticate: Basic` challenge when Basic is configured).
pub async fn auth_layer(
    State(st): State<Arc<AuthState>>,
    mut req: Request,
    next: Next,
) -> Response {
    // SECURITY: a request carrying the inter-node forward header is TRUSTED — its
    // ingress node already authenticated AND authorized it. Mirror authz_layer's
    // FORWARD_HEADER skip so auth and authz agree on the same trust boundary
    // (operators MUST isolate the inter-node forwarding link; a client that forges
    // `X-Forwarded-For-Registry` bypasses both — see the slice-6 security spec).
    // This is required for ALL auth methods: an mTLS client's credential (its TLS
    // client cert) cannot be carried over the secondary→primary proxy hop, so the
    // primary must trust the forward rather than re-authenticate.
    if req
        .headers()
        .contains_key(crate::rest::forward::FORWARD_HEADER)
    {
        if req.extensions().get::<Principal>().is_none() {
            req.extensions_mut().insert(anonymous());
        }
        return next.run(req).await;
    }
    let mtls = req.extensions().get::<MtlsPrincipal>().map(|m| m.0.clone());
    let now = now_ms();
    match resolve(req.headers(), mtls, &st, now).await {
        AuthDecision::Authn(p) => {
            req.extensions_mut().insert(p);
            next.run(req).await
        }
        AuthDecision::Unauthorized => {
            let mut resp = (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
            if st.basic.is_some()
                && let Ok(v) =
                    header::HeaderValue::from_str(&format!("Basic realm=\"{}\"", st.realm))
            {
                resp.headers_mut().insert(header::WWW_AUTHENTICATE, v);
            }
            resp
        }
    }
}

/// Current time as Unix epoch milliseconds, used to pass `now_ms` to
/// [`OAuthBearerValidator::validate`]. Falls back to `0` on clock anomalies.
fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| {
        #[allow(clippy::cast_possible_truncation)]
        let ms = d.as_millis().min(i64::MAX as u128) as i64;
        ms
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header_map(authorization: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(v) = authorization {
            h.insert(
                header::AUTHORIZATION,
                header::HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    fn basic_b64(user: &str, pass: &str) -> String {
        let raw = format!("{user}:{pass}");
        let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
        format!("Basic {b64}")
    }

    fn state_with_basic(require_auth: bool) -> AuthState {
        let store = BasicAuthStore::from_users(
            [("alice".to_string(), "pw".to_string())]
                .into_iter()
                .collect(),
        );
        AuthState {
            basic: Some(Arc::new(store)),
            bearer: None,
            require_auth,
            realm: "schema-registry".to_string(),
        }
    }

    #[tokio::test]
    async fn anonymous_when_no_creds_and_not_required() {
        let st = state_with_basic(false);
        let decision = resolve(&header_map(None), None, &st, 0).await;
        match decision {
            AuthDecision::Authn(p) => {
                assert_eq!(p.name, "ANONYMOUS");
                assert_eq!(p.auth_method, AuthMethod::Anonymous);
            }
            AuthDecision::Unauthorized => panic!("expected anonymous Authn"),
        }
    }

    #[tokio::test]
    async fn unauthorized_when_required_and_no_creds() {
        let st = state_with_basic(true);
        let decision = resolve(&header_map(None), None, &st, 0).await;
        assert_eq!(decision, AuthDecision::Unauthorized);
    }

    #[tokio::test]
    async fn bad_basic_is_unauthorized() {
        // require_auth=false, yet a presented-but-wrong credential still 401s.
        let st = state_with_basic(false);
        let decision = resolve(
            &header_map(Some(&basic_b64("alice", "wrong"))),
            None,
            &st,
            0,
        )
        .await;
        assert_eq!(decision, AuthDecision::Unauthorized);
    }

    #[tokio::test]
    async fn good_basic_authenticates() {
        let st = state_with_basic(false);
        let decision = resolve(&header_map(Some(&basic_b64("alice", "pw"))), None, &st, 0).await;
        match decision {
            AuthDecision::Authn(p) => {
                assert_eq!(p.name, "alice");
                assert_eq!(p.auth_method, AuthMethod::SaslPlain);
            }
            AuthDecision::Unauthorized => panic!("expected alice Authn"),
        }
    }

    #[tokio::test]
    async fn mtls_principal_wins() {
        // mTLS principal present + any/no header → that principal, regardless of
        // require_auth or the Authorization header.
        let st = state_with_basic(true);
        let mtls = Principal {
            name: "cert-user".to_string(),
            auth_method: AuthMethod::MTls,
            groups: Vec::new(),
        };
        let decision = resolve(
            &header_map(Some(&basic_b64("alice", "wrong"))),
            Some(mtls.clone()),
            &st,
            0,
        )
        .await;
        assert_eq!(decision, AuthDecision::Authn(mtls));
    }

    /// Model A: `auth_layer` TRUSTS a request carrying `FORWARD_HEADER` and runs
    /// the handler even under `require_auth` with no credentials (the ingress node
    /// already authenticated it). A non-forwarded credential-less request still
    /// `401`s. This is the mechanism that lets ALL auth methods (incl. mTLS, whose
    /// credential can't cross the secondary→primary hop) work — see `proxy()`,
    /// which forwards no credential, only `FORWARD_HEADER`.
    #[tokio::test]
    async fn forwarded_request_bypasses_require_auth() {
        use axum::Router;
        use axum::body::Body;
        use axum::routing::get;
        use tower::ServiceExt as _; // for `oneshot`

        let st = AuthState {
            basic: None,
            bearer: None,
            require_auth: true,
            realm: "schema-registry".to_string(),
        };
        let app: Router = Router::new().route("/", get(|| async { "ok" })).layer(
            axum::middleware::from_fn_with_state(Arc::new(st), auth_layer),
        );

        // WITH the forward header, no Authorization → handler runs (200), NOT 401.
        let req = Request::builder()
            .uri("/")
            .header(crate::rest::forward::FORWARD_HEADER, "ingress-node")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "forwarded request must bypass require_auth"
        );

        // WITHOUT the forward header and no credentials → 401.
        let req = Request::builder().uri("/").body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "non-forwarded credential-less request must 401"
        );
    }
}
