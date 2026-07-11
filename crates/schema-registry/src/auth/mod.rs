//! Authentication middleware: resolve a `crabka_security::Principal` from each
//! request (mTLS → Bearer → Basic → Anonymous) into request extensions; `401`
//! on a bad credential or a missing one when `require_auth`. Reuses
//! `crabka_security` validators; only `BasicAuthStore` is local. Models
//! `grpc-gateway/src/authz/auth_layer.rs`.
pub mod basic;

use std::sync::Arc;

use axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::Engine as _;
use basic::BasicAuthStore;
use crabka_security::{AuthMethod, OAuthBearerValidator, Principal};

/// An mTLS-authenticated principal inserted by the TLS accept loop;
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
    /// Realm advertised in the `WWW-Authenticate: basic realm="…"` header.
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
/// `WWW-Authenticate: basic realm="…"` challenge when Basic is configured).
pub async fn auth_layer(
    State(st): State<Arc<AuthState>>,
    mut req: Request,
    next: Next,
) -> Response {
    // SECURITY: a request carrying the inter-node forward header is TRUSTED — its
    // ingress node already authenticated AND authorized it. Mirror authz_layer's
    // FORWARD_HEADER skip so auth and authz agree on the same trust boundary
    // (operators MUST isolate the inter-node forwarding link; a client that forges
    // `X-Forwarded-For-Registry` bypasses both).
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
        AuthDecision::Unauthorized => unauthorized(&st),
    }
}

/// Build the cp-byte-exact `401` response.
///
/// Calibrated against `mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0`
/// (`tests/fixtures/auth/basic.json`):
///
/// - status `401`;
/// - body `{"error_code":401,"message":"Unauthorized"}` with the vendor
///   `application/vnd.schemaregistry.v1+json` content-type (cp's standard error
///   envelope — same shape as every other registry error);
/// - when Basic is configured, `WWW-Authenticate: basic realm="<realm>"`. NOTE
///   the scheme token is lowercase `basic`, exactly as cp's Jetty
///   `BasicAuthenticator` emits it; the realm is the operator's
///   `authentication.realm` (the JAAS entry name — `SchemaRegistry-Props` in the
///   capture).
fn unauthorized(st: &AuthState) -> Response {
    let body = serde_json::json!({ "error_code": 401, "message": "Unauthorized" }).to_string();
    let mut resp = (
        StatusCode::UNAUTHORIZED,
        [("content-type", crate::error::CONTENT_TYPE)],
        body,
    )
        .into_response();
    if st.basic.is_some()
        && let Ok(v) = header::HeaderValue::from_str(&format!("basic realm=\"{}\"", st.realm))
    {
        resp.headers_mut().insert(header::WWW_AUTHENTICATE, v);
    }
    resp
}

/// Current time as Unix epoch milliseconds, used to pass `now_ms` to
/// [`OAuthBearerValidator::validate`]. Falls back to `0` on clock anomalies.
fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
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
    async fn no_credentials_cases_respect_auth_requirement() {
        for (_name, require_auth, expected) in [
            (
                "optional_auth_is_anonymous",
                false,
                AuthDecision::Authn(anonymous()),
            ),
            (
                "required_auth_is_unauthorized",
                true,
                AuthDecision::Unauthorized,
            ),
        ] {
            let state = state_with_basic(require_auth);
            let decision = resolve(&header_map(None), None, &state, 0).await;
            assert2::assert!(decision == expected);
        }
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
        assert2::assert!(decision == AuthDecision::Unauthorized);
    }

    #[tokio::test]
    async fn good_basic_authenticates() {
        let st = state_with_basic(false);
        let decision = resolve(&header_map(Some(&basic_b64("alice", "pw"))), None, &st, 0).await;
        assert2::assert!(
            decision
                == AuthDecision::Authn(Principal {
                    name: "alice".to_string(),
                    auth_method: AuthMethod::SaslPlain,
                    groups: Vec::new(),
                })
        );
    }

    #[tokio::test]
    async fn bearer_with_validator_authenticates() {
        // bearer=Some(unsecured validator): a JWT whose `sub` claim is the
        // principal name resolves to that principal (exercises the configured-
        // validator success branch of `resolve`).
        use base64::Engine as _;
        let validator = OAuthBearerValidator::Unsecured(crabka_security::UnsecuredJwsValidator {
            principal_claim_name: "sub".to_string(),
            ..Default::default()
        });
        let st = AuthState {
            basic: None,
            bearer: Some(Arc::new(validator)),
            require_auth: true,
            realm: "schema-registry".to_string(),
        };
        // Minimal unsigned JWT: header.payload.signature (empty sig for
        // `alg:none`). The validator requires an `exp` claim in the future, so
        // set one far ahead (year 2100, in seconds) and evaluate at now_ms=0.
        let b64 = |b: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b);
        let token = format!(
            "{}.{}.",
            b64(br#"{"alg":"none"}"#),
            b64(br#"{"sub":"svc-account","exp":4102444800}"#),
        );
        match resolve(&header_map(Some(&format!("Bearer {token}"))), None, &st, 0).await {
            AuthDecision::Authn(p) => assert2::assert!(p.name == "svc-account"),
            AuthDecision::Unauthorized => panic!("expected svc-account Authn"),
        }
    }

    #[tokio::test]
    async fn presented_but_unconfigured_credential_cases_are_unauthorized() {
        let state = AuthState {
            basic: None,
            bearer: None,
            require_auth: false,
            realm: "schema-registry".to_string(),
        };
        for (_name, authorization) in [
            ("bearer", "Bearer some.jwt.token".to_string()),
            ("basic", basic_b64("alice", "pw")),
        ] {
            let decision =
                resolve(&header_map(Some(authorization.as_str())), None, &state, 0).await;
            assert2::assert!(decision == AuthDecision::Unauthorized);
        }
    }

    #[tokio::test]
    async fn invalid_basic_credential_cases_are_unauthorized() {
        use base64::Engine as _;
        let no_colon = base64::engine::general_purpose::STANDARD.encode("justauser");
        let cases = [
            ("malformed base64", "Basic !!!not-base64!!!".to_string()),
            ("missing colon", format!("Basic {no_colon}")),
        ];
        let st = state_with_basic(false);
        for (_name, authorization) in cases {
            let decision = resolve(&header_map(Some(&authorization)), None, &st, 0).await;
            assert2::assert!(decision == AuthDecision::Unauthorized);
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
        assert2::assert!(decision == AuthDecision::Authn(mtls));
    }

    /// Model A: `auth_layer` TRUSTS a request carrying `FORWARD_HEADER` and runs
    /// the handler even under `require_auth` with no credentials (the ingress node
    /// already authenticated it). A non-forwarded credential-less request still
    /// `401`s. This is the mechanism that lets ALL auth methods (incl. mTLS, whose
    /// credential can't cross the secondary→primary hop) work — see `proxy()`,
    /// which forwards no credential, only `FORWARD_HEADER`.
    #[tokio::test]
    async fn forwarded_request_bypasses_require_auth() {
        use axum::{Router, body::Body, routing::get};
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

        for (_name, forwarded, expected) in [
            ("forwarded_request", true, StatusCode::OK),
            ("non_forwarded_request", false, StatusCode::UNAUTHORIZED),
        ] {
            let mut request = Request::builder().uri("/");
            if forwarded {
                request = request.header(crate::rest::forward::FORWARD_HEADER, "ingress-node");
            }
            let response = app
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert2::assert!(response.status() == expected);
        }
    }

    /// cp-byte-exact pin: drive `auth_layer` (Basic configured) over a tiny
    /// router with NO credentials and assert the `401` matches
    /// `mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0` (`tests/fixtures/auth/basic.json`)
    /// byte-for-byte:
    ///
    ///   * status `401`,
    ///   * `WWW-Authenticate: basic realm="SchemaRegistry-Props"` — lowercase
    ///     `basic`, realm = the configured `authentication.realm`,
    ///   * body `{"error_code":401,"message":"Unauthorized"}` with the vendor
    ///     content-type.
    ///
    /// This runs WITHOUT Docker — the durable regression proof that our `401`
    /// reproduces cp's wire bytes.
    #[tokio::test]
    async fn auth_layer_401_matches_cp_byte_exact() {
        use axum::{Router, body::Body, routing::get};
        use tower::ServiceExt as _; // for `oneshot`

        // The realm cp emitted in the capture (its `authentication.realm` = the
        // JAAS entry name). Our binary defaults to the same value.
        let st = AuthState {
            basic: Some(Arc::new(BasicAuthStore::from_users(
                [("alice".to_string(), "pw".to_string())]
                    .into_iter()
                    .collect(),
            ))),
            bearer: None,
            require_auth: true,
            realm: "SchemaRegistry-Props".to_string(),
        };
        let app: Router = Router::new()
            .route("/subjects", get(|| async { "[]" }))
            .layer(axum::middleware::from_fn_with_state(
                Arc::new(st),
                auth_layer,
            ));

        let req = Request::builder()
            .uri("/subjects")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();

        let status = resp.status();
        let www = resp
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .expect("WWW-Authenticate present when Basic configured")
            .to_str()
            .unwrap()
            .to_owned();
        let content_type = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();

        // Body — the exact captured bytes.
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        assert2::assert!(status == StatusCode::UNAUTHORIZED);
        assert2::assert!(www.as_str() == r#"basic realm="SchemaRegistry-Props""#);
        assert2::assert!(content_type.as_str() == crate::error::CONTENT_TYPE);
        assert2::assert!(
            body.as_ref() == br#"{"error_code":401,"message":"Unauthorized"}"#.as_slice()
        );
    }
}
