//! Webhook inbound handlers.
//!
//! Two routes are provided:
//!
//! * `POST /v1/webhooks/{name}` — named endpoint: body size-limited, optional
//!   HMAC-SHA256 signature verification (with replay guard), idempotency-key
//!   extraction, and record-key extraction — all configured per-endpoint in
//!   [`crate::webhook_config::CompiledWebhook`].
//! * `POST /v1/produce/{topic}` — generic produce-by-topic endpoint: no HMAC;
//!   optional `Idempotency-Key` header; caller identity from the injected
//!   principal extension (mTLS / bearer) or ANONYMOUS.
//!
//! Both routes produce via [`crate::produce::ProduceCore::produce`] and return
//! a [`WebhookResponse`] JSON body on success.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Extension;
use axum::body::Bytes;
use axum::extract::Path;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use crabka_authz::AuthorizationResult;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_security::{AuthMethod, Principal};
use serde::Serialize;
use serde_json::Value;

use crate::error::GatewayError;
use crate::handlers::anonymous_principal;
use crate::metrics::metrics;
use crate::state::AppState;
use crate::types::GatewayRecord;
use crate::webhook_config::{Source, extract_source, verify_signature};

// ---------------------------------------------------------------------------
// Response type
// ---------------------------------------------------------------------------

/// Successful produce result returned as JSON.
#[derive(Serialize)]
pub struct WebhookResponse {
    pub partition: i32,
    pub offset: i64,
    pub deduplicated: bool,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Mount the webhook routes on a new [`Router`].
///
/// # Routes
///
/// * `POST /v1/webhooks/{name}` → [`webhook_handler`]
/// * `POST /v1/produce/{topic}`  → [`produce_handler`]
#[must_use = "router must be merged into the application"]
pub fn webhook_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/v1/webhooks/{name}", post(webhook_handler))
        .route("/v1/produce/{topic}", post(produce_handler))
        .layer(Extension(state))
}

// ---------------------------------------------------------------------------
// Named webhook handler
// ---------------------------------------------------------------------------

/// `POST /v1/webhooks/{name}` — named, config-driven inbound webhook.
pub async fn webhook_handler(
    Extension(state): Extension<Arc<AppState>>,
    Path(name): Path<String>,
    peer: Option<Extension<SocketAddr>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // 1. Look up the compiled endpoint config.
    let Some(cfg) = state.config.webhooks.get(&name) else {
        metrics().record_webhook_in("not_found");
        return StatusCode::NOT_FOUND.into_response();
    };

    // 2. Body size guard.
    if body.len() > cfg.max_body_bytes {
        metrics().record_webhook_in("too_large");
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    }

    // 3. HMAC signature verification (when configured).
    if let Some(sig_header) = &cfg.signature_header {
        // Read the signature header value.
        let provided = if let Some(v) = headers.get(sig_header).and_then(|v| v.to_str().ok()) {
            v.to_owned()
        } else {
            metrics().record_webhook_in("unauthenticated");
            return StatusCode::UNAUTHORIZED.into_response();
        };

        // Replay guard: validate the timestamp before the HMAC check so a
        // stale-timestamp request is rejected without doing crypto work.
        if let Some(ts_header) = &cfg.timestamp_header {
            let ts_str = if let Some(v) = headers.get(ts_header).and_then(|v| v.to_str().ok()) {
                v.to_owned()
            } else {
                metrics().record_webhook_in("unauthenticated");
                return StatusCode::UNAUTHORIZED.into_response();
            };
            let ts: i64 = if let Ok(v) = ts_str.parse() {
                v
            } else {
                metrics().record_webhook_in("unauthenticated");
                return StatusCode::UNAUTHORIZED.into_response();
            };
            let now = now_unix_secs();
            let skew = (i128::from(now) - i128::from(ts)).abs();
            if skew > i128::from(cfg.timestamp_tolerance_secs) {
                metrics().record_webhook_in("unauthenticated");
                return StatusCode::UNAUTHORIZED.into_response();
            }
        }

        // Verify the HMAC-SHA256 digest.
        if !verify_signature(
            cfg.secret.as_deref().unwrap_or(b""),
            &body,
            &provided,
            &cfg.signature_encoding,
            cfg.signature_prefix.as_deref(),
        ) {
            metrics().record_webhook_in("unauthenticated");
            return StatusCode::UNAUTHORIZED.into_response();
        }
    }

    // 4. Parse JSON body once — only when at least one source requires it.
    let body_json: Option<Value> = if needs_json(cfg) {
        serde_json::from_slice(&body).ok()
    } else {
        None
    };

    // 5. Idempotency key extraction.
    let idempotency_key: Option<String> = match &cfg.idempotency_source {
        Some(src) => {
            if let Some(k) = extract_source(src, &headers, body_json.as_ref()) {
                Some(k)
            } else {
                metrics().record_webhook_in("bad_request");
                return StatusCode::BAD_REQUEST.into_response();
            }
        }
        None => None,
    };

    // 6. Record key extraction (optional; None ⇒ producer partitioner chooses).
    let key: Option<String> = match &cfg.key_source {
        Some(src) => extract_source(src, &headers, body_json.as_ref()),
        None => None,
    };

    // 7. Build the transport-agnostic record.
    let rec = GatewayRecord {
        topic: cfg.target_topic.clone(),
        key: key.map(|k| Bytes::from(k.into_bytes())),
        value: body,
        headers: vec![],
        partition: None,
        timestamp_ms: None,
        idempotency_key,
    };

    // 8. Build the principal from the endpoint's configured service identity.
    let principal = Principal {
        name: cfg.principal.clone(),
        auth_method: AuthMethod::MTls,
        groups: vec![],
    };

    // 9. Produce and map the result to HTTP status.
    let host = peer.map_or_else(crate::handlers::unknown_host, |p| p.0);
    produce_and_respond(state, rec, &principal, host).await
}

// ---------------------------------------------------------------------------
// Generic produce handler
// ---------------------------------------------------------------------------

/// `POST /v1/produce/{topic}` — generic produce-by-topic endpoint.
///
/// No HMAC or body-size enforcement; respects an optional `Idempotency-Key`
/// header; uses the injected caller [`Principal`] (from mTLS / bearer auth) or
/// falls back to ANONYMOUS.
pub async fn produce_handler(
    Extension(state): Extension<Arc<AppState>>,
    Path(topic): Path<String>,
    peer: Option<Extension<SocketAddr>>,
    headers: HeaderMap,
    principal: Option<Extension<Principal>>,
    body: Bytes,
) -> Response {
    // Idempotency key from the standard header if present.
    let idempotency_key: Option<String> = headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let rec = GatewayRecord {
        topic,
        key: None,
        value: body,
        headers: vec![],
        partition: None,
        timestamp_ms: None,
        idempotency_key,
    };

    let eff = principal.map_or_else(anonymous_principal, |Extension(p)| p);
    let host = peer.map_or_else(crate::handlers::unknown_host, |p| p.0);

    produce_and_respond(state, rec, &eff, host).await
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Returns `true` when `cfg` has at least one `Source::JsonPath` source,
/// meaning the raw body must be parsed as JSON before extraction.
fn needs_json(cfg: &crate::webhook_config::CompiledWebhook) -> bool {
    let json_src = |src: &Source| matches!(src, Source::JsonPath(_));
    cfg.idempotency_source.as_ref().is_some_and(json_src)
        || cfg.key_source.as_ref().is_some_and(json_src)
}

/// Produce the record and map [`GatewayError`] variants to HTTP status codes.
///
/// Authorizes `(ResourceType::Topic, rec.topic, AclOperation::Write)` for the
/// effective principal before producing; returns `403 FORBIDDEN` on `Deny`.
async fn produce_and_respond(
    state: Arc<AppState>,
    rec: GatewayRecord,
    principal: &Principal,
    host: SocketAddr,
) -> Response {
    if crate::handlers::authorize_resource(
        &state,
        principal,
        &host,
        ResourceType::Topic,
        &rec.topic,
        AclOperation::Write,
    ) == AuthorizationResult::Deny
    {
        metrics().record_webhook_in("unauthorized");
        return StatusCode::FORBIDDEN.into_response();
    }
    match state.produce.produce(rec, principal).await {
        Ok(o) => {
            metrics().record_webhook_in("ok");
            (
                StatusCode::OK,
                Json(WebhookResponse {
                    partition: o.partition,
                    offset: o.offset,
                    deduplicated: o.deduplicated,
                }),
            )
                .into_response()
        }
        Err(GatewayError::Unauthorized(_)) => {
            metrics().record_webhook_in("unauthorized");
            StatusCode::FORBIDDEN.into_response()
        }
        Err(GatewayError::Unavailable) => {
            metrics().record_webhook_in("error");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
        Err(_) => {
            metrics().record_webhook_in("error");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// Current Unix time in whole seconds.
fn now_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs().cast_signed())
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;
    use tower::ServiceExt as _;

    use super::*;
    use crate::authz::GatewayAuthz;
    use crate::codec::RawCodec;
    use crate::config::GatewayConfig;
    use crate::produce::ProduceCore;
    use crate::webhook_config::{CompiledWebhook, SigEncoding};

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn hmac_hex(secret: &[u8], body: &[u8]) -> String {
        let mut mac = <Hmac<Sha256>>::new_from_slice(secret).unwrap();
        mac.update(body);
        hex::encode(mac.finalize().into_bytes())
    }

    fn make_webhook(name: &str, cfg: CompiledWebhook) -> HashMap<String, CompiledWebhook> {
        let mut m = HashMap::new();
        m.insert(name.to_string(), cfg);
        m
    }

    /// Minimal signed endpoint with the given name / topic.
    fn signed_cfg(topic: &str) -> CompiledWebhook {
        CompiledWebhook {
            target_topic: topic.to_string(),
            principal: format!("webhook:{topic}"),
            secret: Some(b"s3cr3t".to_vec()),
            signature_header: Some("X-Sig".to_string()),
            signature_encoding: SigEncoding::Hex,
            signature_prefix: None,
            timestamp_header: None,
            timestamp_tolerance_secs: 300,
            idempotency_source: None,
            key_source: None,
            max_body_bytes: 1024 * 1024,
        }
    }

    /// Minimal unsigned endpoint (no HMAC).
    fn unsigned_cfg(topic: &str) -> CompiledWebhook {
        CompiledWebhook {
            target_topic: topic.to_string(),
            principal: format!("webhook:{topic}"),
            secret: None,
            signature_header: None,
            signature_encoding: SigEncoding::Hex,
            signature_prefix: None,
            timestamp_header: None,
            timestamp_tolerance_secs: 300,
            idempotency_source: None,
            key_source: None,
            max_body_bytes: 64,
        }
    }

    /// Build an `AppState` backed by a non-idempotent `ProduceCore` that does
    /// NOT require a real broker. The producer connects lazily, so route-layer
    /// tests that short-circuit before reaching produce (404, 413, 401, 400)
    /// work without a running broker. Tests that exercise the produce path see
    /// a 500/503 transport error, which is the expected assertion in those cases.
    async fn state_with_webhooks(webhooks: HashMap<String, CompiledWebhook>) -> Arc<AppState> {
        // Port 1 gives immediate ECONNREFUSED so produce-path tests complete
        // quickly; the route-guard tests return before ever calling produce.
        let produce = ProduceCore::new_for_test("127.0.0.1:1", "webhook-test", Arc::new(RawCodec))
            .await
            .expect("non-idempotent producer builds without connecting");
        Arc::new(AppState {
            produce: Arc::new(produce),
            config: Arc::new(GatewayConfig {
                bootstrap: "127.0.0.1:0".to_string(),
                listen_addr: "127.0.0.1:0".parse().unwrap(),
                client_id: "webhook-test".into(),
                dedup_topic: "__wh_dedup".into(),
                dedup_partitions: 4,
                dedup_window_ms: 3_600_000,
                dedup_txn_id_prefix: "wh-dedup".into(),
                advertised_addr: "127.0.0.1:0".into(),
                membership_topic: "__wh_membership".into(),
                tls: None,
                authz: None,
                webhooks,
                outbound: Vec::new(),
            }),
            authz: Arc::new(GatewayAuthz::new(Arc::new(
                crabka_authz::AllowAllAuthorizer,
            ))),
        })
    }

    async fn oneshot(router: Router, req: Request<Body>) -> axum::http::Response<Body> {
        router.oneshot(req).await.unwrap()
    }

    // -----------------------------------------------------------------------
    // webhook_handler: name lookup
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn unknown_name_returns_404() {
        let state = state_with_webhooks(HashMap::new()).await;
        let app = webhook_router(state);
        let req = Request::post("/v1/webhooks/missing")
            .body(Body::from("body"))
            .unwrap();
        let resp = oneshot(app, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // -----------------------------------------------------------------------
    // webhook_handler: body size
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn body_too_large_returns_413() {
        let state = state_with_webhooks(make_webhook("tiny", unsigned_cfg("t"))).await;
        let app = webhook_router(state);
        // max_body_bytes for unsigned_cfg is 64; send 65 bytes.
        let body = vec![b'x'; 65];
        let req = Request::post("/v1/webhooks/tiny")
            .body(Body::from(body))
            .unwrap();
        let resp = oneshot(app, req).await;
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    // -----------------------------------------------------------------------
    // webhook_handler: signature verification
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn missing_sig_header_returns_401() {
        let state = state_with_webhooks(make_webhook("gh", signed_cfg("events"))).await;
        let app = webhook_router(state);
        // No X-Sig header.
        let req = Request::post("/v1/webhooks/gh")
            .body(Body::from("hello"))
            .unwrap();
        let resp = oneshot(app, req).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn bad_sig_returns_401() {
        let state = state_with_webhooks(make_webhook("gh", signed_cfg("events"))).await;
        let app = webhook_router(state);
        let req = Request::post("/v1/webhooks/gh")
            .header("X-Sig", "deadbeef")
            .body(Body::from("hello"))
            .unwrap();
        let resp = oneshot(app, req).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // NOTE: tests that pass the auth guard and reach the produce layer (e.g.
    // "correct_sig → attempt produce") require a real broker; they live in the
    // integration-test suite, not here. The guard tests above (401, 413, 400,
    // 404) are sufficient to prove the routing/auth logic without a running
    // broker.

    // -----------------------------------------------------------------------
    // webhook_handler: timestamp replay guard
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn stale_timestamp_returns_401() {
        let mut cfg = signed_cfg("events");
        cfg.timestamp_header = Some("X-Ts".to_string());
        cfg.timestamp_tolerance_secs = 300;
        let state = state_with_webhooks(make_webhook("ts", cfg)).await;
        let app = webhook_router(state);

        let body = b"hello";
        let sig = hmac_hex(b"s3cr3t", body);
        // Timestamp 10 minutes in the past — outside tolerance.
        let old_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .cast_signed()
            - 601_i64;
        let req = Request::post("/v1/webhooks/ts")
            .header("X-Sig", sig)
            .header("X-Ts", old_ts.to_string())
            .body(Body::from(body.as_slice()))
            .unwrap();
        let resp = oneshot(app, req).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // NOTE: The "fresh timestamp passes replay guard → reaches produce" test is
    // omitted here; it requires a real broker (integration test). The stale-
    // timestamp 401 test above proves the replay-guard logic is wired.

    // -----------------------------------------------------------------------
    // webhook_handler: idempotency key extraction
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn missing_idempotency_key_returns_400() {
        use crate::webhook_config::Source;
        use jsonpath_rust::parser::parse_json_path;

        let mut cfg = unsigned_cfg("t");
        cfg.max_body_bytes = 1024; // allow larger body for this test
        // Require idempotency key from a JSON path that won't exist.
        let q = parse_json_path("$.id").unwrap();
        cfg.idempotency_source = Some(Source::JsonPath(q));

        let state = state_with_webhooks(make_webhook("idem", cfg)).await;
        let app = webhook_router(state);
        // JSON body without the `id` field.
        let req = Request::post("/v1/webhooks/idem")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"type":"push"}"#))
            .unwrap();
        let resp = oneshot(app, req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // -----------------------------------------------------------------------
    // produce_handler
    // -----------------------------------------------------------------------
    // NOTE: produce_handler tests that call produce require a real broker.
    // They are covered by integration tests. The needs_json and signature
    // tests above cover the guard logic without a broker.

    // -----------------------------------------------------------------------
    // needs_json helper
    // -----------------------------------------------------------------------

    #[test]
    fn needs_json_false_when_no_json_sources() {
        use crate::webhook_config::Source;

        let mut cfg = unsigned_cfg("t");
        // header: source — does NOT require JSON parse.
        cfg.idempotency_source = Some(Source::Header("X-Id".to_string()));
        assert!(!needs_json(&cfg));
    }

    #[test]
    fn needs_json_true_when_jsonpath_idempotency() {
        use crate::webhook_config::Source;
        use jsonpath_rust::parser::parse_json_path;

        let mut cfg = unsigned_cfg("t");
        let q = parse_json_path("$.id").unwrap();
        cfg.idempotency_source = Some(Source::JsonPath(q));
        assert!(needs_json(&cfg));
    }

    #[test]
    fn needs_json_true_when_jsonpath_key() {
        use crate::webhook_config::Source;
        use jsonpath_rust::parser::parse_json_path;

        let mut cfg = unsigned_cfg("t");
        let q = parse_json_path("$.key").unwrap();
        cfg.key_source = Some(Source::JsonPath(q));
        assert!(needs_json(&cfg));
    }
}
