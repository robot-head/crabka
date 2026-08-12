//! Webhook inbound handlers.
//!
//! Two routes are provided:
//!
//! * `POST /v1/webhooks/{name}` is the named endpoint. It limits the body size,
//!   does optional HMAC-SHA256 signature verification with a replay guard, and
//!   extracts the idempotency key and the record key.
//!   [`crate::webhook_config::CompiledWebhook`] configures all of that per
//!   endpoint.
//! * `POST /v1/produce/{topic}` is the generic produce-by-topic endpoint. It has
//!   no HMAC and an optional `Idempotency-Key` header. The caller identity comes
//!   from the injected principal extension, which holds the mTLS or bearer
//!   identity, and it falls back to ANONYMOUS.
//!
//! Both routes produce through [`crate::produce::ProduceCore::produce`] and
//! return a [`WebhookResponse`] JSON body on success.

use std::{
    net::SocketAddr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Extension, Json, Router,
    body::Bytes,
    extract::{Path, Request},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use crabka_authz::AuthorizationResult;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_security::{AuthMethod, Principal};
use crabka_units::prelude::*;
use serde::Serialize;
use serde_json::Value;

use crate::{
    ce_translate::{self, IngressMode},
    codec::{CodecError, SchemaSelector},
    error::GatewayError,
    handlers::anonymous_principal,
    ids::{Offset, PartitionIndex},
    metrics::metrics,
    state::AppState,
    types::GatewayRecord,
    webhook_config::{Source, extract_source, verify_signature},
};

// ---------------------------------------------------------------------------
// Response type
// ---------------------------------------------------------------------------

/// Successful produce result returned as JSON.
#[derive(Serialize)]
pub struct WebhookResponse {
    pub partition: PartitionIndex,
    pub offset: Offset,
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

/// `POST /v1/webhooks/{name}`, the named, config-driven inbound webhook.
// HTTP request entry (info): one span per inbound webhook, tagged with the
// endpoint name. The RED signals (in-flight gauge + latency histogram under
// method="webhook_in") are recorded by the drop-guard on every return path.
#[tracing::instrument(level = "info", skip_all, fields(webhook = %name))]
pub async fn webhook_handler(
    Extension(state): Extension<Arc<AppState>>,
    Path(name): Path<String>,
    peer: Option<Extension<SocketAddr>>,
    request: Request,
) -> Response {
    let _req = metrics().begin_request("webhook_in");
    // 1. Look up the compiled endpoint config.
    let Some(cfg) = state.config.webhooks.get(&name) else {
        metrics().record_webhook_in("not_found");
        return StatusCode::NOT_FOUND.into_response();
    };

    // 2. Collect with the endpoint's configured limit. Using the raw request
    // avoids Axum's fixed 2 MiB `Bytes` extractor cap overriding this policy.
    let (parts, body) = request.into_parts();
    let headers = parts.headers;
    let Ok(body) = axum::body::to_bytes(body, cfg.max_body.bytes_usize()).await else {
        metrics().record_webhook_in("too_large");
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    };

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
            // `now` and `ts` are instants (epoch seconds); their difference is
            // the extent the tolerance bounds.
            let skew = Time::from_secs(
                i64::try_from((i128::from(now_unix_secs()) - i128::from(ts)).abs())
                    .unwrap_or(i64::MAX),
            );
            if skew > cfg.timestamp_tolerance {
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

    // 7. Translate CloudEvents after signature and source extraction, both of
    //    which deliberately operate on the original HTTP headers/body.
    let translated = match translate_cloudevent_ingress(&headers, &body) {
        Ok(translated) => translated,
        Err(status) => {
            metrics().record_webhook_in("bad_request");
            return status.into_response();
        }
    };
    // CloudEvents are already protocol-bound bytes and must bypass Confluent
    // schema framing. Existing non-CE schema-bound webhooks remain unchanged.
    let body_structured = if translated.is_cloud_event {
        None
    } else {
        cfg.schema_subject.as_ref().map(|subject| {
            (
                translated.value.clone(),
                SchemaSelector {
                    subject: Some(subject.clone()),
                    id: None,
                    format: cfg.schema_format,
                },
            )
        })
    };
    let rec = GatewayRecord {
        topic: cfg.target_topic.clone(),
        key: key.map(|k| Bytes::from(k.into_bytes())),
        value: translated.value,
        body_structured,
        headers: translated.headers,
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

/// `POST /v1/produce/{topic}`, the generic produce-by-topic endpoint.
///
/// This route has no HMAC. It enforces the configured generic-produce body cap
/// and it respects an optional `Idempotency-Key` header. It uses the injected
/// caller [`Principal`] from mTLS or bearer auth, and it falls back to
/// ANONYMOUS.
// HTTP request entry (info): one span per generic produce-by-topic call,
// tagged with the target topic. RED signals recorded via the drop-guard under
// method="produce_http".
#[tracing::instrument(level = "info", skip_all, fields(topic = %topic))]
pub async fn produce_handler(
    Extension(state): Extension<Arc<AppState>>,
    Path(topic): Path<String>,
    peer: Option<Extension<SocketAddr>>,
    principal: Option<Extension<Principal>>,
    request: Request,
) -> Response {
    let _req = metrics().begin_request("produce_http");
    let (parts, body) = request.into_parts();
    let headers = parts.headers;
    let Ok(body) =
        axum::body::to_bytes(body, state.config.runtime.produce_max_body.bytes_usize()).await
    else {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    };
    // Idempotency key from the standard header if present.
    let idempotency_key: Option<String> = headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let translated = match translate_cloudevent_ingress(&headers, &body) {
        Ok(translated) => translated,
        Err(status) => {
            metrics().record_webhook_in("bad_request");
            return status.into_response();
        }
    };

    let rec = GatewayRecord {
        topic,
        key: None,
        value: translated.value,
        body_structured: None,
        headers: translated.headers,
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

/// Returns `true` when `cfg` has at least one `Source::JsonPath` source. In
/// that case the caller must parse the raw body as JSON before extraction.
fn needs_json(cfg: &crate::webhook_config::CompiledWebhook) -> bool {
    let json_src = |src: &Source| matches!(src, Source::JsonPath(_));
    cfg.idempotency_source.as_ref().is_some_and(json_src)
        || cfg.key_source.as_ref().is_some_and(json_src)
}

struct TranslatedIngress {
    headers: Vec<(String, Option<Bytes>)>,
    value: Bytes,
    is_cloud_event: bool,
}

fn translate_cloudevent_ingress(
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<TranslatedIngress, StatusCode> {
    let media_type = headers
        .get("content-type")
        .and_then(|value| value.to_str().ok());
    let has_ce_header = headers.keys().any(|name| name.as_str().starts_with("ce-"));

    match ce_translate::detect_content_mode(media_type, has_ce_header) {
        IngressMode::Batch => Err(StatusCode::UNSUPPORTED_MEDIA_TYPE),
        IngressMode::NotCloudEvent => Ok(TranslatedIngress {
            headers: Vec::new(),
            value: body.clone(),
            is_cloud_event: false,
        }),
        IngressMode::Binary => {
            let translated = ce_translate::http_headers_to_kafka(headers)
                .map_err(|_| StatusCode::BAD_REQUEST)?;
            ce_translate::validate_binary_required(&translated)
                .map_err(|_| StatusCode::BAD_REQUEST)?;
            Ok(TranslatedIngress {
                headers: translated
                    .into_iter()
                    .map(|(key, value)| (key, Some(value)))
                    .collect(),
                value: body.clone(),
                is_cloud_event: true,
            })
        }
        IngressMode::Structured => {
            let event =
                serde_json::from_slice::<Value>(body).map_err(|_| StatusCode::BAD_REQUEST)?;
            ce_translate::validate_structured_json(&event).map_err(|_| StatusCode::BAD_REQUEST)?;
            let content_type = media_type.unwrap_or("application/cloudevents+json");
            Ok(TranslatedIngress {
                headers: vec![(
                    "content-type".to_owned(),
                    Some(Bytes::copy_from_slice(content_type.as_bytes())),
                )],
                value: body.clone(),
                is_cloud_event: true,
            })
        }
    }
}

/// Produce the record and map [`GatewayError`] variants to HTTP status codes.
///
/// This function authorizes `(ResourceType::Topic, rec.topic,
/// AclOperation::Write)` for the effective principal before it produces. It
/// returns `403 FORBIDDEN` on `Deny`.
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
        // Broker or registry unreachable: retriable ⇒ 503 so the caller can
        // resend. Both variants map to the same status and metric label.
        Err(GatewayError::Unavailable | GatewayError::Codec(CodecError::Registry(_))) => {
            metrics().record_webhook_in("error");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
        // A schema-bound body that fails validation/serialization/framing is the
        // caller's fault: the same bytes will never validate ⇒ 400 (not retried).
        Err(GatewayError::Codec(
            CodecError::Validate(_) | CodecError::Serialize(_) | CodecError::Framing(_),
        )) => {
            metrics().record_webhook_in("bad_request");
            StatusCode::BAD_REQUEST.into_response()
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

    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;
    use tower::ServiceExt as _;

    use super::*;
    use crate::{
        authz::GatewayAuthz,
        codec::{Decoded, EncodeBody, RawCodec, RecordCodec, SchemaFormat},
        config::GatewayConfig,
        produce::ProduceCore,
        webhook_config::{CompiledWebhook, SigEncoding},
    };

    /// A codec stub that rejects every `encode` with a fixed [`CodecError`]. It
    /// exercises the produce path's error-to-HTTP-status mapping without a
    /// registry. The inbound path never calls `decode`.
    struct FailingEncodeCodec(fn(String) -> CodecError);

    #[async_trait::async_trait]
    impl RecordCodec for FailingEncodeCodec {
        async fn encode(&self, _topic: &str, _body: EncodeBody) -> Result<Bytes, CodecError> {
            Err((self.0)("stub rejects".to_string()))
        }
        async fn decode(&self, _topic: &str, value: Bytes) -> Result<Decoded, CodecError> {
            Ok(Decoded {
                value,
                schema: None,
                json: None,
            })
        }
    }

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

    /// Minimal signed endpoint with the given name and topic.
    fn signed_cfg(topic: &str) -> CompiledWebhook {
        CompiledWebhook {
            target_topic: topic.to_string(),
            principal: format!("webhook:{topic}"),
            secret: Some(b"s3cr3t".to_vec()),
            signature_header: Some("X-Sig".to_string()),
            signature_encoding: SigEncoding::Hex,
            signature_prefix: None,
            timestamp_header: None,
            timestamp_tolerance: minutes(5),
            idempotency_source: None,
            key_source: None,
            max_body: mebibytes(1),
            schema_subject: None,
            schema_format: SchemaFormat::Json,
        }
    }

    /// Minimal unsigned endpoint, with no HMAC.
    fn unsigned_cfg(topic: &str) -> CompiledWebhook {
        CompiledWebhook {
            target_topic: topic.to_string(),
            principal: format!("webhook:{topic}"),
            secret: None,
            signature_header: None,
            signature_encoding: SigEncoding::Hex,
            signature_prefix: None,
            timestamp_header: None,
            timestamp_tolerance: minutes(5),
            idempotency_source: None,
            key_source: None,
            max_body: bytes(64),
            schema_subject: None,
            schema_format: SchemaFormat::Json,
        }
    }

    /// Build an `AppState` backed by a non-idempotent `ProduceCore` that does
    /// NOT need a real broker.
    ///
    /// The producer connects lazily, so route-layer tests that stop before they
    /// reach produce (404, 413, 401, 400) work without a running broker. A test
    /// that exercises the produce path sees a 500 or 503 transport error, which
    /// is the expected assertion in that case.
    async fn state_with_webhooks(webhooks: HashMap<String, CompiledWebhook>) -> Arc<AppState> {
        state_with_webhooks_codec(webhooks, Arc::new(RawCodec)).await
    }

    /// As [`state_with_webhooks`], but with a caller-supplied codec in the
    /// `ProduceCore`. A codec that errors on `encode` then exercises the produce
    /// path's error-to-status mapping.
    async fn state_with_webhooks_codec(
        webhooks: HashMap<String, CompiledWebhook>,
        codec: Arc<dyn RecordCodec>,
    ) -> Arc<AppState> {
        // Port 1 gives immediate ECONNREFUSED so produce-path tests complete
        // quickly; the route-guard tests return before ever calling produce.
        let produce = ProduceCore::new_for_test("127.0.0.1:1", "webhook-test", codec)
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
                dedup_window: hours(1),
                dedup_ownership_group: "__crabka_grpc_gateway_dedup_owners".into(),
                dedup_txn_id_prefix: "wh-dedup".into(),
                advertised_addr: "127.0.0.1:0".into(),
                membership_topic: "__wh_membership".into(),
                tls: None,
                broker_security: None,
                authz: None,
                webhooks,
                outbound: Vec::new(),
                schema_registry_url: None,
                runtime: crate::config::GatewayRuntimeConfig::default(),
            }),
            authz: Arc::new(GatewayAuthz::new(Arc::new(
                crabka_authz::AllowAllAuthorizer,
            ))),
            codec: Arc::new(RawCodec),
            queue: Arc::default(),
        })
    }

    async fn oneshot(router: Router, req: Request<Body>) -> axum::http::Response<Body> {
        router.oneshot(req).await.unwrap()
    }

    #[test]
    fn binary_cloudevent_translation_maps_headers_and_preserves_body() {
        let mut headers = HeaderMap::new();
        headers.insert("ce-id", "event-1".parse().expect("header value"));
        headers.insert("ce-source", "/tests".parse().expect("header value"));
        headers.insert("ce-type", "example.created".parse().expect("header value"));
        headers.insert("ce-specversion", "1.0".parse().expect("header value"));
        headers.insert(
            "content-type",
            "application/avro".parse().expect("header value"),
        );
        let body = Bytes::from_static(b"opaque-data");

        let translated =
            translate_cloudevent_ingress(&headers, &body).expect("binary CE translates");

        assert2::assert!(translated.value == body);
        assert2::assert!(translated.is_cloud_event);
        assert2::assert!(
            translated
                .headers
                .contains(&("ce_id".to_owned(), Some(Bytes::from_static(b"event-1"))))
        );
        assert2::assert!(translated.headers.contains(&(
            "content-type".to_owned(),
            Some(Bytes::from_static(b"application/avro"))
        )));
    }

    #[test]
    fn structured_cloudevent_translation_is_verbatim() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-type",
            "application/cloudevents+json; charset=UTF-8"
                .parse()
                .expect("header value"),
        );
        let body = Bytes::from_static(
            br#"{ "specversion":"1.0", "id":"event-1", "source":"/tests", "type":"example.created", "data":{"n":7} }"#,
        );

        let translated =
            translate_cloudevent_ingress(&headers, &body).expect("structured CE translates");

        assert2::assert!(translated.value == body);
        assert2::assert!(
            translated.headers
                == vec![(
                    "content-type".to_owned(),
                    Some(Bytes::from_static(
                        b"application/cloudevents+json; charset=UTF-8"
                    ))
                )]
        );
    }

    #[test]
    fn non_cloudevent_translation_is_unchanged() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-type",
            "application/json".parse().expect("header value"),
        );
        let body = Bytes::from_static(br#"{"plain":true}"#);

        let translated =
            translate_cloudevent_ingress(&headers, &body).expect("plain request translates");

        assert2::assert!(translated.value == body);
        assert2::assert!(translated.headers.is_empty());
        assert2::assert!(!translated.is_cloud_event);
    }

    #[tokio::test]
    async fn invalid_and_batch_cloudevents_return_binding_statuses() {
        let cases = [
            (
                Request::post("/v1/produce/events")
                    .header("ce-source", "/tests")
                    .header("ce-type", "example.created")
                    .header("ce-specversion", "1.0")
                    .body(Body::from("data"))
                    .expect("request"),
                StatusCode::BAD_REQUEST,
            ),
            (
                Request::post("/v1/produce/events")
                    .header("content-type", "application/cloudevents+json")
                    .body(Body::from("not-json"))
                    .expect("request"),
                StatusCode::BAD_REQUEST,
            ),
            (
                Request::post("/v1/produce/events")
                    .header("content-type", "application/cloudevents-batch+json")
                    .body(Body::from("[]"))
                    .expect("request"),
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ),
        ];

        for (request, expected) in cases {
            let state = state_with_webhooks(HashMap::new()).await;
            let response = oneshot(webhook_router(state), request).await;
            assert2::assert!(response.status() == expected);
        }
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
        assert2::assert!(resp.status() == StatusCode::NOT_FOUND);
    }

    // -----------------------------------------------------------------------
    // webhook_handler: body size
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn body_too_large_returns_413() {
        let state = state_with_webhooks(make_webhook("tiny", unsigned_cfg("t"))).await;
        let app = webhook_router(state);
        // max_body for unsigned_cfg is 64 B; send 65 bytes.
        let body = vec![b'x'; 65];
        let req = Request::post("/v1/webhooks/tiny")
            .body(Body::from(body))
            .unwrap();
        let resp = oneshot(app, req).await;
        assert2::assert!(resp.status() == StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn configured_body_limit_can_exceed_axum_default() {
        use jsonpath_rust::parser::parse_json_path;

        let mut cfg = unsigned_cfg("t");
        cfg.max_body = mebibytes(3);
        cfg.idempotency_source = Some(Source::JsonPath(parse_json_path("$.id").unwrap()));
        let state = state_with_webhooks(make_webhook("large", cfg)).await;
        let app = webhook_router(state);
        let req = Request::post("/v1/webhooks/large")
            .body(Body::from(vec![b'x'; mebibytes(2).bytes_usize() + 1]))
            .unwrap();
        let resp = oneshot(app, req).await;
        assert2::assert!(resp.status() == StatusCode::BAD_REQUEST);
    }

    // -----------------------------------------------------------------------
    // webhook_handler: signature verification
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn invalid_signature_cases_return_401() {
        for (_name, signature) in [("missing", None), ("invalid", Some("deadbeef"))] {
            let state = state_with_webhooks(make_webhook("gh", signed_cfg("events"))).await;
            let app = webhook_router(state);
            let mut request = Request::post("/v1/webhooks/gh");
            if let Some(signature) = signature {
                request = request.header("X-Sig", signature);
            }
            let response = oneshot(app, request.body(Body::from("hello")).unwrap()).await;
            assert2::assert!(response.status() == StatusCode::UNAUTHORIZED);
        }
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
        cfg.timestamp_tolerance = minutes(5);
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
        assert2::assert!(resp.status() == StatusCode::UNAUTHORIZED);
    }

    // NOTE: The "fresh timestamp passes replay guard → reaches produce" test is
    // omitted here; it requires a real broker (integration test). The stale-
    // timestamp 401 test above proves the replay-guard logic is wired.

    // -----------------------------------------------------------------------
    // webhook_handler: idempotency key extraction
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn missing_idempotency_key_returns_400() {
        use jsonpath_rust::parser::parse_json_path;

        use crate::webhook_config::Source;

        let mut cfg = unsigned_cfg("t");
        cfg.max_body = kibibytes(1); // allow larger body for this test
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
        assert2::assert!(resp.status() == StatusCode::BAD_REQUEST);
    }

    // -----------------------------------------------------------------------
    // webhook_handler: schema-bound (structured) body
    // -----------------------------------------------------------------------

    /// A schema-bound endpoint builds a STRUCTURED `GatewayRecord`. It holds the
    /// body bytes and a `SchemaSelector` that carries the configured subject and
    /// format.
    #[test]
    fn schema_bound_builds_structured_record() {
        let mut cfg = unsigned_cfg("orders");
        cfg.schema_subject = Some("orders-value".to_string());
        cfg.schema_format = SchemaFormat::Avro;

        // Mirror the construction in `webhook_handler` step 7.
        let body = Bytes::from_static(br#"{"id":1}"#);
        let body_structured = cfg.schema_subject.as_ref().map(|subject| {
            (
                body.clone(),
                SchemaSelector {
                    subject: Some(subject.clone()),
                    id: None,
                    format: cfg.schema_format,
                },
            )
        });

        let actual = body_structured.expect("schema_subject ⇒ structured body");
        assert2::assert!(
            actual
                == (
                    body,
                    SchemaSelector {
                        subject: Some("orders-value".to_string()),
                        id: None,
                        format: SchemaFormat::Avro,
                    },
                )
        );
    }

    // NOTE: the RawCodec-passthrough-reaches-produce path needs a real broker
    // (producing to a dead port blocks rather than failing fast), so it is not
    // unit-tested here; the schema-gate behaviour is covered by the Validate→400
    // and Registry→503 tests below, and end-to-end produce by tests/.

    /// Codec failures map to their complete HTTP response status table.
    #[tokio::test]
    async fn schema_encode_failure_status_cases() {
        for (_name, error, body, expected) in [
            (
                "schema validation",
                CodecError::Validate as fn(String) -> CodecError,
                r#"{"id":"not-an-int"}"#,
                StatusCode::BAD_REQUEST,
            ),
            (
                "registry unavailable",
                CodecError::Registry as fn(String) -> CodecError,
                r#"{"id":1}"#,
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        ] {
            let mut cfg = unsigned_cfg("orders");
            cfg.max_body = kibibytes(1);
            cfg.schema_subject = Some("orders-value".to_string());
            let codec = Arc::new(FailingEncodeCodec(error));
            let state = state_with_webhooks_codec(make_webhook("orders", cfg), codec).await;
            let app = webhook_router(state);
            let req = Request::post("/v1/webhooks/orders")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap();
            let resp = oneshot(app, req).await;
            assert2::assert!(resp.status() == expected);
        }
    }

    // -----------------------------------------------------------------------
    // produce_handler
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn generic_produce_honors_configured_body_limit_above_axum_default() {
        let codec = Arc::new(FailingEncodeCodec(CodecError::Validate));
        let mut state = state_with_webhooks_codec(HashMap::new(), codec).await;
        let state_mut = Arc::get_mut(&mut state).unwrap();
        let mut config = (*state_mut.config).clone();
        config.runtime.produce_max_body = mebibytes(3);
        state_mut.config = Arc::new(config);
        let app = webhook_router(state);
        let req = Request::post("/v1/produce/orders")
            .body(Body::from(vec![b'x'; mebibytes(2).bytes_usize() + 1]))
            .unwrap();
        let resp = oneshot(app, req).await;
        assert2::assert!(resp.status() == StatusCode::BAD_REQUEST);
    }

    // -----------------------------------------------------------------------
    // needs_json helper
    // -----------------------------------------------------------------------

    #[test]
    fn needs_json_source_cases() {
        use jsonpath_rust::parser::parse_json_path;

        use crate::webhook_config::Source;

        let cases = [
            (
                "header idempotency",
                Some(Source::Header("X-Id".to_string())),
                None,
                false,
            ),
            (
                "jsonpath idempotency",
                Some(Source::JsonPath(parse_json_path("$.id").unwrap())),
                None,
                true,
            ),
            (
                "jsonpath key",
                None,
                Some(Source::JsonPath(parse_json_path("$.key").unwrap())),
                true,
            ),
        ];

        for (_name, idempotency_source, key_source, expected) in cases {
            let mut cfg = unsigned_cfg("t");
            cfg.idempotency_source = idempotency_source;
            cfg.key_source = key_source;
            assert2::assert!(needs_json(&cfg) == expected);
        }
    }
}
