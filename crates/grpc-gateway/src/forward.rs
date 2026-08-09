//! Internal gateway-to-gateway forwarding.
//!
//! This module holds the owner-routing client and the `/internal/v1/forward`
//! endpoint. The endpoint receives a forwarded record and produces it LOCALLY,
//! because the receiver is the partition's owner.
//!
//! The transport is JSON over HTTP on the gateway's own listener. It is
//! plaintext by default. When the gateway runs with TLS, the transport is
//! mutually-authenticated https (mTLS): the forwarder presents the gateway's
//! own client cert, and the receiver requires a cert-authenticated peer. This
//! INTERNAL protocol is deliberately separate from the public Connect `Send`
//! API, so the two can evolve independently.

use std::sync::Arc;

use axum::{
    Extension, Json, Router,
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use crabka_authz::{AuthorizationRequest, AuthorizationResult};
use crabka_metadata::{AclOperation, ResourceType};
use crabka_security::{AuthMethod, Principal};
use crabka_units::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    error::GatewayError,
    ids::{Offset, PartitionIndex},
    metrics::metrics,
    state::AppState,
    types::{GatewayRecord, RecordOutcome},
};

/// Wire form of a forwarded record. Bytes are JSON arrays, so no extra
/// dependency is needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardRecord {
    pub topic: String,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub partition: Option<i32>,
    pub timestamp_ms: Option<i64>,
    pub idempotency_key: Option<String>,
    /// The ORIGINAL caller's resolved identity. The forwarding gateway relays
    /// it so the owning replica re-authorizes that caller, not the forwarding
    /// gateway's own mTLS identity. `None` ⇒ the owner treats the caller as
    /// ANONYMOUS. The owner trusts the mTLS-authenticated peer gateway to relay
    /// this truthfully, as a trusted-proxy chain.
    pub principal: Option<ForwardPrincipal>,
}

/// Wire form of a resolved caller [`Principal`] carried on a forward.
///
/// `auth_method` is a string because [`AuthMethod`] is not `Serialize`. It
/// round-trips through [`ForwardPrincipal::from_principal`] and
/// [`ForwardPrincipal::to_principal`]. An unknown string maps to
/// [`AuthMethod::Anonymous`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardPrincipal {
    pub name: String,
    pub auth_method: String,
    pub groups: Vec<String>,
}

impl ForwardPrincipal {
    /// Project a resolved session [`Principal`] onto its wire form.
    #[must_use]
    pub fn from_principal(p: &Principal) -> Self {
        Self {
            name: p.name.clone(),
            auth_method: auth_method_to_str(p.auth_method).to_string(),
            groups: p.groups.clone(),
        }
    }

    /// Reconstruct a session [`Principal`] from the wire form. An unknown
    /// `auth_method` string defaults to [`AuthMethod::Anonymous`].
    #[must_use]
    pub fn to_principal(&self) -> Principal {
        Principal {
            name: self.name.clone(),
            auth_method: auth_method_from_str(&self.auth_method),
            groups: self.groups.clone(),
        }
    }
}

/// Stable string tag for an [`AuthMethod`] on the forward wire.
fn auth_method_to_str(m: AuthMethod) -> &'static str {
    match m {
        AuthMethod::Anonymous => "Anonymous",
        AuthMethod::SaslPlain => "SaslPlain",
        AuthMethod::SaslScramSha256 => "SaslScramSha256",
        AuthMethod::SaslScramSha512 => "SaslScramSha512",
        AuthMethod::SaslOAuthBearer => "SaslOAuthBearer",
        AuthMethod::SaslGssapi => "SaslGssapi",
        AuthMethod::MTls => "MTls",
    }
}

/// Inverse of [`auth_method_to_str`]. An unknown tag ⇒ [`AuthMethod::Anonymous`].
fn auth_method_from_str(s: &str) -> AuthMethod {
    match s {
        "SaslPlain" => AuthMethod::SaslPlain,
        "SaslScramSha256" => AuthMethod::SaslScramSha256,
        "SaslScramSha512" => AuthMethod::SaslScramSha512,
        "SaslOAuthBearer" => AuthMethod::SaslOAuthBearer,
        "SaslGssapi" => AuthMethod::SaslGssapi,
        "MTls" => AuthMethod::MTls,
        _ => AuthMethod::Anonymous,
    }
}

impl ForwardRecord {
    fn from_record(r: &GatewayRecord, principal: &Principal) -> Self {
        Self {
            topic: r.topic.clone(),
            key: r.key.as_ref().map(|b| b.to_vec()),
            value: r.value.to_vec(),
            headers: r
                .headers
                .iter()
                .map(|(k, v)| (k.clone(), v.to_vec()))
                .collect(),
            partition: r.partition,
            timestamp_ms: r.timestamp_ms,
            idempotency_key: r.idempotency_key.clone(),
            principal: Some(ForwardPrincipal::from_principal(principal)),
        }
    }

    fn into_record(self) -> GatewayRecord {
        GatewayRecord {
            topic: self.topic,
            key: self.key.map(bytes::Bytes::from),
            value: bytes::Bytes::from(self.value),
            // A forwarded record carries already-encoded bytes (the origin
            // replica ran the codec before forwarding), so it is always raw.
            body_structured: None,
            headers: self
                .headers
                .into_iter()
                .map(|(k, v)| (k, bytes::Bytes::from(v)))
                .collect(),
            partition: self.partition,
            timestamp_ms: self.timestamp_ms,
            idempotency_key: self.idempotency_key,
        }
    }
}

/// Wire form of a forward result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardResult {
    pub partition: PartitionIndex,
    pub offset: Offset,
    pub deduplicated: bool,
    /// Present when the owner could not produce. `retriable` ⇒ the origin maps
    /// it back to `Unavailable`, then retries and re-resolves.
    pub error: Option<ForwardError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardError {
    pub message: String,
    pub retriable: bool,
}

/// reqwest client that forwards a record to the owning replica.
pub struct Forwarder {
    http: reqwest::Client,
    scheme: &'static str,
}

impl Forwarder {
    /// Plaintext forwarder over `http://`, for a gateway that runs without TLS.
    #[must_use]
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::new(),
            scheme: "http",
        }
    }

    /// mTLS forwarder over `https://`. It presents `client_config`'s identity
    /// and trusts its roots. The gateway's own cert authenticates it to the
    /// owning replica.
    ///
    /// # Errors
    /// Returns `GatewayError::Forward` if the reqwest client cannot be built.
    pub fn with_tls(client_config: Arc<rustls::ClientConfig>) -> Result<Self, GatewayError> {
        // reqwest's `use_preconfigured_tls(impl Any)` wraps its arg in `Some(..)`
        // internally and downcasts `Option<rustls::ClientConfig>`, so it must be
        // handed the BARE config (passing `Some(cfg)` double-wraps and fails at
        // runtime with "Unknown TLS backend").
        let http = reqwest::Client::builder()
            .use_preconfigured_tls(Arc::unwrap_or_clone(client_config))
            .build()
            .map_err(|e| GatewayError::Forward(format!("build tls forward client: {e}")))?;
        Ok(Self {
            http,
            scheme: "https",
        })
    }

    /// POST the record to `owner_addr`'s internal forward endpoint.
    ///
    /// A transport failure and an owner-`retriable` error both become
    /// `Unavailable`, so the origin retries and re-resolves to the owner, which
    /// may be a new one. An owner authorization denial, HTTP 403, becomes a
    /// non-retriable `Unauthorized`.
    #[tracing::instrument(skip_all)]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn forward(
        &self,
        owner_addr: &str,
        rec: &GatewayRecord,
        principal: &Principal,
    ) -> Result<RecordOutcome, GatewayError> {
        let url = format!("{}://{}/internal/v1/forward", self.scheme, owner_addr);
        let body = ForwardRecord::from_record(rec, principal);
        let resp = self.http.post(&url).json(&body).send().await.map_err(|_| {
            metrics().record_forward("unavailable");
            GatewayError::Unavailable
        })?;
        // Parse the body regardless of status: the owner returns a JSON
        // ForwardResult even on 403 (authz deny / unauthenticated peer). A
        // denied forward must surface as a NON-retriable error, not a retriable
        // Unavailable, so the caller doesn't retry-loop a permanent denial.
        let status = resp.status();
        match resp.json::<ForwardResult>().await {
            Ok(result) => match result.error {
                None if status.is_success() => {
                    metrics().record_forward("ok");
                    Ok(RecordOutcome {
                        partition: result.partition,
                        offset: result.offset,
                        deduplicated: result.deduplicated,
                    })
                }
                None => {
                    metrics().record_forward("unavailable");
                    Err(GatewayError::Unavailable)
                }
                Some(e) if e.retriable => {
                    metrics().record_forward("unavailable");
                    Err(GatewayError::Unavailable)
                }
                // A non-retriable owner error on a 403 is an authorization
                // denial: surface it as permanent PERMISSION_DENIED, never retry.
                Some(e) if status == reqwest::StatusCode::FORBIDDEN => {
                    metrics().record_forward("unauthorized");
                    Err(GatewayError::Unauthorized(e.message))
                }
                Some(e) => {
                    metrics().record_forward("forward_error");
                    Err(GatewayError::Forward(e.message))
                }
            },
            // A 2xx with an undecodable body is a malformed owner response (fatal);
            // a non-2xx with no JSON body is a transient transport-level failure.
            Err(e) if status.is_success() => {
                metrics().record_forward("forward_error");
                Err(GatewayError::Forward(format!("decode forward result: {e}")))
            }
            Err(_) => {
                metrics().record_forward("unavailable");
                Err(GatewayError::Unavailable)
            }
        }
    }
}

impl Default for Forwarder {
    fn default() -> Self {
        Self::new()
    }
}

/// The `/internal/v1/forward` route. Mount it beside the Connect and health
/// routers on the gateway listener.
#[must_use = "router must be merged into the application"]
pub fn forward_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/internal/v1/forward", post(forward_handler))
        .layer(Extension(state))
}

/// Receiver side: produce LOCALLY, with no further forwarding.
///
/// This replica owns the partition. If it just lost the partition,
/// `produce_local` returns `Unavailable` and the origin retries. This handler
/// never re-forwards, so there are no forward loops.
async fn forward_handler(
    Extension(state): Extension<Arc<AppState>>,
    principal: Option<Extension<crabka_security::Principal>>,
    request: Request,
) -> Response {
    let Ok(body) = axum::body::to_bytes(
        request.into_body(),
        state.config.runtime.forward_max_body.bytes_usize(),
    )
    .await
    else {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    };
    let Ok(req) = serde_json::from_slice::<ForwardRecord>(&body) else {
        return StatusCode::BAD_REQUEST.into_response();
    };

    // When TLS is configured, the internal forward endpoint only accepts a
    // cert-authenticated peer (an mTLS principal must be present). Plaintext
    // mode (no TLS) skips this so existing non-TLS forwarding still works.
    if state.config.tls.is_some() && principal.is_none() {
        return (
            StatusCode::FORBIDDEN,
            Json(ForwardResult {
                partition: PartitionIndex(-1),
                offset: Offset(-1),
                deduplicated: false,
                error: Some(ForwardError {
                    message: "forward requires an authenticated mTLS peer".into(),
                    retriable: false,
                }),
            }),
        )
            .into_response();
    }

    // Re-authorize the ORIGINAL caller (relayed in `req.principal`), NOT the
    // forwarding gateway's mTLS peer identity gated above. The owner trusts the
    // authenticated peer to truthfully relay who the caller is, then applies its
    // own ACL cache against that caller. A missing identity ⇒ ANONYMOUS.
    let caller = req.principal.as_ref().map_or_else(
        crate::handlers::anonymous_principal,
        ForwardPrincipal::to_principal,
    );
    let host = crate::handlers::unknown_host();
    let authz_req = AuthorizationRequest {
        principal: &caller,
        host: &host,
        resource_type: ResourceType::Topic,
        resource_name: &req.topic,
        operation: AclOperation::Write,
    };
    let cache = state.authz.cache();
    let decision = state.authz.authorizer().authorize(&**cache, &authz_req);
    tracing::info!(
        target: "gateway::audit",
        principal = %caller.name,
        op = "Write",
        topic = %req.topic,
        forwarded = true,
        allowed = matches!(decision, AuthorizationResult::Allow),
        "forward authz",
    );
    if decision == AuthorizationResult::Deny {
        return (
            StatusCode::FORBIDDEN,
            Json(ForwardResult {
                partition: PartitionIndex(-1),
                offset: Offset(-1),
                deduplicated: false,
                error: Some(ForwardError {
                    message: format!("Write Topic:{}", req.topic),
                    retriable: false,
                }),
            }),
        )
            .into_response();
    }

    let rec = req.into_record();
    match state.produce.produce_local(rec).await {
        Ok(o) => Json(ForwardResult {
            partition: o.partition,
            offset: o.offset,
            deduplicated: o.deduplicated,
            error: None,
        })
        .into_response(),
        Err(e) => {
            let retriable = matches!(e, GatewayError::Unavailable);
            Json(ForwardResult {
                partition: PartitionIndex(-1),
                offset: Offset(-1),
                deduplicated: false,
                error: Some(ForwardError {
                    message: e.to_string(),
                    retriable,
                }),
            })
            .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::check;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use crabka_authz::{
        AclSource, AllowAllAuthorizer, AuthorizationRequest, AuthorizationResult, Authorizer,
    };
    use crabka_broker::{Broker, BrokerConfig};
    use tempfile::TempDir;
    use tower::ServiceExt;

    use super::*;
    use crate::{
        codec::RawCodec,
        config::{ClientAuthMode, GatewayConfig, TlsSettings},
        dedup::{DedupEngine, store::DedupStore},
        produce::ProduceCore,
    };

    const N: u32 = 4;

    /// Test double that always denies. It drives `forward_handler`'s
    /// authz-deny arm.
    #[derive(Debug)]
    struct DenyAllAuthorizer;

    impl Authorizer for DenyAllAuthorizer {
        fn authorize(
            &self,
            _source: &dyn AclSource,
            _req: &AuthorizationRequest<'_>,
        ) -> AuthorizationResult {
            AuthorizationResult::Deny
        }
    }

    fn config(bootstrap: &str, dedup: &str, tls: Option<TlsSettings>) -> GatewayConfig {
        GatewayConfig {
            bootstrap: bootstrap.into(),
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            client_id: "fh".into(),
            dedup_topic: dedup.into(),
            dedup_partitions: N,
            dedup_window: hours(1),
            dedup_ownership_group: "__crabka_grpc_gateway_dedup_owners".into(),
            dedup_txn_id_prefix: "fh-dedup".into(),
            advertised_addr: "127.0.0.1:0".into(),
            membership_topic: "__crabka_grpc_gateway_membership_fh".into(),
            tls,
            broker_security: None,
            authz: None,
            webhooks: std::collections::HashMap::new(),
            outbound: Vec::new(),
            schema_registry_url: None,
            runtime: crate::config::GatewayRuntimeConfig::default(),
        }
    }

    async fn build_state(
        bootstrap: &str,
        dedup: &str,
        tls: Option<TlsSettings>,
        authorizer: Arc<dyn Authorizer>,
    ) -> Arc<AppState> {
        let store = Arc::new(DedupStore::new(N));
        let engine = Arc::new(DedupEngine::new(
            bootstrap,
            "fh",
            "fh-dedup",
            dedup.to_string(),
            N,
            store,
            None,
        ));
        let produce = ProduceCore::new(bootstrap, "fh", Arc::new(RawCodec), None)
            .await
            .unwrap()
            .with_dedup(engine);
        Arc::new(AppState {
            produce: Arc::new(produce),
            config: Arc::new(config(bootstrap, dedup, tls)),
            authz: Arc::new(crate::authz::GatewayAuthz::new(authorizer)),
            codec: Arc::new(RawCodec),
            queue: Arc::default(),
        })
    }

    fn forward_record(topic: &str) -> ForwardRecord {
        ForwardRecord {
            topic: topic.into(),
            key: None,
            value: vec![1],
            headers: vec![],
            partition: None,
            timestamp_ms: None,
            idempotency_key: Some("k".into()),
            principal: Some(ForwardPrincipal {
                name: "alice".into(),
                auth_method: "MTls".into(),
                groups: vec![],
            }),
        }
    }

    async fn post_forward(app: Router, fr: &ForwardRecord) -> (StatusCode, ForwardResult) {
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/v1/forward")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(fr).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let result: ForwardResult = serde_json::from_slice(&bytes).unwrap();
        (status, result)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forward_handler_honors_configured_body_limit_above_axum_default() {
        const DEDUP: &str = "__crabka_grpc_dedup_fh_body_limit";
        let dir = TempDir::new().unwrap();
        let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let mut state = build_state(&bootstrap, DEDUP, None, Arc::new(DenyAllAuthorizer)).await;
        let state_mut = Arc::get_mut(&mut state).unwrap();
        let mut config = (*state_mut.config).clone();
        config.runtime.forward_max_body = mebibytes(4);
        state_mut.config = Arc::new(config);

        let mut record = forward_record("orders");
        record.value = vec![255; 600_000];
        let body = serde_json::to_vec(&record).unwrap();
        assert2::assert!(body.len() > mebibytes(2).bytes_usize());
        assert2::assert!(body.len() < mebibytes(4).bytes_usize());

        let app = forward_router(state);
        let response = app
            .clone()
            .oneshot(
                Request::post("/internal/v1/forward")
                    .header("content-type", "application/json")
                    .body(Body::from(body.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert2::assert!(response.status() == StatusCode::FORBIDDEN);

        let limited_state = build_state(&bootstrap, DEDUP, None, Arc::new(DenyAllAuthorizer)).await;
        let response = forward_router(limited_state)
            .oneshot(
                Request::post("/internal/v1/forward")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert2::assert!(response.status() == StatusCode::PAYLOAD_TOO_LARGE);
        broker.shutdown().await;
    }

    /// The TLS-required 403 gate reports the `(-1, -1)` sentinel coordinates.
    /// This test pins the `PartitionIndex(-1)` and `Offset(-1)` in the
    /// mTLS-reject arm.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forward_handler_tls_reject_uses_sentinel_coordinates() {
        const DEDUP: &str = "__crabka_grpc_dedup_fh_tls_sentinel";
        let dir = TempDir::new().unwrap();
        let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let tls = Some(TlsSettings {
            cert_chain_path: "/nonexistent/cert.pem".into(),
            private_key_path: "/nonexistent/key.pem".into(),
            trust_roots_path: None,
            client_ca_path: None,
            client_auth: ClientAuthMode::Disabled,
            reload_interval: secs(30),
        });
        let state = build_state(&bootstrap, DEDUP, tls, Arc::new(AllowAllAuthorizer)).await;

        // No principal extension on the request => anonymous => TLS gate fires.
        let (status, result) = post_forward(forward_router(state), &forward_record("t")).await;

        check!(
            (status, result.partition, result.offset)
                == (StatusCode::FORBIDDEN, PartitionIndex(-1), Offset(-1))
        );
        broker.shutdown().await;
    }

    /// The authz-deny 403 arm reports the `(-1, -1)` sentinel coordinates.
    /// This test pins the `PartitionIndex(-1)` and `Offset(-1)` in the deny arm.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forward_handler_authz_deny_uses_sentinel_coordinates() {
        const DEDUP: &str = "__crabka_grpc_dedup_fh_deny_sentinel";
        let dir = TempDir::new().unwrap();
        let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        // tls: None => TLS gate skipped; DenyAllAuthorizer => authz-deny arm.
        let state = build_state(&bootstrap, DEDUP, None, Arc::new(DenyAllAuthorizer)).await;

        let (status, result) = post_forward(forward_router(state), &forward_record("t")).await;

        check!(
            (
                status,
                result.partition,
                result.offset,
                result.error.as_ref().map(|error| error.message.as_str()),
            ) == (
                StatusCode::FORBIDDEN,
                PartitionIndex(-1),
                Offset(-1),
                Some("Write Topic:t"),
            )
        );
        broker.shutdown().await;
    }

    /// The produce-local error arm reports the `(-1, -1)` sentinel coordinates.
    /// This test pins the `PartitionIndex(-1)` and `Offset(-1)` in the
    /// produce-error arm. The empty `DedupStore` owns no partition, so
    /// `produce_local` returns `Unavailable` before any write.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forward_handler_produce_error_uses_sentinel_coordinates() {
        const DEDUP: &str = "__crabka_grpc_dedup_fh_prod_sentinel";
        let dir = TempDir::new().unwrap();
        let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        // tls: None + AllowAll => request reaches produce_local, which errors.
        let state = build_state(&bootstrap, DEDUP, None, Arc::new(AllowAllAuthorizer)).await;

        let (status, result) = post_forward(forward_router(state), &forward_record("t")).await;

        check!(
            (
                status,
                result.error.is_some(),
                result.partition,
                result.offset
            ) == (StatusCode::OK, true, PartitionIndex(-1), Offset(-1))
        );
        broker.shutdown().await;
    }
}
