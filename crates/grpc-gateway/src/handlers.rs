//! Connect-RPC handlers — thin adapters: proto in, `GatewayRecord` to the
//! core, `RecordOutcome` back to proto.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Extension;
use connectrpc_axum::message::{ConnectError, ConnectRequest, ConnectResponse};
use crabka_authz::{AuthorizationRequest, AuthorizationResult};
use crabka_metadata::{AclOperation, ResourceType};
use crabka_security::{AuthMethod, Principal};

use crate::pb;
use crate::state::AppState;

/// The principal used when no authenticated identity is present on the request
/// (plaintext listener, or no proxy-injected identity). Mirrors Kafka's
/// `ANONYMOUS` principal so ACLs can target it explicitly.
pub(crate) fn anonymous_principal() -> Principal {
    Principal {
        name: "ANONYMOUS".into(),
        auth_method: AuthMethod::Anonymous,
        groups: vec![],
    }
}

/// The host used when the caller's peer address is not available on the
/// request. Matches the broker's "unknown host" convention for ACL host
/// matching (the `*` host pattern still matches it).
pub(crate) fn unknown_host() -> SocketAddr {
    "0.0.0.0:0".parse().expect("constant socket addr parses")
}

/// Authorize a single `(resource_type, resource_name, operation)` for the
/// effective principal/host against the gateway's ACL cache, emitting an audit
/// log line for the decision. Returns the binary result; callers decide how to
/// surface a `Deny`.
///
/// With the default `AllowAllAuthorizer` this always returns `Allow`, so the
/// gateway's pre-authz behavior is preserved.
pub(crate) fn authorize_resource(
    state: &AppState,
    principal: &Principal,
    host: &SocketAddr,
    resource_type: ResourceType,
    resource_name: &str,
    operation: AclOperation,
) -> AuthorizationResult {
    let req = AuthorizationRequest {
        principal,
        host,
        resource_type,
        resource_name,
        operation,
    };
    let cache = state.authz.cache();
    let result = state.authz.authorizer().authorize(&**cache, &req);
    tracing::info!(
        target: "gateway::audit",
        principal = %principal.name,
        op = ?operation,
        resource_type = ?resource_type,
        resource = %resource_name,
        allowed = matches!(result, AuthorizationResult::Allow),
        "gateway authz",
    );
    result
}

/// Map a produce error to a per-record `RecordResult`. `Unavailable` is
/// retriable (the caller should re-route to another replica); `Unauthorized`
/// is a non-retriable `PERMISSION_DENIED`; everything else is reported
/// non-retriable with a generic code.
pub(crate) fn error_result(e: &crate::error::GatewayError) -> crate::pb::RecordResult {
    use crate::error::GatewayError;
    let retriable = matches!(e, GatewayError::Unavailable);
    let code = match e {
        GatewayError::Unavailable => 14,    // gRPC UNAVAILABLE
        GatewayError::Unauthorized(_) => 7, // gRPC PERMISSION_DENIED
        _ => 1,
    };
    crate::pb::RecordResult {
        partition: -1,
        offset: -1,
        deduplicated: false,
        error: Some(crate::pb::ErrorInfo {
            code,
            message: e.to_string(),
            retriable,
        }),
    }
}

/// Convert a wire [`pb::Record`] into the transport-agnostic [`GatewayRecord`].
pub(crate) fn to_gateway_record(r: crate::pb::Record) -> crate::types::GatewayRecord {
    crate::types::GatewayRecord {
        topic: r.topic,
        key: r.key.map(bytes::Bytes::from),
        value: bytes::Bytes::from(r.value),
        headers: r
            .headers
            .into_iter()
            .map(|(k, v)| (k, bytes::Bytes::from(v)))
            .collect(),
        partition: r.partition,
        timestamp_ms: r.timestamp_ms,
        idempotency_key: r.idempotency_key,
    }
}

pub async fn send(
    Extension(state): Extension<Arc<AppState>>,
    principal: Option<Extension<Principal>>,
    peer: Option<Extension<SocketAddr>>,
    req: ConnectRequest<pb::SendRequest>,
) -> Result<ConnectResponse<pb::SendResponse>, ConnectError> {
    let msg = req.0;
    // Effective identity: the proxy-injected principal (P4 mTLS / parallel
    // bearer task) or ANONYMOUS; the caller's peer address or the unknown host.
    let eff = principal.map_or_else(anonymous_principal, |Extension(p)| p);
    let host = peer.map_or_else(unknown_host, |Extension(a)| a);
    // NOTE (P0–P2): `msg.acks` is accepted on the wire but not yet honored —
    // every record is produced with acks=all, which the dedup/EOS path
    // requires anyway. Per-acks handling on the plain path is deferred.
    let mut results = Vec::with_capacity(msg.records.len());
    for r in msg.records {
        let rec = crate::handlers::to_gateway_record(r);
        // Per-record Write ACL on the target topic. Deny ⇒ skip producing and
        // surface a non-retriable PERMISSION_DENIED for that record only.
        if authorize_resource(
            &state,
            &eff,
            &host,
            ResourceType::Topic,
            &rec.topic,
            AclOperation::Write,
        ) == AuthorizationResult::Deny
        {
            results.push(error_result(&crate::error::GatewayError::Unauthorized(
                format!("Write Topic:{}", rec.topic),
            )));
            continue;
        }
        let result = match state.produce.produce(rec, &eff).await {
            Ok(o) => pb::RecordResult {
                partition: o.partition,
                offset: o.offset,
                deduplicated: o.deduplicated,
                error: None,
            },
            Err(e) => crate::handlers::error_result(&e),
        };
        results.push(result);
    }
    Ok(ConnectResponse::new(pb::SendResponse { results }))
}
