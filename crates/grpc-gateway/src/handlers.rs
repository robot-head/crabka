//! Connect-RPC handlers — thin adapters: proto in, `GatewayRecord` to the
//! core, `RecordOutcome` back to proto.

use std::{net::SocketAddr, sync::Arc};

use axum::Extension;
use connectrpc_axum::message::{ConnectError, ConnectRequest, ConnectResponse};
use crabka_authz::{AuthorizationRequest, AuthorizationResult};
use crabka_metadata::{AclOperation, ResourceType};
use crabka_security::{AuthMethod, Principal};

use crate::{metrics::metrics, pb, state::AppState};

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
///
/// Codec errors split by cause: a `Registry` transport/availability failure is
/// retriable (the registry may recover), while `Serialize`/`Validate`/`Framing`
/// faults are non-retriable (the same bytes will fail identically).
pub(crate) fn error_result(e: &crate::error::GatewayError) -> crate::pb::RecordResult {
    use crate::{codec::CodecError, error::GatewayError};
    let retriable = matches!(e, GatewayError::Unavailable)
        || matches!(e, GatewayError::Codec(CodecError::Registry(_)));
    let code = match e {
        // gRPC UNAVAILABLE: re-route (Unavailable) or registry transport retry.
        GatewayError::Unavailable | GatewayError::Codec(CodecError::Registry(_)) => 14,
        GatewayError::Unauthorized(_) => 7, // gRPC PERMISSION_DENIED
        GatewayError::Codec(_) => 3,        // gRPC INVALID_ARGUMENT (payload fault)
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

/// Map a wire [`pb::SchemaFormat`] (an i32) to the codec [`SchemaFormat`].
/// `SCHEMA_FORMAT_UNSPECIFIED` (and any unknown value) defaults to Avro — the
/// Confluent default `schemaType`.
fn schema_format_from_pb(format: i32) -> crate::codec::SchemaFormat {
    use crate::codec::SchemaFormat;
    match crate::pb::SchemaFormat::try_from(format) {
        Ok(crate::pb::SchemaFormat::Json) => SchemaFormat::Json,
        Ok(crate::pb::SchemaFormat::Protobuf) => SchemaFormat::Protobuf,
        // AVRO, UNSPECIFIED, or an unknown enum value ⇒ Avro (Confluent default).
        Ok(crate::pb::SchemaFormat::Avro | crate::pb::SchemaFormat::Unspecified) | Err(_) => {
            SchemaFormat::Avro
        }
    }
}

/// Map a wire [`pb::SchemaSelector`] to the codec [`SchemaSelector`]. An empty
/// `subject` ⇒ `None` (`TopicNameStrategy`); a zero `id` ⇒ `None` (resolve latest).
fn schema_selector_from_pb(sel: crate::pb::SchemaSelector) -> crate::codec::SchemaSelector {
    crate::codec::SchemaSelector {
        subject: (!sel.subject.is_empty()).then_some(sel.subject),
        id: (sel.id != 0).then_some(sel.id),
        format: schema_format_from_pb(sel.format),
    }
}

/// Convert a wire [`pb::Record`] into the transport-agnostic [`GatewayRecord`].
///
/// The `body` oneof splits raw vs structured: `raw` (or an absent oneof) keeps
/// `value` with no structured body; `structured` carries the JSON + the
/// record's `schema` selector (the codec serializes it on the produce path),
/// leaving `value` empty.
pub(crate) fn to_gateway_record(r: crate::pb::Record) -> crate::types::GatewayRecord {
    use crate::pb::record::Body;
    let selector = r.schema.map(schema_selector_from_pb);
    let (value, body_structured) = match r.body {
        Some(Body::Structured(sv)) => {
            let json = bytes::Bytes::from(sv.json);
            // A structured body without a schema selector defaults to Avro via
            // TopicNameStrategy (subject/id resolved by the codec).
            let schema = selector.unwrap_or(crate::codec::SchemaSelector {
                subject: None,
                id: None,
                format: crate::codec::SchemaFormat::Avro,
            });
            (bytes::Bytes::new(), Some((json, schema)))
        }
        Some(Body::Raw(raw)) => (bytes::Bytes::from(raw), None),
        None => (bytes::Bytes::new(), None),
    };
    crate::types::GatewayRecord {
        topic: r.topic,
        key: r.key.map(bytes::Bytes::from),
        value,
        body_structured,
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

// RPC request entry (info): one span per Send. `skip_all` keeps the (large)
// request + identity out of the span; `records` carries the batch size. The
// per-record produce loop is NOT separately instrumented (tight loop).
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(records = req.0.records.len()),
)]
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub async fn send(
    Extension(state): Extension<Arc<AppState>>,
    principal: Option<Extension<Principal>>,
    peer: Option<Extension<SocketAddr>>,
    req: ConnectRequest<pb::SendRequest>,
) -> Result<ConnectResponse<pb::SendResponse>, ConnectError> {
    // RED signals: count this request as in-flight for its whole lifetime and
    // time the end-to-end handler latency under the `send` method label. The
    // guard decrements + observes on drop (covering every return path).
    let _req = metrics().begin_request("send");
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
            metrics().record_send("unauthorized");
            results.push(error_result(&crate::error::GatewayError::Unauthorized(
                format!("Write Topic:{}", rec.topic),
            )));
            continue;
        }
        let t0 = std::time::Instant::now();
        let produce_result = state.produce.produce(rec, &eff).await;
        metrics().observe_produce_latency(t0.elapsed().as_secs_f64());
        let result = match produce_result {
            Ok(ref o) if o.deduplicated => {
                metrics().record_send("deduplicated");
                pb::RecordResult {
                    partition: o.partition.into(),
                    offset: o.offset.into(),
                    deduplicated: o.deduplicated,
                    error: None,
                }
            }
            Ok(o) => {
                metrics().record_send("ok");
                pb::RecordResult {
                    partition: o.partition.into(),
                    offset: o.offset.into(),
                    deduplicated: o.deduplicated,
                    error: None,
                }
            }
            Err(ref e @ crate::error::GatewayError::Unauthorized(_)) => {
                metrics().record_send("unauthorized");
                crate::handlers::error_result(e)
            }
            Err(ref e) => {
                metrics().record_send("error");
                crate::handlers::error_result(e)
            }
        };
        results.push(result);
    }
    Ok(ConnectResponse::new(pb::SendResponse { results }))
}
