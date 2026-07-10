//! Streaming Connect handlers — bidirectional `SendStream` (produce) and
//! `Subscribe` (consume). The per-handler logic lives in a `*_inner` function
//! returning a plain `Stream` (unit-testable); the public handler is a thin
//! wrapper into `ConnectResponse::new(StreamBody::new(inner))`.

use std::{net::SocketAddr, pin::Pin, sync::Arc};

use axum::Extension;
use connectrpc_axum::message::{
    ConnectError, ConnectRequest, ConnectResponse, StreamBody, Streaming,
};
use crabka_authz::AuthorizationResult;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_security::Principal;
use futures_util::{Stream, StreamExt};
use jsonpath_rust::{parser::model::JpQuery, query::js_path_process};
use serde_json::Value;

use crate::{
    codec::{SchemaFormat, SchemaMeta},
    consume::ConsumeSession,
    handlers::{anonymous_principal, authorize_resource, to_gateway_record, unknown_host},
    pb,
    state::AppState,
};

struct CompiledPredicates(Vec<CompiledPredicate>);

struct CompiledPredicate {
    query: JpQuery,
    op: pb::PredicateOp,
    expected: PredicateValue,
}

enum PredicateValue {
    String(String),
    Int64(i64),
    Double(f64),
    Bool(bool),
}

fn compile_subscribe_predicates(
    predicates: Vec<pb::FieldPredicate>,
) -> Result<CompiledPredicates, String> {
    let mut compiled = Vec::with_capacity(predicates.len());
    for predicate in predicates {
        let query = jsonpath_rust::parser::parse_json_path(&predicate.path).map_err(|e| {
            format!(
                "invalid Subscribe predicate JSONPath {:?}: {e}",
                predicate.path
            )
        })?;
        let op = pb::PredicateOp::try_from(predicate.op)
            .map_err(|_| "unknown Subscribe predicate op".to_string())?;
        if op != pb::PredicateOp::Equals {
            return Err("Subscribe predicate op must be EQUALS".to_string());
        }
        let expected = match predicate.value {
            Some(pb::field_predicate::Value::StringValue(v)) => PredicateValue::String(v),
            Some(pb::field_predicate::Value::Int64Value(v)) => PredicateValue::Int64(v),
            Some(pb::field_predicate::Value::DoubleValue(v)) if v.is_finite() => {
                PredicateValue::Double(v)
            }
            Some(pb::field_predicate::Value::DoubleValue(_)) => {
                return Err("Subscribe predicate double_value must be finite".to_string());
            }
            Some(pb::field_predicate::Value::BoolValue(v)) => PredicateValue::Bool(v),
            None => {
                return Err("Subscribe predicate requires a value".to_string());
            }
        };
        compiled.push(CompiledPredicate {
            query,
            op,
            expected,
        });
    }
    Ok(CompiledPredicates(compiled))
}

#[cfg(test)]
fn decoded_record_matches(
    predicates: &CompiledPredicates,
    decoded: &crate::codec::Decoded,
) -> bool {
    structured_json_matches(predicates, decoded.json.as_ref())
}

fn structured_json_matches(predicates: &CompiledPredicates, json: Option<&bytes::Bytes>) -> bool {
    if predicates.0.is_empty() {
        return true;
    }
    let Some(json) = json else {
        return false;
    };
    let Ok(value) = serde_json::from_slice::<Value>(json) else {
        return false;
    };
    predicates
        .0
        .iter()
        .all(|predicate| predicate_matches(predicate, &value))
}

fn predicate_matches(predicate: &CompiledPredicate, value: &Value) -> bool {
    if predicate.op != pb::PredicateOp::Equals {
        return false;
    }
    let Ok(matches) = js_path_process(&predicate.query, value) else {
        return false;
    };
    matches
        .into_iter()
        .any(|matched| expected_value_matches(&predicate.expected, matched.val()))
}

fn expected_value_matches(expected: &PredicateValue, actual: &Value) -> bool {
    match expected {
        PredicateValue::String(expected) => actual.as_str() == Some(expected.as_str()),
        PredicateValue::Int64(expected) => {
            actual
                .as_i64()
                .or_else(|| actual.as_str().and_then(|s| s.parse::<i64>().ok()))
                == Some(*expected)
        }
        PredicateValue::Double(expected) => actual
            .as_f64()
            .or_else(|| actual.as_str().and_then(|s| s.parse::<f64>().ok()))
            .is_some_and(|actual| actual.total_cmp(expected).is_eq()),
        PredicateValue::Bool(expected) => actual.as_bool() == Some(*expected),
    }
}

fn schema_meta_to_pb(meta: SchemaMeta) -> pb::SchemaSelector {
    pb::SchemaSelector {
        subject: meta.subject,
        id: meta.id,
        format: match meta.format {
            SchemaFormat::Avro => pb::SchemaFormat::Avro as i32,
            SchemaFormat::Json => pb::SchemaFormat::Json as i32,
            SchemaFormat::Protobuf => pb::SchemaFormat::Protobuf as i32,
        },
    }
}

fn inbound_from_decoded_record(record: crate::consume::DecodedConsumerRecord) -> pb::Inbound {
    pb::Inbound {
        topic: record.topic,
        partition: record.partition.into(),
        offset: record.offset.into(),
        key: record.key.map(|b| b.to_vec()),
        value: record.value.to_vec(),
        headers: std::collections::HashMap::new(),
        timestamp_ms: record.timestamp.into(),
        structured: record.json.map(|json| pb::StructuredValue {
            json: json.to_vec(),
        }),
        schema: record.schema.map(schema_meta_to_pb),
    }
}

/// Produce every record in each inbound `SendRequest`, emitting one `SendAck`
/// (with a per-record `RecordResult` vector) per request. Each record is gated
/// by a Write ACL on its target topic for the on-behalf-of `principal`/`host`;
/// a denied record is skipped and reported as a non-retriable `PERMISSION_DENIED`.
pub fn send_stream_inner(
    mut inbound: Streaming<pb::SendRequest>,
    state: Arc<AppState>,
    principal: Principal,
    host: SocketAddr,
) -> impl Stream<Item = Result<pb::SendAck, ConnectError>> {
    async_stream::stream! {
        while let Some(item) = inbound.next().await {
            let send_req = match item {
                Ok(r) => r,
                Err(e) => { yield Err(e); break; }
            };
            let mut results = Vec::with_capacity(send_req.records.len());
            for r in send_req.records {
                let rec = to_gateway_record(r);
                if authorize_resource(
                    &state,
                    &principal,
                    &host,
                    ResourceType::Topic,
                    &rec.topic,
                    AclOperation::Write,
                ) == AuthorizationResult::Deny
                {
                    results.push(crate::handlers::error_result(
                        &crate::error::GatewayError::Unauthorized(format!(
                            "Write Topic:{}",
                            rec.topic
                        )),
                    ));
                    continue;
                }
                let result = match state.produce.produce(rec, &principal).await {
                    Ok(o) => pb::RecordResult {
                        partition: o.partition.into(),
                        offset: o.offset.into(),
                        deduplicated: o.deduplicated,
                        error: None,
                    },
                    Err(e) => crate::handlers::error_result(&e),
                };
                results.push(result);
            }
            yield Ok(pb::SendAck { results });
        }
    }
}

/// Bidi `SendStream` Connect handler.
pub async fn send_stream(
    Extension(state): Extension<Arc<AppState>>,
    principal: Option<Extension<Principal>>,
    peer: Option<Extension<SocketAddr>>,
    req: ConnectRequest<Streaming<pb::SendRequest>>,
) -> Result<
    ConnectResponse<
        StreamBody<Pin<Box<dyn Stream<Item = Result<pb::SendAck, ConnectError>> + Send>>>,
    >,
    ConnectError,
> {
    let eff = principal.map_or_else(anonymous_principal, |Extension(p)| p);
    let host = peer.map_or_else(unknown_host, |Extension(a)| a);
    Ok(ConnectResponse::new(StreamBody::new(Box::pin(
        send_stream_inner(req.0, state, eff, host),
    ))))
}

/// Join a consumer group on the caller's behalf and stream records. The first
/// frame MUST be `Start`; subsequent `Ack` frames drive offset commits
/// (at-least-once). The subscription ends when the control stream closes or
/// errors.
///
/// Commit semantics: on `Ack`, the session commits its *current* consumed
/// positions for all assigned partitions (the `Ack`'s `topic`/`partition`/
/// `offset` fields are currently advisory — per-offset commit is a follow-up,
/// pending an offset-specific commit API on the consumer). With `auto_commit`,
/// the session commits after each non-empty poll (at enqueue, slightly weaker
/// than on-receipt). For strict at-least-once, the caller should ack
/// synchronously per received batch.
pub fn subscribe_inner(
    mut frames: Streaming<pb::SubscribeFrame>,
    state: Arc<AppState>,
    principal: Principal,
    host: SocketAddr,
) -> impl Stream<Item = Result<pb::Inbound, ConnectError>> {
    async_stream::stream! {
        // First frame must be Start.
        let start = match frames.next().await {
            Some(Ok(pb::SubscribeFrame { frame: Some(pb::subscribe_frame::Frame::Start(s)) })) => s,
            Some(Ok(_)) => { yield Err(ConnectError::new_invalid_argument("first Subscribe frame must be Start")); return; }
            Some(Err(e)) => { yield Err(e); return; }
            None => return,
        };

        // Authorize the subscription up-front: the consumer group (Read) and
        // every topic (Read). Subscribing requires read access to all of them —
        // any Deny ends the stream with a PERMISSION_DENIED (there is no
        // per-topic partial subscribe on the Connect surface).
        if authorize_resource(&state, &principal, &host, ResourceType::Group, &start.group_id, AclOperation::Read)
            == AuthorizationResult::Deny
        {
            yield Err(ConnectError::new_permission_denied(format!("Read Group:{}", start.group_id)));
            return;
        }
        for topic in &start.topics {
            if authorize_resource(&state, &principal, &host, ResourceType::Topic, topic, AclOperation::Read)
                == AuthorizationResult::Deny
            {
                yield Err(ConnectError::new_permission_denied(format!("Read Topic:{topic}")));
                return;
            }
        }

        let predicates = match compile_subscribe_predicates(start.predicates) {
            Ok(predicates) => predicates,
            Err(e) => { yield Err(ConnectError::new_invalid_argument(e)); return; }
        };

        let client_id = format!("{}-sub", state.config.client_id);
        let mut session = match ConsumeSession::new(&state.config.bootstrap, &start.group_id, &client_id, start.topics, state.config.broker_security.clone(), state.codec.clone()).await {
            Ok(s) => s,
            Err(e) => { yield Err(ConnectError::new_internal(e.to_string())); return; }
        };
        let auto_commit = start.auto_commit;

        loop {
            // BORROW NOTE: do NOT call session.commit() inside a select! arm —
            // session.poll(..) holds a &mut borrow across the select. Instead set
            // flags inside the select and commit AFTER it resolves.
            let mut commit = false;
            let mut stop = false;
            let mut to_emit: Vec<pb::Inbound> = Vec::new();
            tokio::select! {
                frame = frames.next() => {
                    match frame {
                        Some(Ok(pb::SubscribeFrame { frame: Some(pb::subscribe_frame::Frame::Ack(_)) })) => commit = true,
                        Some(Ok(_)) => {}
                        Some(Err(e)) => { yield Err(e); stop = true; }
                        None => stop = true,
                    }
                }
                batch = session.poll(std::time::Duration::from_millis(500)) => {
                    match batch {
                        Ok(records) => {
                            for r in records {
                                if !structured_json_matches(&predicates, r.json.as_ref()) {
                                    continue;
                                }
                                to_emit.push(inbound_from_decoded_record(r));
                            }
                            if !to_emit.is_empty() && auto_commit { commit = true; }
                        }
                        Err(e) => { yield Err(ConnectError::new_internal(e.to_string())); stop = true; }
                    }
                }
            }
            for msg in to_emit {
                yield Ok(msg);
            }
            if commit
                && let Err(e) = session.commit().await
            {
                yield Err(ConnectError::new_internal(e.to_string()));
                break;
            }
            if stop { break; }
        }
    }
}

/// Bidi `Subscribe` Connect handler.
pub async fn subscribe(
    Extension(state): Extension<Arc<AppState>>,
    principal: Option<Extension<Principal>>,
    peer: Option<Extension<SocketAddr>>,
    req: ConnectRequest<Streaming<pb::SubscribeFrame>>,
) -> Result<
    ConnectResponse<
        StreamBody<Pin<Box<dyn Stream<Item = Result<pb::Inbound, ConnectError>> + Send>>>,
    >,
    ConnectError,
> {
    let eff = principal.map_or_else(anonymous_principal, |Extension(p)| p);
    let host = peer.map_or_else(unknown_host, |Extension(a)| a);
    Ok(ConnectResponse::new(StreamBody::new(Box::pin(
        subscribe_inner(req.0, state, eff, host),
    ))))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::codec::{Decoded, SchemaFormat, SchemaMeta};

    fn decoded_json(json: &'static [u8]) -> Decoded {
        Decoded {
            value: Bytes::from_static(b"wire"),
            schema: Some(SchemaMeta {
                subject: "metadata-value".to_string(),
                id: 17,
                format: SchemaFormat::Protobuf,
            }),
            json: Some(Bytes::from_static(json)),
        }
    }

    #[test]
    fn subscribe_predicate_matching_cases() {
        use pb::field_predicate::Value;

        for (name, path, value, matching, nonmatching) in [
            (
                "string_field",
                "$.entity_type",
                Value::StringValue("NETWORK_NODE".to_string()),
                br#"{"entity_type":"NETWORK_NODE"}"#.as_slice(),
                br#"{"entity_type":"TOPIC"}"#.as_slice(),
            ),
            (
                "protobuf_int64_json_string",
                "$.node_id",
                Value::Int64Value(7),
                br#"{"node_id":"7"}"#.as_slice(),
                br#"{"node_id":"8"}"#.as_slice(),
            ),
            (
                "bool_field",
                "$.ready",
                Value::BoolValue(true),
                br#"{"ready":true}"#.as_slice(),
                br#"{"ready":false}"#.as_slice(),
            ),
            (
                "finite_double_field",
                "$.load",
                Value::DoubleValue(1.5),
                br#"{"load":1.5}"#.as_slice(),
                br#"{"load":2.5}"#.as_slice(),
            ),
        ] {
            let predicates = compile_subscribe_predicates(vec![pb::FieldPredicate {
                path: path.to_string(),
                op: pb::PredicateOp::Equals as i32,
                value: Some(value),
            }])
            .expect("predicate compiles");

            assert_eq!(
                (
                    decoded_record_matches(&predicates, &decoded_json(matching)),
                    decoded_record_matches(&predicates, &decoded_json(nonmatching)),
                ),
                (true, false),
                "case {name}"
            );
        }
    }

    #[test]
    fn subscribe_predicate_rejects_non_finite_double_value() {
        let result = compile_subscribe_predicates(vec![pb::FieldPredicate {
            path: "$.load".to_string(),
            op: pb::PredicateOp::Equals as i32,
            value: Some(pb::field_predicate::Value::DoubleValue(f64::INFINITY)),
        }]);

        assert!(matches!(
            result,
            Err(err) if err == "Subscribe predicate double_value must be finite"
        ));
    }

    #[test]
    fn subscribe_predicate_rejects_raw_unstructured_records() {
        let predicates = compile_subscribe_predicates(vec![pb::FieldPredicate {
            path: "$.entity_type".to_string(),
            op: pb::PredicateOp::Equals as i32,
            value: Some(pb::field_predicate::Value::StringValue(
                "NETWORK_NODE".to_string(),
            )),
        }])
        .expect("predicate compiles");
        let decoded = Decoded {
            value: Bytes::from_static(b"raw"),
            schema: None,
            json: None,
        };

        assert!(!decoded_record_matches(&predicates, &decoded));
    }

    #[test]
    fn inbound_carries_structured_json_and_schema_metadata() {
        let record = crate::consume::DecodedConsumerRecord {
            topic: "metadata".to_string(),
            partition: crate::ids::PartitionIndex(2),
            offset: crate::ids::Offset(9),
            timestamp: crate::ids::Timestamp(1234),
            key: Some(Bytes::from_static(b"k")),
            value: Bytes::from_static(b"\x08\x07"),
            schema: Some(SchemaMeta {
                subject: "metadata-value".to_string(),
                id: 17,
                format: SchemaFormat::Protobuf,
            }),
            json: Some(Bytes::from_static(br#"{"entity_type":"NETWORK_NODE"}"#)),
        };

        let inbound = inbound_from_decoded_record(record);

        assert_eq!(
            inbound,
            pb::Inbound {
                topic: "metadata".to_string(),
                partition: 2,
                offset: 9,
                key: Some(b"k".to_vec()),
                value: b"\x08\x07".to_vec(),
                headers: std::collections::HashMap::new(),
                timestamp_ms: 1234,
                structured: Some(pb::StructuredValue {
                    json: br#"{"entity_type":"NETWORK_NODE"}"#.to_vec(),
                }),
                schema: Some(pb::SchemaSelector {
                    subject: "metadata-value".to_string(),
                    id: 17,
                    format: pb::SchemaFormat::Protobuf as i32,
                }),
            }
        );
    }
}
