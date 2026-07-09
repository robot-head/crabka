//! JSON-stdio conformance adapter for the Rust app SDK.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use crabka_app_sdk::{
    AcquireOptions, CloudEvent, CrabkaClient, CrabkaError, Filter, PublishOptions, QueueAckEntry,
    QueueAckType, QueueRenewEntry,
};
use crabka_sdk_conformance::protocol::{
    AdapterError, CONTRACT_MAJOR, CONTRACT_MINOR_QUEUE_RPC, Command, ErrorKind, Header, Message,
    Response,
};
use futures_util::StreamExt as _;
use serde_json::json;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, BufWriter};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut adapter = Adapter::default();
    let mut stdin = BufReader::new(tokio::io::stdin());
    let mut stdout = BufWriter::new(tokio::io::stdout());
    let mut line = String::new();
    loop {
        line.clear();
        if stdin.read_line(&mut line).await? == 0 {
            break;
        }
        let command = serde_json::from_str::<Command>(line.trim_end())?;
        let response = adapter.handle(command).await;
        stdout
            .write_all(serde_json::to_string(&response)?.as_bytes())
            .await?;
        stdout.write_all(b"\n").await?;
        stdout.flush().await?;
    }
    Ok(())
}

#[derive(Default)]
struct Adapter {
    client: Option<CrabkaClient>,
    stream: Option<crabka_app_sdk::MessageStream>,
    next_queue_session_id: u64,
    queue_sessions: std::collections::BTreeMap<String, String>,
}

impl Adapter {
    // One match keeps the wire command dispatch auditable against the shared protocol enum.
    #[allow(clippy::too_many_lines)]
    async fn handle(&mut self, command: Command) -> Response {
        match command {
            Command::Hello => Response::Hello {
                contract_major: CONTRACT_MAJOR,
                contract_minor: CONTRACT_MINOR_QUEUE_RPC,
                language: "rust".into(),
            },
            Command::Configure { endpoint, bearer } => {
                let mut builder = CrabkaClient::builder().endpoint(endpoint);
                if let Some(bearer) = bearer {
                    builder = builder.bearer_token(bearer);
                }
                match builder.build() {
                    Ok(client) => {
                        let bearer_configured = client.auth().bearer_token().is_some();
                        self.client = Some(client);
                        Response::Ok(json!({ "bearer_configured": bearer_configured }))
                    }
                    Err(error) => error_response(error),
                }
            }
            Command::Publish {
                topic,
                value_b64,
                headers,
            } => {
                let Some(client) = &self.client else {
                    return error_response(CrabkaError::InvalidArgument(
                        "configure before publish".into(),
                    ));
                };
                let value = match STANDARD.decode(value_b64) {
                    Ok(value) => Bytes::from(value),
                    Err(e) => return error_response(CrabkaError::InvalidArgument(e.to_string())),
                };
                let options = PublishOptions {
                    headers: decode_headers(headers),
                    ..PublishOptions::default()
                };
                match client.messaging().publish(topic, value, options).await {
                    Ok(result) => Response::Ok(json!({
                        "partition": result.partition,
                        "offset": result.offset,
                        "deduplicated": result.deduplicated,
                    })),
                    Err(error) => error_response(error),
                }
            }
            Command::PublishEvent { topic, event } => {
                let Some(client) = &self.client else {
                    return error_response(CrabkaError::InvalidArgument(
                        "configure before publish_event".into(),
                    ));
                };
                let data = match STANDARD.decode(event.data_b64) {
                    Ok(value) => Bytes::from(value),
                    Err(e) => return error_response(CrabkaError::InvalidArgument(e.to_string())),
                };
                let event = CloudEvent {
                    id: event.id,
                    source: event.source,
                    type_: event.type_,
                    specversion: event.specversion,
                    datacontenttype: event.datacontenttype,
                    data,
                };
                match client.messaging().publish_event(topic, event).await {
                    Ok(result) => Response::Ok(json!({
                        "partition": result.partition,
                        "offset": result.offset,
                        "deduplicated": result.deduplicated,
                    })),
                    Err(error) => error_response(error),
                }
            }
            Command::Subscribe {
                topics,
                group,
                filter,
            } => {
                let Some(client) = &self.client else {
                    return error_response(CrabkaError::InvalidArgument(
                        "configure before subscribe".into(),
                    ));
                };
                let filter = filter.map(|filter| Filter {
                    path: filter.path,
                    value: filter.value,
                });
                match client
                    .messaging()
                    .subscribe(&topics, group, filter.as_ref())
                {
                    Ok(stream) => {
                        self.stream = Some(stream);
                        Response::Ok(json!({}))
                    }
                    Err(error) => error_response(error),
                }
            }
            Command::NextMessage { timeout_ms } => {
                let Some(stream) = &mut self.stream else {
                    return error_response(CrabkaError::InvalidArgument(
                        "subscribe before next_message".into(),
                    ));
                };
                match tokio::time::timeout(
                    std::time::Duration::from_millis(timeout_ms),
                    stream.next(),
                )
                .await
                {
                    Ok(Some(Ok(message))) => Response::Message(Message {
                        topic: message.topic,
                        partition: message.partition,
                        offset: message.offset,
                        value_b64: STANDARD.encode(message.value),
                        headers: encode_headers(message.headers),
                    }),
                    Ok(Some(Err(error))) => error_response(error),
                    Ok(None) => {
                        error_response(CrabkaError::NotFound("no message available".into()))
                    }
                    Err(_) => error_response(CrabkaError::NotFound("no message available".into())),
                }
            }
            Command::QueueAcquire {
                topic,
                group,
                max,
                lock_duration_ms,
            } => {
                let Some(client) = &self.client else {
                    return error_response(CrabkaError::InvalidArgument(
                        "configure before queue_acquire".into(),
                    ));
                };
                match client
                    .queues()
                    .acquire(
                        topic,
                        group,
                        AcquireOptions {
                            max,
                            lock_duration_ms,
                            ..AcquireOptions::default()
                        },
                    )
                    .await
                {
                    Ok(result) => {
                        let session_id = self.alias_for_queue_session(result.session_id);
                        Response::Ok(json!({
                            "session_id": session_id,
                            "messages": result.messages.into_iter().map(|message| json!({
                                "message_id": message.message_id(),
                                "topic": message.topic,
                                "partition": message.partition,
                                "offset": message.offset,
                                "value_b64": STANDARD.encode(message.value),
                                "headers": encode_headers(message.headers),
                                "delivery_count": message.delivery_count,
                            })).collect::<Vec<_>>(),
                        }))
                    }
                    Err(error) => error_response(error),
                }
            }
            Command::QueueAck { message_id } => {
                let _ = message_id;
                error_response(CrabkaError::Unimplemented {
                    module: "queues",
                    gated_on: "gateway-sharegroup-rpc",
                })
            }
            Command::QueueAcknowledge {
                session_id,
                entries,
            } => {
                let Some(client) = &self.client else {
                    return error_response(CrabkaError::InvalidArgument(
                        "configure before queue_acknowledge".into(),
                    ));
                };
                let Some(real_session_id) = self.queue_sessions.get(&session_id).cloned() else {
                    let _ = entries;
                    return error_response(CrabkaError::InvalidArgument(
                        "queue session expired; re-acquire".into(),
                    ));
                };
                let entries = match entries
                    .into_iter()
                    .map(|entry| queue_ack_entry(&entry.message_id, entry.ack_type))
                    .collect::<Result<Vec<_>, _>>()
                {
                    Ok(entries) => entries,
                    Err(error) => return error_response(error),
                };
                match client.queues().acknowledge(real_session_id, entries).await {
                    Ok(results) => Response::Ok(json!({ "results": queue_results(results) })),
                    Err(error) => error_response(error),
                }
            }
            Command::QueueRenew {
                session_id,
                entries,
            } => {
                let Some(client) = &self.client else {
                    return error_response(CrabkaError::InvalidArgument(
                        "configure before queue_renew".into(),
                    ));
                };
                let Some(real_session_id) = self.queue_sessions.get(&session_id).cloned() else {
                    let _ = entries;
                    return error_response(CrabkaError::InvalidArgument(
                        "queue session expired; re-acquire".into(),
                    ));
                };
                let entries = match entries
                    .into_iter()
                    .map(|entry| queue_renew_entry(&entry.message_id))
                    .collect::<Result<Vec<_>, _>>()
                {
                    Ok(entries) => entries,
                    Err(error) => return error_response(error),
                };
                match client.queues().renew(real_session_id, entries).await {
                    Ok(results) => Response::Ok(json!({ "results": queue_results(results) })),
                    Err(error) => error_response(error),
                }
            }
            Command::DbConnect { name } => {
                let Some(client) = &self.client else {
                    return error_response(CrabkaError::InvalidArgument(
                        "configure before db_connect".into(),
                    ));
                };
                match client.database().connect(&name) {
                    Ok(()) => Response::Ok(json!({})),
                    Err(error) => error_response(error),
                }
            }
            Command::AuthSignIn { username, password } => {
                let Some(client) = &self.client else {
                    return error_response(CrabkaError::InvalidArgument(
                        "configure before auth_sign_in".into(),
                    ));
                };
                match client.auth().sign_in(&username, &password) {
                    Ok(()) => Response::Ok(json!({})),
                    Err(error) => error_response(error),
                }
            }
            Command::BlobPut { key, value_b64 } => {
                let Some(client) = &self.client else {
                    return error_response(CrabkaError::InvalidArgument(
                        "configure before blob_put".into(),
                    ));
                };
                let value = match STANDARD.decode(value_b64) {
                    Ok(value) => Bytes::from(value),
                    Err(e) => return error_response(CrabkaError::InvalidArgument(e.to_string())),
                };
                match client.blob().put(&key, value) {
                    Ok(()) => Response::Ok(json!({})),
                    Err(error) => error_response(error),
                }
            }
            Command::BlobGet { key } => {
                let Some(client) = &self.client else {
                    return error_response(CrabkaError::InvalidArgument(
                        "configure before blob_get".into(),
                    ));
                };
                match client.blob().get(&key) {
                    Ok(value) => Response::Ok(json!({ "value_b64": STANDARD.encode(value) })),
                    Err(error) => error_response(error),
                }
            }
        }
    }

    fn alias_for_queue_session(&mut self, real_session_id: String) -> String {
        if let Some((alias, _)) = self
            .queue_sessions
            .iter()
            .find(|(_, stored_real_session_id)| *stored_real_session_id == &real_session_id)
        {
            return alias.clone();
        }

        self.next_queue_session_id += 1;
        let alias = format!("queue-session-{}", self.next_queue_session_id);
        self.queue_sessions.insert(alias.clone(), real_session_id);
        alias
    }
}

fn queue_ack_entry(
    message_id: &str,
    ack_type: crabka_sdk_conformance::protocol::QueueAckType,
) -> Result<QueueAckEntry, CrabkaError> {
    let entry = queue_renew_entry(message_id)?;
    Ok(QueueAckEntry {
        topic: entry.topic,
        partition: entry.partition,
        offset: entry.offset,
        ack_type: match ack_type {
            crabka_sdk_conformance::protocol::QueueAckType::Accept => QueueAckType::Accept,
            crabka_sdk_conformance::protocol::QueueAckType::Release => QueueAckType::Release,
            crabka_sdk_conformance::protocol::QueueAckType::Reject => QueueAckType::Reject,
        },
    })
}

fn queue_renew_entry(message_id: &str) -> Result<QueueRenewEntry, CrabkaError> {
    let Some((topic, partition, offset)) = parse_message_id(message_id) else {
        return Err(CrabkaError::InvalidArgument(
            "queue message_id must be topic:partition:offset".into(),
        ));
    };
    Ok(QueueRenewEntry {
        topic,
        partition,
        offset,
    })
}

fn parse_message_id(message_id: &str) -> Option<(String, i32, i64)> {
    let mut parts = message_id.rsplitn(3, ':');
    let offset = parts.next()?.parse().ok()?;
    let partition = parts.next()?.parse().ok()?;
    let topic = parts.next()?.to_string();
    if topic.is_empty() {
        return None;
    }
    Some((topic, partition, offset))
}

fn queue_results(results: Vec<crabka_app_sdk::QueueAckResult>) -> Vec<serde_json::Value> {
    results
        .into_iter()
        .map(|result| {
            json!({
                "message_id": format!("{}:{}:{}", result.entry.topic, result.entry.partition, result.entry.offset),
                "error": result.error.map(adapter_error_value),
            })
        })
        .collect()
}

fn adapter_error_value(error: CrabkaError) -> serde_json::Value {
    let Response::Error(wire) = error_response(error) else {
        unreachable!("error_response returns errors")
    };
    serde_json::to_value(wire).expect("adapter errors serialize")
}

fn decode_headers(headers: Vec<Header>) -> Vec<(String, Option<Bytes>)> {
    headers
        .into_iter()
        .map(|header| {
            let value = header
                .value_b64
                .and_then(|value_b64| STANDARD.decode(value_b64).ok())
                .map(Bytes::from);
            (header.name, value)
        })
        .collect()
}

fn encode_headers(headers: Vec<(String, Option<Bytes>)>) -> Vec<Header> {
    headers
        .into_iter()
        .map(|(name, value)| Header {
            name,
            value_b64: value.map(|value| STANDARD.encode(value)),
        })
        .collect()
}

fn error_response(error: CrabkaError) -> Response {
    let wire = match error {
        CrabkaError::Transport(message) => {
            AdapterError::with_message(ErrorKind::Transport, message)
        }
        CrabkaError::Unauthenticated(message) => {
            AdapterError::with_message(ErrorKind::Unauthenticated, message)
        }
        CrabkaError::InvalidArgument(message) => {
            AdapterError::with_message(ErrorKind::InvalidArgument, message)
        }
        CrabkaError::NotFound(message) => AdapterError::with_message(ErrorKind::NotFound, message),
        CrabkaError::ServerError(message) => {
            AdapterError::with_message(ErrorKind::ServerError, message)
        }
        CrabkaError::Unimplemented { module, gated_on } => {
            AdapterError::unimplemented(module, gated_on)
        }
        _ => AdapterError::with_message(ErrorKind::ServerError, error.to_string()),
    };
    Response::Error(wire)
}
