//! In-process mock adapter used to validate the harness and vectors.

use std::collections::HashMap;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::json;
use tokio::io::{AsyncBufRead, AsyncBufReadExt as _, AsyncWrite, AsyncWriteExt as _};

use crate::protocol::{
    AdapterError, CONTRACT_MAJOR, CONTRACT_MINOR_QUEUE_RPC, CONTRACT_MINOR_V1_0, Command,
    ErrorKind, Filter, FilterOp, Header, Message, QueueAckEntry, QueueAckType, QueueRenewEntry,
    Response,
};

#[derive(Debug, Clone)]
struct StoredMessage {
    topic: String,
    partition: i32,
    offset: i64,
    value_b64: String,
    headers: Vec<Header>,
    queue_states: HashMap<String, QueueDeliveryState>,
}

#[derive(Debug, Clone)]
struct QueueSession {
    topic: String,
    group: String,
    max_messages: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueMessageState {
    Available,
    Acquired,
    Accepted,
    Rejected,
}

#[derive(Debug, Clone)]
struct QueueDeliveryState {
    state: QueueMessageState,
    session_id: Option<String>,
    delivery_count: u32,
}

#[derive(Debug, Clone)]
struct Subscription {
    topics: Vec<String>,
    filter: Option<Filter>,
    next_index: usize,
}

/// Fault injection mode for harness negative tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultMode {
    /// Normal mock behavior.
    None,
    /// Return an incorrect response for publish commands.
    WrongPublish,
    /// Return an incorrect response for configure commands.
    WrongConfigure,
}

/// Mock adapter with deterministic in-memory publish/subscribe behavior.
#[derive(Debug, Clone)]
pub struct MockAdapter {
    endpoint: String,
    bearer: Option<String>,
    messages: Vec<StoredMessage>,
    subscription: Option<Subscription>,
    fault_mode: FaultMode,
    contract_minor: u16,
    next_session_id: u64,
    queue_sessions: HashMap<String, QueueSession>,
}

impl Default for MockAdapter {
    fn default() -> Self {
        Self {
            endpoint: "mock://gateway".into(),
            bearer: None,
            messages: vec![],
            subscription: None,
            fault_mode: FaultMode::None,
            contract_minor: CONTRACT_MINOR_V1_0,
            next_session_id: 1,
            queue_sessions: HashMap::new(),
        }
    }
}

impl MockAdapter {
    /// Create a mock adapter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a mock adapter with a fault mode.
    #[must_use]
    pub fn with_fault_mode(fault_mode: FaultMode) -> Self {
        Self {
            fault_mode,
            ..Self::default()
        }
    }

    /// Create a mock adapter with an explicit contract minor.
    #[must_use]
    pub fn with_contract_minor(contract_minor: u16, fault_mode: FaultMode) -> Self {
        Self {
            fault_mode,
            contract_minor,
            ..Self::default()
        }
    }

    /// Handle one protocol command.
    #[must_use]
    pub fn handle(&mut self, command: Command) -> Response {
        match command {
            Command::Hello => Response::Hello {
                contract_major: CONTRACT_MAJOR,
                contract_minor: self.contract_minor,
                language: "mock".into(),
            },
            Command::Configure { endpoint, bearer } => {
                self.endpoint = endpoint;
                self.bearer = bearer;
                let bearer_configured = self.bearer.is_some();
                if self.fault_mode == FaultMode::WrongConfigure {
                    Response::Ok(json!({ "bearer_configured": !bearer_configured }))
                } else {
                    Response::Ok(json!({ "bearer_configured": bearer_configured }))
                }
            }
            Command::Publish {
                topic,
                value_b64,
                headers,
            } => self.publish(topic, value_b64, headers),
            Command::PublishEvent { topic, event } => {
                if event.id.is_empty() {
                    return Response::Error(AdapterError::with_message(
                        ErrorKind::InvalidArgument,
                        "CloudEvent id is required",
                    ));
                }
                let mut headers = vec![
                    header("ce_id", event.id),
                    header("ce_source", event.source),
                    header("ce_type", event.type_),
                    header("ce_specversion", event.specversion),
                ];
                if let Some(content_type) = event.datacontenttype {
                    headers.push(header("content-type", content_type));
                }
                self.publish(topic, event.data_b64, headers)
            }
            Command::Subscribe {
                topics,
                group: _,
                filter,
            } => {
                if let Some(error) = filter.as_ref().and_then(validate_filter) {
                    return Response::Error(error);
                }
                self.subscription = Some(Subscription {
                    topics,
                    filter,
                    next_index: 0,
                });
                Response::Ok(json!({}))
            }
            Command::NextMessage { timeout_ms: _ } => self.next_message(),
            Command::QueueAcquire {
                topic,
                group,
                max,
                lock_duration_ms,
                session_id,
            } => self.queue_acquire(&topic, &group, max, lock_duration_ms, &session_id),
            Command::QueueAck { .. } => Self::unimplemented_queues(),
            Command::QueueAcknowledge {
                session_id,
                entries,
            } => self.queue_acknowledge(&session_id, &entries),
            Command::QueueRenew {
                session_id,
                entries,
            } => self.queue_renew(&session_id, &entries),
            Command::DbConnect { .. } => Response::Error(AdapterError::unimplemented(
                "database",
                "chapter-f-control-plane",
            )),
            Command::AuthSignIn { .. } => Response::Error(AdapterError::with_message(
                ErrorKind::Unauthenticated,
                "identity APIs are not part of contract v1",
            )),
            Command::BlobPut { .. } | Command::BlobGet { .. } => {
                Response::Error(AdapterError::unimplemented("blob", "chapter-b-blob-api"))
            }
        }
    }

    fn publish(&mut self, topic: String, value_b64: String, headers: Vec<Header>) -> Response {
        if self.fault_mode == FaultMode::WrongPublish {
            return Response::Ok(json!({ "partition": 9, "offset": 999, "deduplicated": true }));
        }
        if self.endpoint.starts_with("unreachable://") {
            return Response::Error(AdapterError::with_message(
                ErrorKind::Transport,
                "endpoint unreachable",
            ));
        }
        if topic.is_empty() {
            return Response::Error(AdapterError::with_message(
                ErrorKind::InvalidArgument,
                "topic is required",
            ));
        }
        if topic == "__missing_topic" {
            return Response::Error(AdapterError::with_message(
                ErrorKind::NotFound,
                "topic not found",
            ));
        }
        let offset = self
            .messages
            .iter()
            .filter(|message| message.topic == topic)
            .count()
            .try_into()
            .expect("message count fits i64");
        self.messages.push(StoredMessage {
            topic,
            partition: 0,
            offset,
            value_b64,
            headers,
            queue_states: HashMap::new(),
        });
        Response::Ok(json!({ "partition": 0, "offset": offset, "deduplicated": false }))
    }

    fn next_message(&mut self) -> Response {
        let Some(subscription) = self.subscription.as_mut() else {
            return Response::Error(AdapterError::with_message(
                ErrorKind::InvalidArgument,
                "subscribe before next_message",
            ));
        };
        while let Some(message) = self.messages.get(subscription.next_index) {
            subscription.next_index += 1;
            if !subscription
                .topics
                .iter()
                .any(|topic| topic == &message.topic)
            {
                continue;
            }
            if !filter_matches(subscription.filter.as_ref(), message) {
                continue;
            }
            return Response::Message(Message {
                topic: message.topic.clone(),
                partition: message.partition,
                offset: message.offset,
                value_b64: message.value_b64.clone(),
                headers: message.headers.clone(),
            });
        }
        Response::Error(AdapterError::with_message(
            ErrorKind::NotFound,
            "no message available",
        ))
    }

    fn queue_acquire(
        &mut self,
        topic: &str,
        group: &str,
        max: u32,
        lock_duration_ms: u64,
        requested_session_id: &str,
    ) -> Response {
        if self.contract_minor < CONTRACT_MINOR_QUEUE_RPC {
            return Self::unimplemented_queues();
        }
        if group.is_empty() {
            return Response::Error(AdapterError::with_message(
                ErrorKind::InvalidArgument,
                "queue group is required",
            ));
        }
        if topic.is_empty() {
            return Response::Error(AdapterError::with_message(
                ErrorKind::InvalidArgument,
                "queue topic is required",
            ));
        }
        if lock_duration_ms != 30_000 {
            return Response::Error(AdapterError::with_message(
                ErrorKind::InvalidArgument,
                "queue lock_duration_ms must be 30000; per-acquire lock durations are not supported",
            ));
        }

        let effective_max = max.clamp(1, 500);
        let session_id = if requested_session_id.is_empty() {
            let session_id = self.next_queue_session_id();
            self.queue_sessions.insert(
                session_id.clone(),
                QueueSession {
                    topic: topic.to_string(),
                    group: group.to_string(),
                    max_messages: effective_max,
                },
            );
            session_id
        } else {
            let Some(session) = self.queue_sessions.get(requested_session_id) else {
                return Response::Error(AdapterError::with_message(
                    ErrorKind::InvalidArgument,
                    "queue session expired; re-acquire",
                ));
            };
            if session.topic != topic || session.group != group {
                return Response::Error(AdapterError::with_message(
                    ErrorKind::InvalidArgument,
                    "group_id and topics are fixed when a queue session is created",
                ));
            }
            if max != 0 && effective_max != session.max_messages {
                return Response::Error(AdapterError::with_message(
                    ErrorKind::InvalidArgument,
                    "max_messages is fixed when a queue session is created",
                ));
            }
            requested_session_id.to_string()
        };
        let max = usize::try_from(effective_max).expect("u32 fits usize");
        let messages =
            self.messages
                .iter_mut()
                .filter(|message| {
                    message.topic == topic
                        && message
                            .queue_states
                            .get(group)
                            .is_none_or(|state| state.state == QueueMessageState::Available)
                })
                .take(max)
                .map(|message| {
                    let message_id = queue_message_id(message);
                    let state = message.queue_states.entry(group.to_string()).or_insert(
                        QueueDeliveryState {
                            state: QueueMessageState::Available,
                            session_id: None,
                            delivery_count: 0,
                        },
                    );
                    state.state = QueueMessageState::Acquired;
                    state.session_id = Some(session_id.clone());
                    state.delivery_count = state.delivery_count.saturating_add(1);
                    json!({
                        "message_id": message_id,
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "value_b64": message.value_b64,
                        "headers": message.headers,
                        "delivery_count": state.delivery_count,
                    })
                })
                .collect::<Vec<_>>();

        Response::Ok(json!({ "session_id": session_id, "messages": messages }))
    }

    fn queue_acknowledge(&mut self, session_id: &str, entries: &[QueueAckEntry]) -> Response {
        if self.contract_minor < CONTRACT_MINOR_QUEUE_RPC {
            return Self::unimplemented_queues();
        }
        if session_id.is_empty() {
            return Response::Error(AdapterError::with_message(
                ErrorKind::InvalidArgument,
                "queue session_id is required",
            ));
        }
        if !self.queue_sessions.contains_key(session_id) {
            return Response::Error(AdapterError::with_message(
                ErrorKind::InvalidArgument,
                "queue session expired; re-acquire",
            ));
        }

        let results = entries
            .iter()
            .map(|entry| self.queue_ack_result(session_id, entry))
            .collect::<Vec<_>>();
        Response::Ok(json!({ "results": results }))
    }

    fn queue_renew(&mut self, session_id: &str, entries: &[QueueRenewEntry]) -> Response {
        if self.contract_minor < CONTRACT_MINOR_QUEUE_RPC {
            return Self::unimplemented_queues();
        }
        if session_id.is_empty() {
            return Response::Error(AdapterError::with_message(
                ErrorKind::InvalidArgument,
                "queue session_id is required",
            ));
        }
        if !self.queue_sessions.contains_key(session_id) {
            return Response::Error(AdapterError::with_message(
                ErrorKind::InvalidArgument,
                "queue session expired; re-acquire",
            ));
        }

        let results = entries
            .iter()
            .map(|entry| self.queue_renew_result(session_id, entry))
            .collect::<Vec<_>>();
        Response::Ok(json!({ "results": results }))
    }

    fn next_queue_session_id(&mut self) -> String {
        let session_id = format!("queue-session-{}", self.next_session_id);
        self.next_session_id = self.next_session_id.saturating_add(1);
        session_id
    }

    fn queue_ack_result(&mut self, session_id: &str, entry: &QueueAckEntry) -> serde_json::Value {
        let group = self
            .queue_sessions
            .get(session_id)
            .expect("queue session was validated")
            .group
            .clone();
        let Some(state) = self.queue_state_mut(&group, session_id, &entry.message_id) else {
            return json!({
                "message_id": entry.message_id,
                "error": { "kind": "invalid_argument", "message": "queue message is not acquired" },
            });
        };
        state.state = match entry.ack_type {
            QueueAckType::Accept => QueueMessageState::Accepted,
            QueueAckType::Release => QueueMessageState::Available,
            QueueAckType::Reject => QueueMessageState::Rejected,
        };
        state.session_id = None;
        json!({ "message_id": entry.message_id, "error": null })
    }

    fn queue_renew_result(
        &mut self,
        session_id: &str,
        entry: &QueueRenewEntry,
    ) -> serde_json::Value {
        let group = self
            .queue_sessions
            .get(session_id)
            .expect("queue session was validated")
            .group
            .clone();
        if self
            .queue_state_mut(&group, session_id, &entry.message_id)
            .is_some()
        {
            return json!({ "message_id": entry.message_id, "error": null });
        }

        json!({
            "message_id": entry.message_id,
            "error": { "kind": "invalid_argument", "message": "queue message is not acquired" },
        })
    }

    fn queue_state_mut(
        &mut self,
        group: &str,
        session_id: &str,
        message_id: &str,
    ) -> Option<&mut QueueDeliveryState> {
        self.messages
            .iter_mut()
            .find(|message| queue_message_id(message) == message_id)
            .and_then(|message| message.queue_states.get_mut(group))
            .filter(|state| {
                state.state == QueueMessageState::Acquired
                    && state.session_id.as_deref() == Some(session_id)
            })
    }

    fn unimplemented_queues() -> Response {
        Response::Error(AdapterError::unimplemented(
            "queues",
            "gateway-sharegroup-rpc",
        ))
    }
}

fn validate_filter(filter: &Filter) -> Option<AdapterError> {
    if filter.op != FilterOp::Equals {
        return Some(AdapterError::with_message(
            ErrorKind::InvalidArgument,
            "only equals filters are supported",
        ));
    }
    let Some(field) = filter.path.strip_prefix("$.") else {
        return Some(AdapterError::with_message(
            ErrorKind::InvalidArgument,
            "filter path must start with $.",
        ));
    };
    if field.is_empty()
        || !field.split('.').all(|segment| {
            let mut characters = segment.chars();
            characters.next().is_some_and(|first| {
                (first == '_' || first.is_ascii_alphabetic())
                    && characters
                        .all(|character| character == '_' || character.is_ascii_alphanumeric())
            })
        })
    {
        return Some(AdapterError::with_message(
            ErrorKind::InvalidArgument,
            "filter path must contain identifier segments",
        ));
    }
    if filter.value.is_array() || filter.value.is_object() {
        return Some(AdapterError::with_message(
            ErrorKind::InvalidArgument,
            "filter value must be a string, number, boolean, or null",
        ));
    }
    None
}

fn queue_message_id(message: &StoredMessage) -> String {
    format!("{}:{}:{}", message.topic, message.partition, message.offset)
}

fn header(name: &str, value: String) -> Header {
    Header {
        name: name.into(),
        value_b64: Some(STANDARD.encode(value)),
    }
}

fn filter_matches(filter: Option<&Filter>, message: &StoredMessage) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    if filter.op != FilterOp::Equals {
        return false;
    }
    let Ok(value_bytes) = STANDARD.decode(&message.value_b64) else {
        return false;
    };
    let Ok(json) = serde_json::from_slice::<serde_json::Value>(&value_bytes) else {
        return false;
    };
    let Some(field) = filter.path.strip_prefix("$.") else {
        return false;
    };
    json.get(field) == Some(&filter.value)
}

/// Run a mock adapter over JSON lines.
///
/// # Errors
///
/// Returns an error when JSON-lines input/output fails or a command cannot be
/// decoded.
pub async fn run_stdio<R, W>(
    mut input: R,
    mut output: W,
    fault_mode: FaultMode,
    contract_minor: u16,
) -> Result<(), MockAdapterError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut adapter = MockAdapter::with_contract_minor(contract_minor, fault_mode);
    let mut line = String::new();
    loop {
        line.clear();
        if input.read_line(&mut line).await? == 0 {
            break;
        }
        let command = serde_json::from_str::<Command>(line.trim_end())?;
        let response = adapter.handle(command);
        let encoded = serde_json::to_string(&response)?;
        output.write_all(encoded.as_bytes()).await?;
        output.write_all(b"\n").await?;
        output.flush().await?;
    }
    Ok(())
}

/// Errors returned by the mock adapter runner.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MockAdapterError {
    /// I/O failed.
    #[error("mock adapter io: {0}")]
    Io(#[from] std::io::Error),
    /// JSON failed to decode or encode.
    #[error("mock adapter json: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_adapter_answers_hello_and_publish() {
        let mut adapter = MockAdapter::new();
        let hello = adapter.handle(Command::Hello);
        assert_eq!(
            hello,
            Response::Hello {
                contract_major: CONTRACT_MAJOR,
                contract_minor: CONTRACT_MINOR_V1_0,
                language: "mock".into()
            }
        );

        let published = adapter.handle(Command::Publish {
            topic: "t".into(),
            value_b64: "aGk=".into(),
            headers: vec![],
        });
        assert2::assert!(let Response::Ok(value) = published);
        assert_eq!(value["offset"], 0);
    }
}
