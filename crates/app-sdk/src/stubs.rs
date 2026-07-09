//! Queue client plus gated stubs for modules not implemented in v1.

use bytes::Bytes;

use crate::{
    client::{CrabkaClient, MockMessage, MockQueueDelivery, MockQueueSession},
    error::CrabkaError,
    pb,
};

const SUPPORTED_QUEUE_LOCK_DURATION_MS: u64 = 30_000;

/// Queues module client.
#[derive(Debug, Clone)]
pub struct QueuesClient {
    client: CrabkaClient,
}

impl QueuesClient {
    pub(crate) fn new(client: CrabkaClient) -> Self {
        Self { client }
    }

    /// Acquire queue messages.
    pub async fn acquire(
        &self,
        topic: impl Into<String>,
        group: impl Into<String>,
        options: AcquireOptions,
    ) -> Result<QueueAcquireResult, CrabkaError> {
        self.acquire_with_session(topic, group, options, "").await
    }

    /// Acquire queue messages, optionally reusing a gateway session.
    pub async fn acquire_with_session(
        &self,
        topic: impl Into<String>,
        group: impl Into<String>,
        options: AcquireOptions,
        session_id: impl Into<String>,
    ) -> Result<QueueAcquireResult, CrabkaError> {
        let topic = topic.into();
        let group = group.into();
        if group.is_empty() {
            return Err(CrabkaError::InvalidArgument(
                "queue group is required".into(),
            ));
        }
        if topic.is_empty() {
            return Err(CrabkaError::InvalidArgument(
                "queue topic is required".into(),
            ));
        }
        if options.lock_duration_ms != SUPPORTED_QUEUE_LOCK_DURATION_MS {
            return Err(CrabkaError::InvalidArgument(format!(
                "queue lock_duration_ms must be {SUPPORTED_QUEUE_LOCK_DURATION_MS}; per-acquire lock durations are not supported"
            )));
        }
        let session_id = session_id.into();

        if self.client.is_mock() {
            return Ok(self.acquire_mock(&topic, options, &session_id));
        }
        if self.client.is_unreachable() {
            return Err(CrabkaError::Transport("endpoint unreachable".into()));
        }

        let response: pb::QueueAcquireResponse = self
            .client
            .inner
            .connect
            .unary(
                "/crabka.gateway.v1.Gateway/QueueAcquire",
                &pb::QueueAcquireRequest {
                    group_id: group,
                    topics: vec![topic],
                    max_messages: options.max,
                    wait_ms: options.wait_ms,
                    session_id,
                    lock_duration_ms: options.lock_duration_ms,
                },
            )
            .await?;
        Ok(QueueAcquireResult {
            session_id: response.session_id,
            messages: response
                .messages
                .into_iter()
                .map(QueueMessage::from)
                .collect(),
        })
    }

    /// Acknowledge queue messages.
    pub async fn acknowledge(
        &self,
        session_id: impl Into<String>,
        entries: Vec<QueueAckEntry>,
    ) -> Result<Vec<QueueAckResult>, CrabkaError> {
        let session_id = session_id.into();
        if self.client.is_mock() {
            return self.acknowledge_mock(&session_id, entries);
        }
        if self.client.is_unreachable() {
            return Err(CrabkaError::Transport("endpoint unreachable".into()));
        }

        let response: pb::QueueAcknowledgeResponse = self
            .client
            .inner
            .connect
            .unary(
                "/crabka.gateway.v1.Gateway/QueueAcknowledge",
                &pb::QueueAcknowledgeRequest {
                    session_id,
                    entries: entries.into_iter().map(pb::QueueAckEntry::from).collect(),
                },
            )
            .await?;
        Ok(response
            .results
            .into_iter()
            .map(QueueAckResult::from)
            .collect())
    }

    /// Renew queue message locks.
    pub async fn renew(
        &self,
        session_id: impl Into<String>,
        entries: Vec<QueueRenewEntry>,
    ) -> Result<Vec<QueueAckResult>, CrabkaError> {
        let session_id = session_id.into();
        if self.client.is_mock() {
            return self.renew_mock(&session_id, entries);
        }
        if self.client.is_unreachable() {
            return Err(CrabkaError::Transport("endpoint unreachable".into()));
        }

        let response: pb::QueueRenewResponse = self
            .client
            .inner
            .connect
            .unary(
                "/crabka.gateway.v1.Gateway/QueueRenew",
                &pb::QueueRenewRequest {
                    session_id,
                    entries: entries.into_iter().map(pb::QueueAckEntry::from).collect(),
                },
            )
            .await?;
        Ok(response
            .results
            .into_iter()
            .map(QueueAckResult::from)
            .collect())
    }

    fn acquire_mock(
        &self,
        topic: &str,
        options: AcquireOptions,
        requested_session_id: &str,
    ) -> QueueAcquireResult {
        let mut state = self
            .client
            .inner
            .mock
            .lock()
            .expect("mock state lock not poisoned");
        let session_id = mock_session_id(&mut state.queue_sessions, requested_session_id);
        let delivered = state
            .messages
            .iter()
            .filter(|message| message.topic == topic)
            .filter(|message| !mock_message_is_acquired(&state.queue_sessions, message))
            .take(usize::try_from(options.max).unwrap_or(usize::MAX))
            .cloned()
            .collect::<Vec<_>>();

        if let Some(session) = state
            .queue_sessions
            .iter_mut()
            .find(|session| session.id == session_id)
        {
            session
                .delivered
                .extend(delivered.iter().map(|message| MockQueueDelivery {
                    topic: message.topic.clone(),
                    partition: message.partition,
                    offset: message.offset,
                }));
        }

        QueueAcquireResult {
            session_id,
            messages: delivered.into_iter().map(queue_message_from_mock).collect(),
        }
    }

    fn acknowledge_mock(
        &self,
        session_id: &str,
        entries: Vec<QueueAckEntry>,
    ) -> Result<Vec<QueueAckResult>, CrabkaError> {
        let mut state = self
            .client
            .inner
            .mock
            .lock()
            .expect("mock state lock not poisoned");
        let Some(session_index) = mock_session_index(&state.queue_sessions, session_id) else {
            return Err(CrabkaError::InvalidArgument(
                "queue session expired; re-acquire".into(),
            ));
        };

        let mut results = Vec::with_capacity(entries.len());
        for entry in entries {
            let delivery = MockQueueDelivery::from(&entry);
            let is_acquired = state.queue_sessions[session_index]
                .delivered
                .contains(&delivery);
            if !is_acquired {
                results.push(queue_result_error(
                    entry.into(),
                    "queue message is not acquired",
                ));
                continue;
            }

            if entry.ack_type == QueueAckType::Accept || entry.ack_type == QueueAckType::Reject {
                state
                    .messages
                    .retain(|message| !mock_message_matches(message, &delivery));
            }
            state.queue_sessions[session_index]
                .delivered
                .retain(|stored| stored != &delivery);
            results.push(QueueAckResult {
                entry: entry.into(),
                error: None,
            });
        }
        Ok(results)
    }

    fn renew_mock(
        &self,
        session_id: &str,
        entries: Vec<QueueRenewEntry>,
    ) -> Result<Vec<QueueAckResult>, CrabkaError> {
        let state = self
            .client
            .inner
            .mock
            .lock()
            .expect("mock state lock not poisoned");
        let Some(session_index) = mock_session_index(&state.queue_sessions, session_id) else {
            return Err(CrabkaError::InvalidArgument(
                "queue session expired; re-acquire".into(),
            ));
        };

        Ok(entries
            .into_iter()
            .map(|entry| {
                let delivery = MockQueueDelivery::from(&entry);
                if state.queue_sessions[session_index]
                    .delivered
                    .contains(&delivery)
                {
                    return QueueAckResult { entry, error: None };
                }
                queue_result_error(entry, "queue message is not acquired")
            })
            .collect())
    }
}

/// Queue acquire options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcquireOptions {
    /// Maximum messages to acquire.
    pub max: u32,
    /// Lock duration in milliseconds.
    pub lock_duration_ms: u64,
    /// Maximum server wait time in milliseconds.
    pub wait_ms: u32,
}

impl Default for AcquireOptions {
    fn default() -> Self {
        Self {
            max: 1,
            lock_duration_ms: 30_000,
            wait_ms: 1_000,
        }
    }
}

/// Queue acquire result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueAcquireResult {
    /// Gateway session id.
    pub session_id: String,
    /// Acquired messages.
    pub messages: Vec<QueueMessage>,
}

/// Acquired queue message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueMessage {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Record offset.
    pub offset: i64,
    /// Record value.
    pub value: Bytes,
    /// Headers in delivery order.
    pub headers: Vec<(String, Option<Bytes>)>,
    /// Gateway delivery count.
    pub delivery_count: i32,
}

impl QueueMessage {
    /// Stable conformance message id.
    #[must_use]
    pub fn message_id(&self) -> String {
        format!("{}:{}:{}", self.topic, self.partition, self.offset)
    }
}

impl From<pb::QueuedMessage> for QueueMessage {
    fn from(value: pb::QueuedMessage) -> Self {
        Self {
            topic: value.topic,
            partition: value.partition,
            offset: value.offset,
            value: Bytes::from(value.value),
            headers: value
                .headers
                .into_iter()
                .map(|header| (header.key, header.value.map(Bytes::from)))
                .collect(),
            delivery_count: value.delivery_count,
        }
    }
}

/// Queue acknowledgement type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueAckType {
    /// Accept and remove the message.
    Accept,
    /// Release the message for redelivery.
    Release,
    /// Reject the message.
    Reject,
}

/// Queue acknowledgement entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueAckEntry {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Record offset.
    pub offset: i64,
    /// Ack verdict.
    pub ack_type: QueueAckType,
}

impl From<QueueAckEntry> for pb::QueueAckEntry {
    fn from(value: QueueAckEntry) -> Self {
        Self {
            topic: value.topic,
            partition: value.partition,
            offset: value.offset,
            r#type: match value.ack_type {
                QueueAckType::Accept => pb::QueueAckType::Accept,
                QueueAckType::Release => pb::QueueAckType::Release,
                QueueAckType::Reject => pb::QueueAckType::Reject,
            } as i32,
        }
    }
}

/// Queue renew entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueRenewEntry {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Record offset.
    pub offset: i64,
}

impl From<QueueRenewEntry> for pb::QueueAckEntry {
    fn from(value: QueueRenewEntry) -> Self {
        Self {
            topic: value.topic,
            partition: value.partition,
            offset: value.offset,
            r#type: pb::QueueAckType::Unspecified as i32,
        }
    }
}

impl From<&QueueAckEntry> for MockQueueDelivery {
    fn from(value: &QueueAckEntry) -> Self {
        Self {
            topic: value.topic.clone(),
            partition: value.partition,
            offset: value.offset,
        }
    }
}

impl From<&QueueRenewEntry> for MockQueueDelivery {
    fn from(value: &QueueRenewEntry) -> Self {
        Self {
            topic: value.topic.clone(),
            partition: value.partition,
            offset: value.offset,
        }
    }
}

impl From<QueueAckEntry> for QueueRenewEntry {
    fn from(value: QueueAckEntry) -> Self {
        Self {
            topic: value.topic,
            partition: value.partition,
            offset: value.offset,
        }
    }
}

/// Per-entry queue acknowledgement result.
#[derive(Debug, PartialEq, Eq)]
pub struct QueueAckResult {
    /// Original entry key.
    pub entry: QueueRenewEntry,
    /// Optional per-entry error.
    pub error: Option<CrabkaError>,
}

impl From<pb::QueueAckResult> for QueueAckResult {
    fn from(value: pb::QueueAckResult) -> Self {
        let entry = value.entry.unwrap_or_default();
        Self {
            entry: QueueRenewEntry {
                topic: entry.topic,
                partition: entry.partition,
                offset: entry.offset,
            },
            error: value.error.map(error_info_to_crabka),
        }
    }
}

fn error_info_to_crabka(error: pb::ErrorInfo) -> CrabkaError {
    let message = if error.message == "record is not acquired by this session" {
        "queue message is not acquired".into()
    } else {
        error.message
    };
    match error.code {
        3 | 9 => CrabkaError::InvalidArgument(message),
        16 => CrabkaError::Unauthenticated(message),
        5 => CrabkaError::NotFound(message),
        13 if error.retriable => CrabkaError::Transport(message),
        _ => CrabkaError::ServerError(message),
    }
}

fn mock_session_id(sessions: &mut Vec<MockQueueSession>, requested_session_id: &str) -> String {
    if !requested_session_id.is_empty() {
        return requested_session_id.to_string();
    }

    let session_id = format!("mock-queue-session-{}", sessions.len() + 1);
    sessions.push(MockQueueSession {
        id: session_id.clone(),
        delivered: vec![],
    });
    session_id
}

fn mock_session_index(sessions: &[MockQueueSession], session_id: &str) -> Option<usize> {
    sessions.iter().position(|session| session.id == session_id)
}

fn mock_message_is_acquired(sessions: &[MockQueueSession], message: &MockMessage) -> bool {
    let delivery = MockQueueDelivery {
        topic: message.topic.clone(),
        partition: message.partition,
        offset: message.offset,
    };
    sessions
        .iter()
        .any(|session| session.delivered.contains(&delivery))
}

fn mock_message_matches(message: &MockMessage, delivery: &MockQueueDelivery) -> bool {
    message.topic == delivery.topic
        && message.partition == delivery.partition
        && message.offset == delivery.offset
}

fn queue_message_from_mock(message: MockMessage) -> QueueMessage {
    QueueMessage {
        topic: message.topic,
        partition: message.partition,
        offset: message.offset,
        value: message.value,
        headers: message.headers,
        delivery_count: 1,
    }
}

fn queue_result_error(entry: QueueRenewEntry, message: impl Into<String>) -> QueueAckResult {
    QueueAckResult {
        entry,
        error: Some(CrabkaError::InvalidArgument(message.into())),
    }
}

/// Database module client.
#[derive(Debug, Clone, Copy, Default)]
pub struct DatabaseClient;

impl DatabaseClient {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Connect to a named database.
    pub fn connect(&self, _name: &str) -> Result<(), CrabkaError> {
        Err(CrabkaError::Unimplemented {
            module: "database",
            gated_on: "chapter-f-control-plane",
        })
    }
}

/// Auth module client.
#[derive(Debug, Clone, Default)]
pub struct AuthClient {
    bearer: Option<String>,
}

impl AuthClient {
    pub(crate) fn new(bearer: Option<String>) -> Self {
        Self { bearer }
    }

    /// Return the configured bearer token, if any.
    #[must_use]
    pub fn bearer_token(&self) -> Option<&str> {
        self.bearer.as_deref()
    }

    /// Sign-in is outside contract v1.
    pub fn sign_in(&self, _username: &str, _password: &str) -> Result<(), CrabkaError> {
        Err(CrabkaError::Unauthenticated(
            "identity APIs are not part of contract v1".into(),
        ))
    }
}

/// Blob module client.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlobClient;

impl BlobClient {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Put a blob.
    pub fn put(&self, _key: &str, _value: Bytes) -> Result<(), CrabkaError> {
        Err(CrabkaError::Unimplemented {
            module: "blob",
            gated_on: "chapter-b-blob-api",
        })
    }

    /// Get a blob.
    pub fn get(&self, _key: &str) -> Result<Bytes, CrabkaError> {
        Err(CrabkaError::Unimplemented {
            module: "blob",
            gated_on: "chapter-b-blob-api",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CrabkaClient;

    #[tokio::test]
    async fn mock_queues_acquire_published_records() {
        let client = CrabkaClient::new("mock://gateway").unwrap();
        client
            .messaging()
            .publish(
                "work",
                Bytes::from_static(b"job"),
                crate::PublishOptions::default(),
            )
            .await
            .expect("publish to mock");

        let acquired = client
            .queues()
            .acquire("work", "workers", AcquireOptions::default())
            .await
            .expect("acquire from mock");

        assert_eq!(acquired.session_id, "mock-queue-session-1");
        assert_eq!(acquired.messages.len(), 1);
        assert_eq!(acquired.messages[0].value, Bytes::from_static(b"job"));
    }

    #[tokio::test]
    async fn stub_modules_return_gated_unimplemented() {
        let client = CrabkaClient::new("mock://gateway").unwrap();
        assert!(
            client.database().connect("orders")
                == Err(CrabkaError::Unimplemented {
                    module: "database",
                    gated_on: "chapter-f-control-plane"
                })
        );
        assert!(
            client.blob().put("k", Bytes::from_static(b"v"))
                == Err(CrabkaError::Unimplemented {
                    module: "blob",
                    gated_on: "chapter-b-blob-api"
                })
        );
    }
}
