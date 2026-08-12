//! Queue client plus gated stubs for modules not implemented in v1.

use bytes::Bytes;

use crate::{
    client::{
        CrabkaClient, MockMessage, MockQueueDelivery, MockQueueDisposition, MockQueueMessageState,
        MockQueueSession,
    },
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
    ///
    /// # Errors
    ///
    /// Returns an error for invalid queue options, transport failures, or a
    /// gateway request error.
    pub async fn acquire(
        &self,
        topic: impl Into<String>,
        group: impl Into<String>,
        options: AcquireOptions,
    ) -> Result<QueueAcquireResult, CrabkaError> {
        self.acquire_with_session(topic, group, options, "").await
    }

    /// Acquire queue messages, optionally reusing a gateway session.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid queue options, transport failures, or a
    /// gateway request error.
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
            return self.acquire_mock(&topic, &group, options, &session_id);
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
    ///
    /// # Errors
    ///
    /// Returns an error when the session is invalid or the gateway request
    /// fails. Per-entry failures are returned in the result vector.
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
    ///
    /// # Errors
    ///
    /// Returns an error when the session is invalid or the gateway request
    /// fails. Per-entry failures are returned in the result vector.
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
        group: &str,
        options: AcquireOptions,
        requested_session_id: &str,
    ) -> Result<QueueAcquireResult, CrabkaError> {
        let mut state = self
            .client
            .inner
            .mock
            .lock()
            .expect("mock state lock not poisoned");
        let effective_max = options.max.clamp(1, 500);
        let session_id = mock_session_id(
            &mut state.queue_sessions,
            requested_session_id,
            topic,
            group,
            options.max,
            effective_max,
        )?;
        let delivered = state
            .messages
            .iter()
            .filter(|message| message.topic == topic)
            .filter(|message| mock_message_is_available(&state.queue_states, group, message))
            .take(usize::try_from(effective_max).unwrap_or(usize::MAX))
            .cloned()
            .collect::<Vec<_>>();

        let messages = delivered
            .into_iter()
            .map(|message| {
                let delivery = MockQueueDelivery::from(&message);
                let queue_state = mock_queue_state_mut(&mut state.queue_states, group, delivery);
                queue_state.disposition = MockQueueDisposition::Acquired(session_id.clone());
                queue_state.delivery_count = queue_state.delivery_count.saturating_add(1);
                queue_message_from_mock(message, queue_state.delivery_count)
            })
            .collect();

        Ok(QueueAcquireResult {
            session_id,
            messages,
        })
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
        let Some(session) = state
            .queue_sessions
            .iter()
            .find(|session| session.id == session_id)
            .cloned()
        else {
            return Err(CrabkaError::InvalidArgument(
                "queue session expired; re-acquire".into(),
            ));
        };

        let mut results = Vec::with_capacity(entries.len());
        for entry in entries {
            let delivery = MockQueueDelivery::from(&entry);
            let Some(queue_state) = state.queue_states.iter_mut().find(|queue_state| {
                queue_state.group == session.group
                    && queue_state.delivery == delivery
                    && matches!(
                        &queue_state.disposition,
                        MockQueueDisposition::Acquired(owner) if owner == session_id
                    )
            }) else {
                results.push(queue_result_error(
                    entry.into(),
                    "queue message is not acquired",
                ));
                continue;
            };

            queue_state.disposition = match entry.ack_type {
                QueueAckType::Accept => MockQueueDisposition::Accepted,
                QueueAckType::Release => MockQueueDisposition::Available,
                QueueAckType::Reject => MockQueueDisposition::Rejected,
            };
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
        let Some(session) = state
            .queue_sessions
            .iter()
            .find(|session| session.id == session_id)
        else {
            return Err(CrabkaError::InvalidArgument(
                "queue session expired; re-acquire".into(),
            ));
        };

        Ok(entries
            .into_iter()
            .map(|entry| {
                let delivery = MockQueueDelivery::from(&entry);
                if state.queue_states.iter().any(|queue_state| {
                    queue_state.group == session.group
                        && queue_state.delivery == delivery
                        && matches!(
                            &queue_state.disposition,
                            MockQueueDisposition::Acquired(owner) if owner == session_id
                        )
                }) {
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

impl From<&MockMessage> for MockQueueDelivery {
    fn from(value: &MockMessage) -> Self {
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
    let message = error.message;
    if error.retriable {
        return CrabkaError::Transport(message);
    }
    match error.code {
        3 | 9 => CrabkaError::InvalidArgument(message),
        16 => CrabkaError::Unauthenticated(message),
        5 => CrabkaError::NotFound(message),
        _ => CrabkaError::ServerError(message),
    }
}

fn mock_session_id(
    sessions: &mut Vec<MockQueueSession>,
    requested_session_id: &str,
    topic: &str,
    group: &str,
    requested_max: u32,
    effective_max: u32,
) -> Result<String, CrabkaError> {
    if !requested_session_id.is_empty() {
        let Some(session) = sessions
            .iter()
            .find(|session| session.id == requested_session_id)
        else {
            return Err(CrabkaError::InvalidArgument(
                "queue session expired; re-acquire".into(),
            ));
        };
        if session.topic != topic || session.group != group {
            return Err(CrabkaError::InvalidArgument(
                "group_id and topics are fixed when a queue session is created".into(),
            ));
        }
        if requested_max != 0 && effective_max != session.max_messages {
            return Err(CrabkaError::InvalidArgument(
                "max_messages is fixed when a queue session is created".into(),
            ));
        }
        return Ok(requested_session_id.to_string());
    }

    let session_id = format!("mock-queue-session-{}", sessions.len() + 1);
    sessions.push(MockQueueSession {
        id: session_id.clone(),
        topic: topic.to_string(),
        group: group.to_string(),
        max_messages: effective_max,
    });
    Ok(session_id)
}

fn mock_message_is_available(
    queue_states: &[MockQueueMessageState],
    group: &str,
    message: &MockMessage,
) -> bool {
    let delivery = MockQueueDelivery::from(message);
    queue_states
        .iter()
        .find(|queue_state| queue_state.group == group && queue_state.delivery == delivery)
        .is_none_or(|queue_state| queue_state.disposition == MockQueueDisposition::Available)
}

fn mock_queue_state_mut<'a>(
    queue_states: &'a mut Vec<MockQueueMessageState>,
    group: &str,
    delivery: MockQueueDelivery,
) -> &'a mut MockQueueMessageState {
    if let Some(index) = queue_states
        .iter()
        .position(|queue_state| queue_state.group == group && queue_state.delivery == delivery)
    {
        return &mut queue_states[index];
    }
    queue_states.push(MockQueueMessageState {
        group: group.to_string(),
        delivery,
        disposition: MockQueueDisposition::Available,
        delivery_count: 0,
    });
    queue_states
        .last_mut()
        .expect("queue state was just inserted")
}

fn queue_message_from_mock(message: MockMessage, delivery_count: i32) -> QueueMessage {
    QueueMessage {
        topic: message.topic,
        partition: message.partition,
        offset: message.offset,
        value: message.value,
        headers: message.headers,
        delivery_count,
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
    ///
    /// # Errors
    ///
    /// Always returns [`CrabkaError::Unimplemented`] while the database module
    /// remains gated.
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
    ///
    /// # Errors
    ///
    /// Always returns [`CrabkaError::Unauthenticated`] because sign-in is not
    /// part of contract v1.
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
    ///
    /// # Errors
    ///
    /// Always returns [`CrabkaError::Unimplemented`] while the blob module
    /// remains gated.
    pub fn put(&self, _key: &str, _value: Bytes) -> Result<(), CrabkaError> {
        Err(CrabkaError::Unimplemented {
            module: "blob",
            gated_on: "chapter-b-blob-api",
        })
    }

    /// Get a blob.
    ///
    /// # Errors
    ///
    /// Always returns [`CrabkaError::Unimplemented`] while the blob module
    /// remains gated.
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
    async fn mock_queue_sessions_own_delivered_coordinates() {
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
        let first = client
            .queues()
            .acquire("work", "workers", AcquireOptions::default())
            .await
            .expect("first session");
        let second = client
            .queues()
            .acquire("work", "workers", AcquireOptions::default())
            .await
            .expect("second session");
        let entry = QueueAckEntry {
            topic: "work".into(),
            partition: 0,
            offset: 0,
            ack_type: QueueAckType::Accept,
        };

        let wrong_session = client
            .queues()
            .acknowledge(&second.session_id, vec![entry.clone()])
            .await
            .expect("wrong-session acknowledgement returns per-entry result");
        assert2::assert!(
            wrong_session
                == vec![queue_result_error(
                    QueueRenewEntry::from(entry.clone()),
                    "queue message is not acquired"
                )]
        );
        let renewed = client
            .queues()
            .renew(
                &second.session_id,
                vec![QueueRenewEntry::from(entry.clone())],
            )
            .await
            .expect("wrong-session renew returns per-entry result");
        assert2::assert!(
            renewed
                == vec![queue_result_error(
                    QueueRenewEntry::from(entry),
                    "queue message is not acquired"
                )]
        );

        let missing = client
            .queues()
            .acquire_with_session(
                "work",
                "workers",
                AcquireOptions::default(),
                "missing-session",
            )
            .await;
        assert2::assert!(
            missing
                == Err(CrabkaError::InvalidArgument(
                    "queue session expired; re-acquire".into()
                ))
        );
        let changed = client
            .queues()
            .acquire_with_session(
                "work",
                "other-workers",
                AcquireOptions::default(),
                first.session_id,
            )
            .await;
        assert2::assert!(
            changed
                == Err(CrabkaError::InvalidArgument(
                    "group_id and topics are fixed when a queue session is created".into()
                ))
        );
    }

    #[tokio::test]
    async fn mock_queue_delivery_state_is_independent_per_group() {
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

        let first = client
            .queues()
            .acquire("work", "workers-a", AcquireOptions::default())
            .await
            .expect("group A acquire");
        let second = client
            .queues()
            .acquire("work", "workers-b", AcquireOptions::default())
            .await
            .expect("group B acquire");

        assert2::assert!(first.messages.len() == 1);
        assert2::assert!(second.messages.len() == 1);
        assert2::assert!(first.messages[0].delivery_count == 1);
        assert2::assert!(second.messages[0].delivery_count == 1);

        let coordinate = QueueAckEntry {
            topic: "work".into(),
            partition: 0,
            offset: 0,
            ack_type: QueueAckType::Accept,
        };
        let accepted = client
            .queues()
            .acknowledge(&second.session_id, vec![coordinate.clone()])
            .await
            .expect("group B acknowledge");
        assert2::assert!(accepted[0].error.is_none());

        let renewed = client
            .queues()
            .renew(
                &first.session_id,
                vec![QueueRenewEntry::from(coordinate.clone())],
            )
            .await
            .expect("group A renew");
        assert2::assert!(renewed[0].error.is_none());

        let accepted = client
            .queues()
            .acknowledge(&first.session_id, vec![coordinate])
            .await
            .expect("group A acknowledge");
        assert2::assert!(accepted[0].error.is_none());
    }

    #[test]
    fn retriable_queue_error_overrides_gateway_code_and_preserves_message() {
        assert2::assert!(
            error_info_to_crabka(pb::ErrorInfo {
                code: 9,
                message: "coordinator retry".into(),
                retriable: true,
            }) == CrabkaError::Transport("coordinator retry".into())
        );
        assert2::assert!(
            error_info_to_crabka(pb::ErrorInfo {
                code: 9,
                message: "record is not acquired by this session".into(),
                retriable: false,
            }) == CrabkaError::InvalidArgument("record is not acquired by this session".into())
        );
    }

    #[tokio::test]
    async fn stub_modules_return_gated_unimplemented() {
        let client = CrabkaClient::new("mock://gateway").unwrap();
        assert_eq!(
            client.database().connect("orders"),
            Err(CrabkaError::Unimplemented {
                module: "database",
                gated_on: "chapter-f-control-plane"
            })
        );
        assert_eq!(
            client.blob().put("k", Bytes::from_static(b"v")),
            Err(CrabkaError::Unimplemented {
                module: "blob",
                gated_on: "chapter-b-blob-api"
            })
        );
    }
}
