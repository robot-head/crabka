//! Queue RPC scaffolding and session table.
//!
//! The table is deliberately small and deterministic: callers provide the
//! current [`Instant`] in tests, while production helpers use [`Instant::now`].

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use axum::Extension;
use connectrpc_axum::message::{ConnectError, ConnectRequest, ConnectResponse};
use crabka_authz::AuthorizationResult;
use crabka_client_consumer::{ShareAckMode, ShareAckType, ShareConsumer, ShareConsumerRecord};
use crabka_metadata::{AclOperation, ResourceType};
use crabka_security::Principal;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    config::GatewayConfig,
    handlers::{anonymous_principal, authorize_resource, unknown_host},
    pb,
};

const SUPPORTED_QUEUE_LOCK_DURATION_MS: u64 = 30_000;

/// Queue session table configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueSessionConfig {
    /// How long a session may sit unused before expiry.
    pub idle_timeout: Duration,
    /// Maximum number of retained sessions.
    pub max_sessions: usize,
}

impl QueueSessionConfig {
    /// Build queue session settings from the process configuration.
    #[must_use]
    pub fn from_gateway_config(config: &GatewayConfig) -> Self {
        Self {
            idle_timeout: Duration::from_secs(config.queue_session_idle_secs),
            max_sessions: config.queue_max_sessions,
        }
    }
}

/// Queue-session failures surfaced by the unary queue RPCs.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum QueueError {
    /// The caller used a session owned by a different authenticated principal.
    #[error("queue session belongs to a different principal")]
    PermissionDenied,
    /// The session does not exist anymore; clients should acquire again.
    #[error("queue session expired; re-acquire")]
    SessionExpired,
    /// The gateway is at its configured queue-session cap.
    #[error("queue session limit exceeded")]
    ResourceExhausted,
}

impl From<QueueError> for ConnectError {
    fn from(error: QueueError) -> Self {
        let message = error.to_string();
        match error {
            QueueError::PermissionDenied => ConnectError::new_permission_denied(message),
            QueueError::SessionExpired => ConnectError::new_failed_precondition(message),
            QueueError::ResourceExhausted => ConnectError::new_resource_exhausted(message),
        }
    }
}

struct QueueSessionEntry<T> {
    principal: Principal,
    session: Arc<Mutex<T>>,
    last_used: StdMutex<std::time::Instant>,
}

impl<T> QueueSessionEntry<T> {
    fn new(principal: Principal, session: T, now: std::time::Instant) -> Self {
        Self {
            principal,
            session: Arc::new(Mutex::new(session)),
            last_used: StdMutex::new(now),
        }
    }

    fn is_idle_at(&self, now: std::time::Instant, idle_timeout: Duration) -> bool {
        let last_used = *self
            .last_used
            .lock()
            .expect("queue session last-used mutex poisoned");
        now.duration_since(last_used) >= idle_timeout
    }

    fn mark_used_at(&self, now: std::time::Instant) {
        *self
            .last_used
            .lock()
            .expect("queue session last-used mutex poisoned") = now;
    }
}

/// Principal-bound queue sessions retained by the gateway.
pub struct QueueSessionTable<T = QueueSession> {
    config: QueueSessionConfig,
    sessions: StdMutex<HashMap<String, Arc<QueueSessionEntry<T>>>>,
}

impl<T> QueueSessionTable<T> {
    /// Create an empty queue session table.
    #[must_use]
    pub fn new(config: QueueSessionConfig) -> Self {
        Self {
            config,
            sessions: StdMutex::new(HashMap::new()),
        }
    }

    fn sessions(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<QueueSessionEntry<T>>>> {
        self.sessions
            .lock()
            .expect("queue session table mutex poisoned")
    }

    /// Insert a session for `principal` and return its random 128-bit id.
    ///
    /// # Errors
    ///
    /// Returns [`QueueError::ResourceExhausted`] when the configured session cap
    /// is already reached.
    pub fn insert(&self, principal: Principal, session: T) -> Result<String, QueueError> {
        self.insert_at(principal, session, std::time::Instant::now())
    }

    /// Deterministic insertion helper for tests.
    ///
    /// # Errors
    ///
    /// Returns [`QueueError::ResourceExhausted`] when the configured session cap
    /// is already reached.
    pub fn insert_at(
        &self,
        principal: Principal,
        session: T,
        now: std::time::Instant,
    ) -> Result<String, QueueError> {
        let mut sessions = self.sessions();
        sessions.retain(|_, entry| !entry.is_idle_at(now, self.config.idle_timeout));
        if sessions.len() >= self.config.max_sessions {
            return Err(QueueError::ResourceExhausted);
        }

        let session_id = Uuid::new_v4().simple().to_string();
        let entry = QueueSessionEntry::new(principal, session, now);
        sessions.insert(session_id.clone(), Arc::new(entry));
        Ok(session_id)
    }

    /// Resolve a session for `principal`, refreshing its idle timestamp.
    ///
    /// # Errors
    ///
    /// Returns [`QueueError::SessionExpired`] when the id is unknown or idle, and
    /// [`QueueError::PermissionDenied`] when a different principal owns it.
    pub fn get(
        &self,
        principal: &Principal,
        session_id: &str,
    ) -> Result<Arc<Mutex<T>>, QueueError> {
        self.get_at(principal, session_id, std::time::Instant::now())
    }

    /// Deterministic lookup helper for tests.
    ///
    /// # Errors
    ///
    /// Returns [`QueueError::SessionExpired`] when the id is unknown or idle, and
    /// [`QueueError::PermissionDenied`] when a different principal owns it.
    pub fn get_at(
        &self,
        principal: &Principal,
        session_id: &str,
        now: std::time::Instant,
    ) -> Result<Arc<Mutex<T>>, QueueError> {
        let mut sessions = self.sessions();
        let Some(entry) = sessions.get(session_id).cloned() else {
            return Err(QueueError::SessionExpired);
        };

        if entry.is_idle_at(now, self.config.idle_timeout) {
            sessions.remove(session_id);
            return Err(QueueError::SessionExpired);
        }

        if entry.principal != *principal {
            return Err(QueueError::PermissionDenied);
        }

        entry.mark_used_at(now);
        drop(sessions);
        Ok(Arc::clone(&entry.session))
    }

    /// Evict idle sessions and return the number removed.
    #[must_use]
    pub fn evict_idle_at(&self, now: std::time::Instant) -> usize {
        let mut sessions = self.sessions();
        let before = sessions.len();
        sessions.retain(|_, entry| !entry.is_idle_at(now, self.config.idle_timeout));
        before.saturating_sub(sessions.len())
    }

    /// Return the number of retained sessions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.sessions().len()
    }

    /// Return true when no sessions are retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sessions().is_empty()
    }
}

pub struct QueueSession {
    consumer: Option<ShareConsumer>,
    pending_acquired: VecDeque<ShareConsumerRecord>,
    delivered: HashMap<QueueRecordKey, ShareConsumerRecord>,
    staged_acks: HashMap<QueueRecordKey, StagedQueueAck>,
}

impl QueueSession {
    async fn new(
        bootstrap: &str,
        client_id: String,
        group_id: String,
        topics: Vec<String>,
        security: Option<crabka_client_core::security::ClientSecurity>,
    ) -> Result<Self, crabka_client_consumer::ConsumerError> {
        let consumer = ShareConsumer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id(client_id)
            .group_id(group_id)
            .subscribe(topics)
            .ack_mode(ShareAckMode::Explicit)
            .maybe_security(security)
            .build()
            .await?;
        Ok(Self {
            consumer: Some(consumer),
            pending_acquired: VecDeque::new(),
            delivered: HashMap::new(),
            staged_acks: HashMap::new(),
        })
    }

    fn consumer(&mut self) -> &mut ShareConsumer {
        self.consumer
            .as_mut()
            .expect("queue session used after consumer close")
    }

    async fn acquire(
        &mut self,
        timeout: Duration,
        max_messages: usize,
    ) -> Result<Vec<pb::QueuedMessage>, crabka_client_consumer::ConsumerError> {
        if max_messages == 0 {
            return Ok(Vec::new());
        }

        let mut messages = self.drain_pending_acquired(max_messages);
        if messages.len() == max_messages {
            return Ok(messages);
        }

        let remaining = max_messages.saturating_sub(messages.len());
        let records = self.consumer().poll(timeout).await?;
        messages.extend(self.deliver_acquired_records(records, remaining));
        Ok(messages)
    }

    fn drain_pending_acquired(&mut self, max_messages: usize) -> Vec<pb::QueuedMessage> {
        let mut messages = Vec::with_capacity(self.pending_acquired.len().min(max_messages));
        while messages.len() < max_messages {
            let Some(record) = self.pending_acquired.pop_front() else {
                return messages;
            };
            self.delivered
                .insert(QueueRecordKey::from(&record), record.clone());
            messages.push(queued_message_from_record(record));
        }

        messages
    }

    fn deliver_acquired_records(
        &mut self,
        records: Vec<ShareConsumerRecord>,
        max_messages: usize,
    ) -> Vec<pb::QueuedMessage> {
        let mut messages = Vec::with_capacity(records.len().min(max_messages));
        for record in records {
            if messages.len() == max_messages {
                self.pending_acquired.push_back(record);
                continue;
            }

            self.delivered
                .insert(QueueRecordKey::from(&record), record.clone());
            messages.push(queued_message_from_record(record));
        }

        messages
    }

    async fn acknowledge(&mut self, entries: Vec<pb::QueueAckEntry>) -> Vec<pb::QueueAckResult> {
        let mut results = Vec::with_capacity(entries.len());
        for entry in entries {
            let (mut result, staged_ack) = self.prepare_ack(entry);
            if let Some((key, staged_ack)) = staged_ack
                && let Some(error) = self.commit_staged_ack(key, staged_ack).await
            {
                result.error = Some(error);
            }
            results.push(result);
        }

        results
    }

    async fn commit_staged_ack(
        &mut self,
        key: QueueRecordKey,
        staged_ack: StagedQueueAck,
    ) -> Option<pb::ErrorInfo> {
        if let Some(error) = self.pending_ack_conflict(&key, staged_ack.share_ack) {
            return Some(error);
        }

        if self.staged_acks.contains_key(&key) {
            // The previous commit attempt failed after staging this ack in the
            // ShareConsumer. Retrying must flush the retained pending ack without
            // staging another identical ack for the same acquired record.
        } else {
            if let Err(error) = self
                .consumer()
                .acknowledge(&staged_ack.record, staged_ack.share_ack)
            {
                return Some(error_info(13, error.to_string(), true));
            }
            self.staged_acks.insert(key.clone(), staged_ack);
        }

        if let Err(error) = self.consumer().commit().await {
            return Some(error_info(13, error.to_string(), true));
        }

        self.complete_staged_acks([key]);
        None
    }

    fn pending_ack_conflict(
        &self,
        key: &QueueRecordKey,
        share_ack: ShareAckType,
    ) -> Option<pb::ErrorInfo> {
        let existing_ack = self.staged_acks.get(key)?;
        if existing_ack.share_ack == share_ack {
            return None;
        }

        Some(error_info(
            9,
            "record already has a different pending acknowledgement",
            false,
        ))
    }

    async fn renew(&mut self, entries: Vec<pb::QueueAckEntry>) -> Vec<pb::QueueAckResult> {
        let mut results = Vec::with_capacity(entries.len());
        for entry in entries {
            let key = QueueRecordKey::from_entry(&entry);
            let Some(record) = self.delivered.get(&key).cloned() else {
                results.push(pb::QueueAckResult {
                    entry: Some(entry),
                    error: Some(error_info(
                        9,
                        "record is not acquired by this session",
                        false,
                    )),
                });
                continue;
            };

            let error = self
                .consumer()
                .renew(&record)
                .await
                .err()
                .map(|error| error_info(13, error.to_string(), true));
            results.push(pb::QueueAckResult {
                entry: Some(entry),
                error,
            });
        }
        results
    }

    fn prepare_ack(
        &self,
        entry: pb::QueueAckEntry,
    ) -> (pb::QueueAckResult, Option<(QueueRecordKey, StagedQueueAck)>) {
        let Some(ack_type) = pb::QueueAckType::try_from(entry.r#type).ok() else {
            return (
                ack_result_error(entry, 3, "unknown queue acknowledgement type", false),
                None,
            );
        };
        let Some(share_ack) = share_ack_type(ack_type) else {
            return (
                ack_result_error(entry, 3, "queue acknowledgement type is unspecified", false),
                None,
            );
        };

        let key = QueueRecordKey::from_entry(&entry);
        let Some(record) = self.delivered.get(&key).cloned() else {
            return (
                ack_result_error(entry, 9, "record is not acquired by this session", false),
                None,
            );
        };

        let result = pb::QueueAckResult {
            entry: Some(entry),
            error: None,
        };
        let staged_ack = StagedQueueAck { record, share_ack };

        (result, Some((key, staged_ack)))
    }

    fn complete_staged_acks(&mut self, keys: impl IntoIterator<Item = QueueRecordKey>) {
        for key in keys {
            self.delivered.remove(&key);
            self.staged_acks.remove(&key);
        }
    }

    #[cfg(test)]
    fn validate_ack_batch_without_commit(
        &mut self,
        entries: Vec<pb::QueueAckEntry>,
    ) -> Vec<pb::QueueAckResult> {
        let mut results = Vec::with_capacity(entries.len());
        let mut staged_keys = Vec::new();
        let mut has_invalid_entry = false;

        for entry in entries {
            let (result, staged_ack) = self.prepare_ack(entry);
            let Some((key, staged_ack)) = staged_ack else {
                has_invalid_entry = true;
                results.push(result);
                continue;
            };

            self.staged_acks.insert(key.clone(), staged_ack);
            staged_keys.push(key);
            results.push(result);
        }

        if has_invalid_entry {
            for key in staged_keys {
                self.staged_acks.remove(&key);
            }
        }

        results
    }
}

impl Drop for QueueSession {
    fn drop(&mut self) {
        if let Some(mut consumer) = self.consumer.take() {
            tokio::spawn(async move {
                let _ = consumer.close().await;
            });
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct QueueRecordKey {
    topic: String,
    partition: i32,
    offset: i64,
}

#[derive(Debug, Clone)]
struct StagedQueueAck {
    record: ShareConsumerRecord,
    share_ack: ShareAckType,
}

impl QueueRecordKey {
    fn from_entry(entry: &pb::QueueAckEntry) -> Self {
        Self {
            topic: entry.topic.clone(),
            partition: entry.partition,
            offset: entry.offset,
        }
    }
}

impl From<&ShareConsumerRecord> for QueueRecordKey {
    fn from(record: &ShareConsumerRecord) -> Self {
        Self {
            topic: record.topic.clone(),
            partition: record.partition,
            offset: record.offset,
        }
    }
}

fn queued_message_from_record(record: ShareConsumerRecord) -> pb::QueuedMessage {
    pb::QueuedMessage {
        topic: record.topic,
        partition: record.partition,
        offset: record.offset,
        key: record.key.map(|bytes| bytes.to_vec()),
        value: record.value.map_or_else(Vec::new, |bytes| bytes.to_vec()),
        headers: record
            .headers
            .into_iter()
            .map(|header| pb::Header {
                key: header.key,
                value: header.value.map(|bytes| bytes.to_vec()),
            })
            .collect(),
        timestamp_ms: record.timestamp,
        delivery_count: i32::from(record.delivery_count),
    }
}

fn share_ack_type(ack_type: pb::QueueAckType) -> Option<ShareAckType> {
    match ack_type {
        pb::QueueAckType::Accept => Some(ShareAckType::Accept),
        pb::QueueAckType::Release => Some(ShareAckType::Release),
        pb::QueueAckType::Reject => Some(ShareAckType::Reject),
        pb::QueueAckType::Unspecified => None,
    }
}

fn error_info(code: i32, message: impl Into<String>, retriable: bool) -> pb::ErrorInfo {
    pb::ErrorInfo {
        code,
        message: message.into(),
        retriable,
    }
}

fn ack_result_error(
    entry: pb::QueueAckEntry,
    code: i32,
    message: impl Into<String>,
    retriable: bool,
) -> pb::QueueAckResult {
    pb::QueueAckResult {
        entry: Some(entry),
        error: Some(error_info(code, message, retriable)),
    }
}

fn bounded_max_messages(requested: u32, configured: u32) -> usize {
    let effective = if requested == 0 {
        configured
    } else {
        requested.min(configured)
    };
    usize::try_from(effective).unwrap_or(usize::MAX)
}

fn bounded_wait(requested_ms: u32, configured_cap_ms: u32) -> Duration {
    Duration::from_millis(u64::from(requested_ms.min(configured_cap_ms)))
}

fn invalid_lock_duration(lock_duration_ms: u64) -> Option<ConnectError> {
    if lock_duration_ms == SUPPORTED_QUEUE_LOCK_DURATION_MS {
        return None;
    }

    Some(ConnectError::new_invalid_argument(format!(
        "queue lock_duration_ms must be {SUPPORTED_QUEUE_LOCK_DURATION_MS}; per-acquire lock durations are not supported"
    )))
}

fn effective_principal(principal: Option<Extension<Principal>>) -> Principal {
    principal.map_or_else(anonymous_principal, |Extension(principal)| principal)
}

/// Acquire records from a broker-backed explicit share consumer queue session.
pub async fn queue_acquire(
    Extension(state): Extension<Arc<crate::state::AppState>>,
    principal: Option<Extension<Principal>>,
    req: ConnectRequest<pb::QueueAcquireRequest>,
) -> Result<ConnectResponse<pb::QueueAcquireResponse>, ConnectError> {
    let principal = effective_principal(principal);
    let request = req.0;
    if request.group_id.is_empty() {
        return Err(ConnectError::new_invalid_argument("group_id is required"));
    }
    if request.topics.is_empty() && request.session_id.is_empty() {
        return Err(ConnectError::new_invalid_argument("topics are required"));
    }
    if let Some(error) = invalid_lock_duration(request.lock_duration_ms) {
        return Err(error);
    }

    let host = unknown_host();
    if authorize_resource(
        &state,
        &principal,
        &host,
        ResourceType::Group,
        &request.group_id,
        AclOperation::Read,
    ) == AuthorizationResult::Deny
    {
        return Err(ConnectError::new_permission_denied(format!(
            "Read Group:{}",
            request.group_id
        )));
    }
    for topic in &request.topics {
        if authorize_resource(
            &state,
            &principal,
            &host,
            ResourceType::Topic,
            topic,
            AclOperation::Read,
        ) == AuthorizationResult::Deny
        {
            return Err(ConnectError::new_permission_denied(format!(
                "Read Topic:{topic}"
            )));
        }
    }

    let max_messages = bounded_max_messages(request.max_messages, state.config.queue_max_messages);
    let wait = bounded_wait(request.wait_ms, state.config.queue_wait_ms_cap);
    let session_id = if request.session_id.is_empty() {
        let client_id = format!("{}-queue", state.config.client_id);
        let session = QueueSession::new(
            &state.config.bootstrap,
            client_id,
            request.group_id,
            request.topics,
            state.config.broker_security.clone(),
        )
        .await
        .map_err(|error| ConnectError::new_internal(error.to_string()))?;
        state.queue_sessions.insert(principal.clone(), session)?
    } else {
        request.session_id
    };

    let session = state.queue_sessions.get(&principal, &session_id)?;
    let mut session = session.lock().await;
    let messages = session
        .acquire(wait, max_messages)
        .await
        .map_err(|error| ConnectError::new_internal(error.to_string()))?;
    Ok(ConnectResponse::new(pb::QueueAcquireResponse {
        session_id,
        messages,
    }))
}

/// Acknowledge acquired queue records through the explicit share consumer session.
pub async fn queue_acknowledge(
    Extension(state): Extension<Arc<crate::state::AppState>>,
    principal: Option<Extension<Principal>>,
    req: ConnectRequest<pb::QueueAcknowledgeRequest>,
) -> Result<ConnectResponse<pb::QueueAcknowledgeResponse>, ConnectError> {
    let principal = effective_principal(principal);
    let request = req.0;
    if request.session_id.is_empty() {
        return Err(ConnectError::new_invalid_argument("session_id is required"));
    }

    let session = state.queue_sessions.get(&principal, &request.session_id)?;
    let mut session = session.lock().await;
    let results = session.acknowledge(request.entries).await;
    Ok(ConnectResponse::new(pb::QueueAcknowledgeResponse {
        results,
    }))
}

/// Renew acquired queue records and refresh the gateway-side session idle timer.
pub async fn queue_renew(
    Extension(state): Extension<Arc<crate::state::AppState>>,
    principal: Option<Extension<Principal>>,
    req: ConnectRequest<pb::QueueRenewRequest>,
) -> Result<ConnectResponse<pb::QueueRenewResponse>, ConnectError> {
    let principal = effective_principal(principal);
    let request = req.0;
    if request.session_id.is_empty() {
        return Err(ConnectError::new_invalid_argument("session_id is required"));
    }

    let session = state.queue_sessions.get(&principal, &request.session_id)?;
    let mut session = session.lock().await;
    let results = session.renew(request.entries).await;
    Ok(ConnectResponse::new(pb::QueueRenewResponse { results }))
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;

    use assert2::assert;
    use bytes::Bytes;
    use crabka_security::AuthMethod;

    use super::*;

    fn test_config() -> QueueSessionConfig {
        QueueSessionConfig {
            idle_timeout: Duration::from_secs(10),
            max_sessions: 2,
        }
    }

    fn principal(name: &str) -> Principal {
        Principal {
            name: name.to_string(),
            auth_method: AuthMethod::Anonymous,
            groups: vec![],
        }
    }

    fn record(offset: i64) -> ShareConsumerRecord {
        ShareConsumerRecord {
            topic: "topic".to_string(),
            partition: 0,
            offset,
            timestamp: 7,
            key: None,
            value: Some(Bytes::from_static(b"value")),
            headers: vec![],
            delivery_count: 1,
        }
    }

    fn ack_entry(offset: i64, ack_type: pb::QueueAckType) -> pb::QueueAckEntry {
        pb::QueueAckEntry {
            topic: "topic".to_string(),
            partition: 0,
            offset,
            r#type: ack_type as i32,
        }
    }

    fn session_with_record(record: ShareConsumerRecord) -> QueueSession {
        let mut delivered = HashMap::new();
        delivered.insert(QueueRecordKey::from(&record), record);
        QueueSession {
            consumer: None,
            pending_acquired: VecDeque::new(),
            delivered,
            staged_acks: HashMap::new(),
        }
    }

    #[test]
    fn issues_and_resolves_principal_bound_sessions() {
        let table = QueueSessionTable::new(test_config());
        let issued_at = std::time::Instant::now();

        let id = table
            .insert_at(principal("a"), "session", issued_at)
            .expect("session insert succeeds");

        assert!(id.len() >= 32);
        assert!(let Ok(_) = table.get_at(&principal("a"), &id, issued_at));
        assert!(let Err(QueueError::PermissionDenied) = table.get_at(&principal("b"), &id, issued_at));
    }

    #[test]
    fn idle_sessions_evict_and_lookup_says_expired() {
        let table = QueueSessionTable::new(test_config());
        let issued_at = std::time::Instant::now();
        let expired_at = issued_at + Duration::from_secs(11);
        let id = table
            .insert_at(principal("a"), "session", issued_at)
            .expect("session insert succeeds");

        assert!(table.evict_idle_at(expired_at) == 1);

        assert!(let Err(QueueError::SessionExpired) = table.get_at(&principal("a"), &id, expired_at));
        assert!(table.is_empty());
    }

    #[test]
    fn idle_lookup_removes_session_and_reports_expired() {
        let table = QueueSessionTable::new(test_config());
        let issued_at = std::time::Instant::now();
        let expired_at = issued_at + Duration::from_secs(10);
        let id = table
            .insert_at(principal("a"), "session", issued_at)
            .expect("session insert succeeds");

        assert!(let Err(QueueError::SessionExpired) = table.get_at(&principal("a"), &id, expired_at));

        assert!(table.is_empty());
    }

    #[test]
    fn max_sessions_cap_is_resource_exhausted() {
        let table = QueueSessionTable::new(test_config());
        let now = std::time::Instant::now();
        table
            .insert_at(principal("a"), "session-1", now)
            .expect("first session insert succeeds");
        table
            .insert_at(principal("a"), "session-2", now)
            .expect("second session insert succeeds");

        assert!(let Err(QueueError::ResourceExhausted) = table.insert_at(principal("a"), "session-3", now));
    }

    #[test]
    fn expired_sessions_free_capacity_before_insert_cap_check() {
        let table = QueueSessionTable::new(QueueSessionConfig {
            idle_timeout: Duration::from_secs(10),
            max_sessions: 1,
        });
        let issued_at = std::time::Instant::now();
        let expired_at = issued_at + Duration::from_secs(10);
        table
            .insert_at(principal("a"), "expired-session", issued_at)
            .expect("initial session insert succeeds");

        let inserted = table.insert_at(principal("a"), "fresh-session", expired_at);

        assert!(inserted.is_ok());
        assert!(table.len() == 1);
    }

    #[test]
    fn concurrent_inserts_do_not_exceed_max_sessions() {
        let table = Arc::new(QueueSessionTable::new(QueueSessionConfig {
            idle_timeout: Duration::from_secs(10),
            max_sessions: 1,
        }));
        let ready = Arc::new(Barrier::new(16));
        let now = std::time::Instant::now();

        let handles = (0..16)
            .map(|index| {
                let table = Arc::clone(&table);
                let ready = Arc::clone(&ready);
                std::thread::spawn(move || {
                    ready.wait();
                    table.insert_at(principal("a"), format!("session-{index}"), now)
                })
            })
            .collect::<Vec<_>>();

        let inserted = handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .expect("queue session insert thread panicked")
                    .is_ok()
            })
            .filter(|inserted| *inserted)
            .count();

        assert!(inserted == 1);
        assert!(table.len() == 1);
    }

    #[test]
    fn queued_message_headers_preserve_order_duplicates_and_nulls() {
        let message = pb::QueuedMessage {
            topic: "topic".to_string(),
            partition: 0,
            offset: 42,
            key: None,
            value: b"value".to_vec(),
            headers: vec![
                pb::Header {
                    key: "x".to_string(),
                    value: Some(b"first".to_vec()),
                },
                pb::Header {
                    key: "x".to_string(),
                    value: None,
                },
                pb::Header {
                    key: "x".to_string(),
                    value: Some(b"second".to_vec()),
                },
            ],
            timestamp_ms: 7,
            delivery_count: 1,
        };

        assert!(message.headers[0].value == Some(b"first".to_vec()));
        assert!(message.headers[1].key == "x");
        assert!(message.headers[1].value.is_none());
        assert!(message.headers[2].value == Some(b"second".to_vec()));
    }

    #[test]
    fn staged_ack_keeps_record_acquired_until_broker_success() {
        let mut session = session_with_record(record(42));

        let (first, staged_ack) = session.prepare_ack(ack_entry(42, pb::QueueAckType::Accept));
        let (key, staged_ack) = staged_ack.expect("valid ack is staged");
        session.staged_acks.insert(key, staged_ack);
        let (retry, _) = session.prepare_ack(ack_entry(42, pb::QueueAckType::Accept));

        assert!(first.error.is_none());
        assert!(retry.error.is_none());
        assert!(session.delivered.contains_key(&QueueRecordKey {
            topic: "topic".to_string(),
            partition: 0,
            offset: 42,
        }));
        assert!(session.staged_acks.len() == 1);
    }

    #[test]
    fn same_staged_ack_retry_is_allowed_but_conflicting_type_is_rejected() {
        let mut session = session_with_record(record(42));
        let key = QueueRecordKey {
            topic: "topic".to_string(),
            partition: 0,
            offset: 42,
        };
        let staged_ack = StagedQueueAck {
            record: record(42),
            share_ack: ShareAckType::Accept,
        };

        session.staged_acks.insert(key.clone(), staged_ack);
        let same_ack = session.pending_ack_conflict(&key, ShareAckType::Accept);
        let conflicting_ack = session.pending_ack_conflict(&key, ShareAckType::Reject);

        assert!(same_ack.is_none());
        assert!(let Some(error) = conflicting_ack);
        assert!(error.code == 9);
        assert!(!error.retriable);
        assert!(session.staged_acks.len() == 1);
    }

    #[test]
    fn broker_success_removes_delivered_and_staged_ack_state() {
        let mut session = session_with_record(record(7));
        let key = QueueRecordKey {
            topic: "topic".to_string(),
            partition: 0,
            offset: 7,
        };

        let (staged, staged_ack) = session.prepare_ack(ack_entry(7, pb::QueueAckType::Release));
        let (_, staged_ack) = staged_ack.expect("valid ack is staged");
        session.staged_acks.insert(key.clone(), staged_ack);
        session.complete_staged_acks([key.clone()]);
        let (retry_after_success, _) = session.prepare_ack(ack_entry(7, pb::QueueAckType::Release));

        assert!(staged.error.is_none());
        assert!(!session.delivered.contains_key(&key));
        assert!(!session.staged_acks.contains_key(&key));
        assert!(retry_after_success.error.is_some());
    }

    #[test]
    fn acquired_overflow_is_retained_until_later_acquire() {
        let mut session = QueueSession {
            consumer: None,
            pending_acquired: VecDeque::new(),
            delivered: HashMap::new(),
            staged_acks: HashMap::new(),
        };

        let first_batch = session.deliver_acquired_records(vec![record(1), record(2)], 1);
        let overflow_ack = session
            .prepare_ack(ack_entry(2, pb::QueueAckType::Accept))
            .0;
        let second_batch = session.drain_pending_acquired(1);
        let delivered_ack = session
            .prepare_ack(ack_entry(2, pb::QueueAckType::Accept))
            .0;

        assert!(first_batch.len() == 1);
        assert!(first_batch[0].offset == 1);
        assert!(overflow_ack.error.is_some());
        assert!(second_batch.len() == 1);
        assert!(second_batch[0].offset == 2);
        assert!(delivered_ack.error.is_none());
        assert!(session.pending_acquired.is_empty());
    }

    #[test]
    fn non_default_lock_duration_is_rejected_until_share_fetch_supports_it() {
        let error = invalid_lock_duration(1_000).expect("non-default duration rejected");

        assert!(error.code() == connectrpc_axum::message::Code::InvalidArgument);
        assert!(error.message().is_some_and(|message| {
            message.contains("lock_duration_ms") && message.contains("not supported")
        }));
        assert!(invalid_lock_duration(SUPPORTED_QUEUE_LOCK_DURATION_MS).is_none());
    }

    #[test]
    fn invalid_ack_batch_does_not_persist_valid_staged_ack() {
        let mut session = session_with_record(record(42));

        let results = session.validate_ack_batch_without_commit(vec![
            ack_entry(42, pb::QueueAckType::Accept),
            ack_entry(43, pb::QueueAckType::Accept),
        ]);

        assert!(results[0].error.is_none());
        assert!(results[1].error.is_some());
        assert!(session.staged_acks.is_empty());
        assert!(session.delivered.contains_key(&QueueRecordKey {
            topic: "topic".to_string(),
            partition: 0,
            offset: 42,
        }));
    }
}
