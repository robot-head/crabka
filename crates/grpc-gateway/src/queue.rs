//! Pull-based gateway access to Kafka share groups, the KIP-932 work queues.

use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{
        Arc, Mutex as StdMutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use axum::Extension;
use connectrpc_axum::message::{ConnectError, ConnectRequest, ConnectResponse};
use crabka_authz::AuthorizationResult;
use crabka_client_consumer::{
    ShareAckMode, ShareAckType, ShareAcquireMode, ShareConsumer, ShareConsumerRecord,
};
use crabka_metadata::{AclOperation, ResourceType};
use crabka_security::Principal;
use crabka_units::{Time, convert::TimeExt};
use dashmap::DashMap;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

use crate::{
    handlers::{anonymous_principal, authorize_resource, unknown_host},
    pb,
    state::AppState,
};

const MAX_SESSIONS: usize = 1_024;
const MAX_MESSAGES: u32 = 500;
const MAX_WAIT_MS: u32 = 30_000;
const SUPPORTED_QUEUE_LOCK_DURATION_MS: u64 = 30_000;
const SESSION_IDLE: Duration = Duration::from_mins(1);
const SESSION_SWEEP: Duration = Duration::from_secs(5);

struct QueueSession {
    principal: Principal,
    group_id: String,
    topics: Vec<String>,
    consumer: Mutex<ShareConsumer>,
    acquired: StdMutex<HashSet<QueueCoordinate>>,
    max_messages: u32,
    last_used: StdMutex<Instant>,
    _permit: OwnedSemaphorePermit,
}

type QueueCoordinate = (String, i32, i64);

impl QueueSession {
    fn touch(&self) {
        *self
            .last_used
            .lock()
            .expect("queue session timestamp mutex poisoned") = Instant::now();
    }

    fn is_expired(&self) -> bool {
        self.last_used
            .lock()
            .expect("queue session timestamp mutex poisoned")
            .elapsed()
            >= SESSION_IDLE
    }
}

/// Process-local queue sessions.
///
/// Broker-side acquisition expiry stays the durability and redelivery
/// authority. This table only keeps native consumer connections alive between
/// unary RPCs.
pub struct QueueSessionTable {
    sessions: DashMap<String, Arc<QueueSession>>,
    capacity: Arc<Semaphore>,
    cleanup_started: AtomicBool,
}

impl Default for QueueSessionTable {
    fn default() -> Self {
        Self {
            sessions: DashMap::new(),
            capacity: Arc::new(Semaphore::new(MAX_SESSIONS)),
            cleanup_started: AtomicBool::new(false),
        }
    }
}

impl QueueSessionTable {
    fn close_removed(session: Arc<QueueSession>) {
        tokio::spawn(async move {
            let mut consumer = session.consumer.lock().await;
            let _ = consumer.close().await;
        });
    }

    fn remove_expired(&self) {
        let ids: Vec<String> = self
            .sessions
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        for id in ids {
            if let Some((_, session)) = self.sessions.remove_if(&id, |_, s| s.is_expired()) {
                Self::close_removed(session);
            }
        }
    }

    fn start_cleanup(self: &Arc<Self>) {
        if self
            .cleanup_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let table = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(SESSION_SWEEP);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await;
            loop {
                interval.tick().await;
                let Some(table) = Weak::upgrade(&table) else {
                    break;
                };
                table.remove_expired();
            }
        });
    }

    #[allow(clippy::result_large_err)]
    fn reserve(self: &Arc<Self>) -> Result<OwnedSemaphorePermit, ConnectError> {
        self.start_cleanup();
        Arc::clone(&self.capacity)
            .try_acquire_owned()
            .map_err(|_| ConnectError::new_resource_exhausted("queue session limit reached"))
    }

    fn insert(&self, session: QueueSession) -> (String, Arc<QueueSession>) {
        let id = uuid::Uuid::new_v4().simple().to_string();
        let session = Arc::new(session);
        self.sessions.insert(id.clone(), Arc::clone(&session));
        (id, session)
    }

    fn remove(&self, id: &str, expected: &Arc<QueueSession>) {
        if let Some((_, session)) = self
            .sessions
            .remove_if(id, |_, current| Arc::ptr_eq(current, expected))
        {
            Self::close_removed(session);
        }
    }

    #[allow(clippy::result_large_err)]
    fn get(&self, id: &str, principal: &Principal) -> Result<Arc<QueueSession>, ConnectError> {
        let session = match self.sessions.get(id) {
            Some(entry) if !entry.value().is_expired() => {
                if entry.value().principal != *principal {
                    return Err(ConnectError::new_permission_denied(
                        "queue session belongs to another principal",
                    ));
                }
                entry.value().touch();
                Arc::clone(entry.value())
            }
            Some(entry) => {
                drop(entry);
                if let Some((_, removed)) = self
                    .sessions
                    .remove_if(id, |_, session| session.is_expired())
                {
                    Self::close_removed(removed);
                }
                return Err(ConnectError::new_failed_precondition(
                    "queue session expired; re-acquire",
                ));
            }
            None => {
                return Err(ConnectError::new_failed_precondition(
                    "queue session expired; re-acquire",
                ));
            }
        };
        Ok(session)
    }
}

fn effective_identity(
    principal: Option<Extension<Principal>>,
    peer: Option<Extension<SocketAddr>>,
) -> (Principal, SocketAddr) {
    (
        principal.map_or_else(anonymous_principal, |Extension(p)| p),
        peer.map_or_else(unknown_host, |Extension(h)| h),
    )
}

#[allow(clippy::result_large_err)]
fn authorize_queue(
    state: &AppState,
    principal: &Principal,
    host: &SocketAddr,
    group_id: &str,
    topics: &[String],
) -> Result<(), ConnectError> {
    if matches!(
        authorize_resource(
            state,
            principal,
            host,
            ResourceType::Group,
            group_id,
            AclOperation::Read,
        ),
        AuthorizationResult::Deny
    ) {
        return Err(ConnectError::new_permission_denied(format!(
            "Read Group:{group_id}"
        )));
    }
    for topic in topics {
        if matches!(
            authorize_resource(
                state,
                principal,
                host,
                ResourceType::Topic,
                topic,
                AclOperation::Read,
            ),
            AuthorizationResult::Deny
        ) {
            return Err(ConnectError::new_permission_denied(format!(
                "Read Topic:{topic}"
            )));
        }
    }
    Ok(())
}

async fn start_session(
    state: &AppState,
    principal: Principal,
    group_id: String,
    topics: Vec<String>,
    max_messages: u32,
    permit: OwnedSemaphorePermit,
) -> Result<QueueSession, ConnectError> {
    if group_id.is_empty() {
        return Err(ConnectError::new_invalid_argument(
            "queue group is required",
        ));
    }
    if topics.is_empty() || topics.iter().any(String::is_empty) {
        return Err(ConnectError::new_invalid_argument(
            "queue topic is required",
        ));
    }
    let consumer = ShareConsumer::builder()
        .bootstrap(state.config.bootstrap.clone())
        .client_id(format!("{}-queue", state.config.client_id))
        .group_id(group_id.clone())
        .subscribe(topics.clone())
        .ack_mode(ShareAckMode::Explicit)
        .acquire_mode(ShareAcquireMode::RecordLimit)
        .fetch_max_records(i32::try_from(max_messages).expect("queue cap fits i32"))
        .dispatch_queue_capacity(state.config.runtime.client_dispatch_queue_capacity.get())
        .frame_max(state.config.runtime.client_frame_max.size())
        .maybe_security(state.config.broker_security.clone())
        .build()
        .await
        .map_err(|error| ConnectError::new_unavailable(error.to_string()))?;
    Ok(QueueSession {
        principal,
        group_id,
        topics,
        consumer: Mutex::new(consumer),
        acquired: StdMutex::new(HashSet::new()),
        max_messages,
        last_used: StdMutex::new(Instant::now()),
        _permit: permit,
    })
}

#[allow(clippy::result_large_err)]
fn validate_session_subscription(
    session_group_id: &str,
    session_topics: &[String],
    request_group_id: &str,
    request_topics: &[String],
) -> Result<(), ConnectError> {
    if session_group_id == request_group_id && session_topics == request_topics {
        return Ok(());
    }
    Err(ConnectError::new_invalid_argument(
        "group_id and topics are fixed when a queue session is created",
    ))
}

fn queued_message(record: ShareConsumerRecord) -> pb::QueuedMessage {
    pb::QueuedMessage {
        topic: record.topic,
        partition: record.partition,
        offset: record.offset,
        key: record.key.map(|key| key.to_vec()),
        value: record.value.map_or_else(Vec::new, |value| value.to_vec()),
        headers: record
            .headers
            .into_iter()
            .map(|(key, value)| pb::Header {
                key,
                value: value.map(|value| value.to_vec()),
            })
            .collect(),
        timestamp_ms: record.timestamp,
        delivery_count: i32::from(record.delivery_count),
    }
}

fn validate_lock_duration(lock_duration_ms: u64) -> Result<(), String> {
    if lock_duration_ms == SUPPORTED_QUEUE_LOCK_DURATION_MS {
        return Ok(());
    }
    Err(format!(
        "queue lock_duration_ms must be {SUPPORTED_QUEUE_LOCK_DURATION_MS}; per-acquire lock durations are not supported"
    ))
}

/// Acquire up to the session's fixed message limit from a share group.
///
/// # Errors
/// Returns a Connect error for invalid input, denied access, session expiry, or
/// native share-consumer failures.
///
/// # Panics
/// Panics if the process-local queue session mutex is poisoned.
pub async fn queue_acquire(
    Extension(state): Extension<Arc<AppState>>,
    principal: Option<Extension<Principal>>,
    peer: Option<Extension<SocketAddr>>,
    req: ConnectRequest<pb::QueueAcquireRequest>,
) -> Result<ConnectResponse<pb::QueueAcquireResponse>, ConnectError> {
    let request = req.0;
    validate_lock_duration(request.lock_duration_ms).map_err(ConnectError::new_invalid_argument)?;
    let (principal, host) = effective_identity(principal, peer);
    let requested_max = request.max_messages.clamp(1, MAX_MESSAGES);
    let is_new = request.session_id.is_empty();
    let (session_id, session) = if is_new {
        authorize_queue(
            &state,
            &principal,
            &host,
            &request.group_id,
            &request.topics,
        )?;
        let permit = state.queue.reserve()?;
        let session = start_session(
            &state,
            principal,
            request.group_id,
            request.topics,
            requested_max,
            permit,
        )
        .await?;
        state.queue.insert(session)
    } else {
        let session = state.queue.get(&request.session_id, &principal)?;
        validate_session_subscription(
            &session.group_id,
            &session.topics,
            &request.group_id,
            &request.topics,
        )?;
        if request.max_messages != 0 && requested_max != session.max_messages {
            return Err(ConnectError::new_invalid_argument(
                "max_messages is fixed when a queue session is created",
            ));
        }
        (request.session_id, session)
    };

    let wait_ms = request.wait_ms.min(MAX_WAIT_MS);
    let records = session
        .consumer
        .lock()
        .await
        .poll(Time::from_millis(i64::from(wait_ms)))
        .await;
    let records = match records {
        Ok(records) => records,
        Err(error) => {
            if is_new {
                state.queue.remove(&session_id, &session);
            }
            return Err(ConnectError::new_unavailable(error.to_string()));
        }
    };
    session
        .acquired
        .lock()
        .expect("queue acquired-record mutex poisoned")
        .extend(
            records
                .iter()
                .map(|record| (record.topic.clone(), record.partition, record.offset)),
        );
    session.touch();
    Ok(ConnectResponse::new(pb::QueueAcquireResponse {
        session_id,
        messages: records.into_iter().map(queued_message).collect(),
    }))
}

fn ack_error(error: &crabka_client_consumer::ConsumerError) -> pb::ErrorInfo {
    let code = match &error {
        crabka_client_consumer::ConsumerError::Server(code) => i32::from(*code),
        _ => 13,
    };
    pb::ErrorInfo {
        code,
        message: error.to_string(),
        retriable: matches!(
            error,
            crabka_client_consumer::ConsumerError::Client(_)
                | crabka_client_consumer::ConsumerError::CoordinatorUnavailable
                | crabka_client_consumer::ConsumerError::CommitInvalid
                | crabka_client_consumer::ConsumerError::RebalanceFailed(_)
                | crabka_client_consumer::ConsumerError::Server(
                    3 | 5 | 6 | 7 | 8 | 9 | 13 | 14 | 15 | 16 | 19 | 20 | 41 | 56 | 80 | 88 | 89
                )
        ),
    }
}

fn ack_record(entry: &pb::QueueAckEntry) -> ShareConsumerRecord {
    ShareConsumerRecord {
        topic: entry.topic.clone(),
        partition: entry.partition,
        offset: entry.offset,
        timestamp: 0,
        key: None,
        value: None,
        headers: Vec::new(),
        delivery_count: 0,
    }
}

fn not_acquired_error() -> pb::ErrorInfo {
    pb::ErrorInfo {
        code: 9,
        message: "record is not acquired by this session".to_string(),
        retriable: false,
    }
}

fn coordinate(entry: &pb::QueueAckEntry) -> QueueCoordinate {
    (entry.topic.clone(), entry.partition, entry.offset)
}

fn contains_acquired(acquired: &HashSet<QueueCoordinate>, entry: &pb::QueueAckEntry) -> bool {
    acquired.contains(&coordinate(entry))
}

fn is_acquired(session: &QueueSession, entry: &pb::QueueAckEntry) -> bool {
    let acquired = session
        .acquired
        .lock()
        .expect("queue acquired-record mutex poisoned");
    contains_acquired(&acquired, entry)
}

fn finish_acquired(session: &QueueSession, entry: &pb::QueueAckEntry) {
    session
        .acquired
        .lock()
        .expect("queue acquired-record mutex poisoned")
        .remove(&coordinate(entry));
}

async fn acknowledge_entry(
    consumer: &mut ShareConsumer,
    entry: &pb::QueueAckEntry,
    ack: ShareAckType,
) -> Option<crabka_client_consumer::ConsumerError> {
    if let Err(error) = consumer.acknowledge(&ack_record(entry), ack) {
        return Some(error);
    }
    consumer.commit().await.err()
}

/// Apply explicit share acknowledgements. This function commits each entry
/// separately, so it can return a broker verdict for that exact coordinate.
///
/// # Errors
/// Returns a Connect error when the queue session is missing or belongs to a
/// different principal.
pub async fn queue_acknowledge(
    Extension(state): Extension<Arc<AppState>>,
    principal: Option<Extension<Principal>>,
    req: ConnectRequest<pb::QueueAcknowledgeRequest>,
) -> Result<ConnectResponse<pb::QueueAcknowledgeResponse>, ConnectError> {
    let principal = principal.map_or_else(anonymous_principal, |Extension(p)| p);
    let session = state.queue.get(&req.0.session_id, &principal)?;
    let mut consumer = session.consumer.lock().await;
    let mut results = Vec::with_capacity(req.0.entries.len());
    for entry in req.0.entries {
        let ack = match pb::QueueAckType::try_from(entry.r#type) {
            Ok(pb::QueueAckType::Accept) => Some(ShareAckType::Accept),
            Ok(pb::QueueAckType::Release) => Some(ShareAckType::Release),
            Ok(pb::QueueAckType::Reject) => Some(ShareAckType::Reject),
            Ok(pb::QueueAckType::Unspecified) | Err(_) => None,
        };
        if ack.is_some() && !is_acquired(&session, &entry) {
            results.push(pb::QueueAckResult {
                entry: Some(entry),
                error: Some(not_acquired_error()),
            });
            continue;
        }
        let error = match ack {
            Some(ack) => acknowledge_entry(&mut consumer, &entry, ack).await,
            None => Some(crabka_client_consumer::ConsumerError::IllegalState(
                "queue acknowledgement type is required".to_string(),
            )),
        };
        if error.is_none() {
            finish_acquired(&session, &entry);
        }
        results.push(pb::QueueAckResult {
            entry: Some(entry),
            error: error.map(|error| ack_error(&error)),
        });
    }
    session.touch();
    Ok(ConnectResponse::new(pb::QueueAcknowledgeResponse {
        results,
    }))
}

/// Renew acquisition locks for long-running queue work.
///
/// # Errors
/// Returns a Connect error when the queue session is missing or belongs to a
/// different principal.
pub async fn queue_renew(
    Extension(state): Extension<Arc<AppState>>,
    principal: Option<Extension<Principal>>,
    req: ConnectRequest<pb::QueueRenewRequest>,
) -> Result<ConnectResponse<pb::QueueRenewResponse>, ConnectError> {
    let principal = principal.map_or_else(anonymous_principal, |Extension(p)| p);
    let session = state.queue.get(&req.0.session_id, &principal)?;
    let mut consumer = session.consumer.lock().await;
    let mut results = Vec::with_capacity(req.0.entries.len());
    for entry in req.0.entries {
        if !is_acquired(&session, &entry) {
            results.push(pb::QueueAckResult {
                entry: Some(entry),
                error: Some(not_acquired_error()),
            });
            continue;
        }
        let error = consumer
            .renew(&ack_record(&entry))
            .await
            .err()
            .map(|error| ack_error(&error));
        results.push(pb::QueueAckResult {
            entry: Some(entry),
            error,
        });
    }
    session.touch();
    Ok(ConnectResponse::new(pb::QueueRenewResponse { results }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn queue_session_capacity_is_reserved_before_consumer_creation() {
        let table = Arc::new(QueueSessionTable {
            sessions: DashMap::new(),
            capacity: Arc::new(Semaphore::new(1)),
            cleanup_started: AtomicBool::new(false),
        });

        let permit = table.reserve().expect("first reservation");
        assert2::assert!(table.reserve().is_err());
        drop(permit);
        assert2::assert!(table.reserve().is_ok());
    }

    #[test]
    fn queued_message_preserves_nulls_duplicate_headers_and_delivery_count() {
        let message = queued_message(ShareConsumerRecord {
            topic: "jobs".to_string(),
            partition: 2,
            offset: 7,
            timestamp: 11,
            key: None,
            value: None,
            headers: vec![
                (
                    "ce_type".to_string(),
                    Some(bytes::Bytes::from_static(b"job.created")),
                ),
                ("ce_type".to_string(), None),
            ],
            delivery_count: 3,
        });

        assert2::assert!(
            (message.value, message.delivery_count, message.headers)
                == (
                    Vec::new(),
                    3,
                    vec![
                        pb::Header {
                            key: "ce_type".to_string(),
                            value: Some(b"job.created".to_vec()),
                        },
                        pb::Header {
                            key: "ce_type".to_string(),
                            value: None,
                        },
                    ],
                )
        );
    }

    #[test]
    fn queue_lock_duration_is_fixed_to_broker_supported_value() {
        assert2::assert!(validate_lock_duration(SUPPORTED_QUEUE_LOCK_DURATION_MS).is_ok());
        let error = validate_lock_duration(1_000)
            .map_err(ConnectError::new_invalid_argument)
            .expect_err("custom duration rejected");
        assert2::assert!(error.code() == connectrpc_axum::message::Code::InvalidArgument);
        assert2::assert!(
            error
                .message()
                .is_some_and(|message| message.contains("lock_duration_ms"))
        );
    }

    #[test]
    fn acknowledgement_preserves_broker_error_code() {
        let error = ack_error(&crabka_client_consumer::ConsumerError::Server(121));
        assert2::assert!(
            error
                == pb::ErrorInfo {
                    code: 121,
                    message: "broker error_code 121".to_string(),
                    retriable: false,
                }
        );
        assert2::assert!(ack_error(&crabka_client_consumer::ConsumerError::Server(6)).retriable);
    }

    #[test]
    fn unknown_queue_coordinate_is_rejected_before_the_broker() {
        let acquired = HashSet::from([("jobs".to_string(), 0, 7)]);
        let known = pb::QueueAckEntry {
            topic: "jobs".to_string(),
            partition: 0,
            offset: 7,
            r#type: pb::QueueAckType::Accept as i32,
        };
        let unknown = pb::QueueAckEntry {
            topic: "missing".to_string(),
            ..known.clone()
        };
        let error = not_acquired_error();
        assert2::assert!(
            (
                contains_acquired(&acquired, &known),
                contains_acquired(&acquired, &unknown),
                error
            ) == (
                true,
                false,
                pb::ErrorInfo {
                    code: 9,
                    message: "record is not acquired by this session".to_string(),
                    retriable: false,
                }
            )
        );
    }

    #[test]
    fn queue_session_subscription_is_immutable() {
        let topics = vec!["jobs".to_string()];
        assert2::assert!(
            validate_session_subscription("workers", &topics, "workers", &topics).is_ok()
        );

        for (group_id, request_topics) in [
            ("other-workers", vec!["jobs".to_string()]),
            ("workers", vec!["other-jobs".to_string()]),
        ] {
            let error =
                validate_session_subscription("workers", &topics, group_id, &request_topics)
                    .expect_err("changed queue subscription rejected");
            assert2::assert!(
                error.code() == connectrpc_axum::message::Code::InvalidArgument
                    && error.message().is_some_and(|message| message
                        == "group_id and topics are fixed when a queue session is created")
            );
        }
    }
}
