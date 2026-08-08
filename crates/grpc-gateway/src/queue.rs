//! Pull-based gateway access to Kafka share groups (KIP-932 work queues).

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex as StdMutex},
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
use tokio::sync::Mutex;

use crate::{
    handlers::{anonymous_principal, authorize_resource, unknown_host},
    pb,
    state::AppState,
};

const MAX_SESSIONS: usize = 1_024;
const MAX_MESSAGES: u32 = 500;
const MAX_WAIT_MS: u32 = 30_000;
const SESSION_IDLE: Duration = Duration::from_mins(1);

struct QueueSession {
    principal: Principal,
    consumer: Mutex<ShareConsumer>,
    max_messages: u32,
    last_used: StdMutex<Instant>,
}

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

/// Process-local queue sessions. Broker-side acquisition expiry remains the
/// durability and redelivery authority; this table only keeps native consumer
/// connections alive between unary RPCs.
#[derive(Default)]
pub struct QueueSessionTable {
    sessions: DashMap<String, Arc<QueueSession>>,
}

impl QueueSessionTable {
    fn close_removed(session: Arc<QueueSession>) {
        tokio::spawn(async move {
            let mut consumer = session.consumer.lock().await;
            let _ = consumer.close().await;
        });
    }

    fn remove_expired(&self) {
        let expired: Vec<String> = self
            .sessions
            .iter()
            .filter(|entry| entry.value().is_expired())
            .map(|entry| entry.key().clone())
            .collect();
        for id in expired {
            if let Some((_, session)) = self.sessions.remove(&id) {
                Self::close_removed(session);
            }
        }
    }

    #[allow(clippy::result_large_err)]
    fn insert(&self, session: QueueSession) -> Result<(String, Arc<QueueSession>), ConnectError> {
        self.remove_expired();
        if self.sessions.len() >= MAX_SESSIONS {
            return Err(ConnectError::new_resource_exhausted(
                "queue session limit reached",
            ));
        }
        let id = uuid::Uuid::new_v4().simple().to_string();
        let session = Arc::new(session);
        self.sessions.insert(id.clone(), Arc::clone(&session));
        Ok((id, session))
    }

    #[allow(clippy::result_large_err)]
    fn get(&self, id: &str, principal: &Principal) -> Result<Arc<QueueSession>, ConnectError> {
        let session = self
            .sessions
            .get(id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| {
                ConnectError::new_failed_precondition("queue session expired; re-acquire")
            })?;
        if session.is_expired() {
            if let Some((_, removed)) = self.sessions.remove(id) {
                Self::close_removed(removed);
            }
            return Err(ConnectError::new_failed_precondition(
                "queue session expired; re-acquire",
            ));
        }
        if session.principal != *principal {
            return Err(ConnectError::new_permission_denied(
                "queue session belongs to another principal",
            ));
        }
        session.touch();
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
) -> Result<QueueSession, ConnectError> {
    if group_id.is_empty() || topics.is_empty() {
        return Err(ConnectError::new_invalid_argument(
            "group_id and at least one topic are required",
        ));
    }
    let consumer = ShareConsumer::builder()
        .bootstrap(state.config.bootstrap.clone())
        .client_id(format!("{}-queue", state.config.client_id))
        .group_id(group_id)
        .subscribe(topics)
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
        consumer: Mutex::new(consumer),
        max_messages,
        last_used: StdMutex::new(Instant::now()),
    })
}

fn queued_message(record: ShareConsumerRecord) -> pb::QueuedMessage {
    pb::QueuedMessage {
        topic: record.topic,
        partition: record.partition,
        offset: record.offset,
        key: record.key.map(|key| key.to_vec()),
        value: record.value.unwrap_or_default().to_vec(),
        headers: record
            .headers
            .into_iter()
            .map(|(key, value)| (key, value.unwrap_or_default().to_vec()))
            .collect(),
        timestamp_ms: record.timestamp,
        delivery_count: i32::from(record.delivery_count),
    }
}

/// Acquire up to the session's fixed message limit from a share group.
///
/// # Errors
/// Returns a Connect error for invalid input, denied access, session expiry, or
/// native share-consumer failures.
pub async fn queue_acquire(
    Extension(state): Extension<Arc<AppState>>,
    principal: Option<Extension<Principal>>,
    peer: Option<Extension<SocketAddr>>,
    req: ConnectRequest<pb::QueueAcquireRequest>,
) -> Result<ConnectResponse<pb::QueueAcquireResponse>, ConnectError> {
    let request = req.0;
    let (principal, host) = effective_identity(principal, peer);
    let requested_max = request.max_messages.clamp(1, MAX_MESSAGES);
    let (session_id, session) = if request.session_id.is_empty() {
        authorize_queue(
            &state,
            &principal,
            &host,
            &request.group_id,
            &request.topics,
        )?;
        let session = start_session(
            &state,
            principal,
            request.group_id,
            request.topics,
            requested_max,
        )
        .await?;
        state.queue.insert(session)?
    } else {
        let session = state.queue.get(&request.session_id, &principal)?;
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
        .await
        .map_err(|error| ConnectError::new_unavailable(error.to_string()))?;
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
        retriable: false,
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

/// Apply explicit share acknowledgements. Each entry is committed separately
/// so a broker verdict can be returned for that exact coordinate.
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
        let error = match ack {
            Some(ack) => consumer.acknowledge(&ack_record(&entry), ack).err(),
            None => Some(crabka_client_consumer::ConsumerError::IllegalState(
                "queue acknowledgement type is required".to_string(),
            )),
        };
        let error = if error.is_none() {
            consumer.commit().await.err()
        } else {
            error
        };
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

    #[test]
    fn queued_message_preserves_headers_and_delivery_count() {
        let message = queued_message(ShareConsumerRecord {
            topic: "jobs".to_string(),
            partition: 2,
            offset: 7,
            timestamp: 11,
            key: None,
            value: Some(bytes::Bytes::from_static(b"work")),
            headers: vec![(
                "ce_type".to_string(),
                Some(bytes::Bytes::from_static(b"job.created")),
            )],
            delivery_count: 3,
        });

        assert2::assert!(
            (message.value, message.delivery_count, message.headers)
                == (
                    b"work".to_vec(),
                    3,
                    std::collections::HashMap::from([(
                        "ce_type".to_string(),
                        b"job.created".to_vec(),
                    )]),
                )
        );
    }

    #[test]
    fn acknowledgement_preserves_broker_error_code() {
        let error = ack_error(&crabka_client_consumer::ConsumerError::Server(121));
        assert_eq!(error.code, 121);
        assert_eq!(error.message, "broker error_code 121");
    }
}
