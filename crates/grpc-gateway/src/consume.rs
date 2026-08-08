//! Consume core: a group-subscribed session that yields records and commits
//! offsets.
//!
//! The streaming/poll wire (later plan) drives this session. The codec decodes
//! each record on the way out.

use std::sync::Arc;

use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_units::prelude::*;

use crate::{
    codec::{RecordCodec, SchemaMeta},
    error::GatewayError,
    ids::{Offset, PartitionIndex, Timestamp},
};

#[derive(Debug, Clone)]
pub struct DecodedConsumerRecord {
    pub topic: String,
    pub partition: PartitionIndex,
    pub offset: Offset,
    pub timestamp: Timestamp,
    pub key: Option<bytes::Bytes>,
    pub value: bytes::Bytes,
    pub headers: Vec<crabka_client_consumer::Header>,
    pub schema: Option<SchemaMeta>,
    pub json: Option<bytes::Bytes>,
}

pub struct ConsumeSession {
    /// Held in an `Option` so [`Drop`] can `take()` the consumer and stop its
    /// background coordinator. See the `Drop` impl. This field is always `Some`
    /// while the session is alive, and `None` only for a moment inside `drop`.
    consumer: Option<Consumer>,
    codec: Arc<dyn RecordCodec>,
}

impl ConsumeSession {
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn new(
        bootstrap: &str,
        group_id: &str,
        client_id: &str,
        topics: Vec<String>,
        security: Option<crabka_client_core::security::ClientSecurity>,
        codec: Arc<dyn RecordCodec>,
    ) -> Result<Self, GatewayError> {
        Self::new_with_policy(
            bootstrap,
            group_id,
            client_id,
            topics,
            security,
            codec,
            &crate::config::GatewayRuntimeConfig::default(),
        )
        .await
    }

    /// Build a consume session with the deployment's client resource policy.
    /// # Errors
    /// Returns an error when client construction fails.
    pub async fn new_with_policy(
        bootstrap: &str,
        group_id: &str,
        client_id: &str,
        topics: Vec<String>,
        security: Option<crabka_client_core::security::ClientSecurity>,
        codec: Arc<dyn RecordCodec>,
        policy: &crate::config::GatewayRuntimeConfig,
    ) -> Result<Self, GatewayError> {
        let consumer = Consumer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id(client_id.to_string())
            .dispatch_queue_capacity(policy.client_dispatch_queue_capacity.get())
            .frame_max(policy.client_frame_max.size())
            .group_id(group_id.to_string())
            .subscribe(topics)
            .isolation_level(IsolationLevel::ReadCommitted)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .maybe_security(security)
            .build()
            .await?;
        Ok(Self {
            consumer: Some(consumer),
            codec,
        })
    }

    /// Poll a batch. The codec decodes each record value.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub async fn poll(
        &mut self,
        timeout: Time,
    ) -> Result<Vec<DecodedConsumerRecord>, GatewayError> {
        let batch = self
            .consumer
            .as_mut()
            .expect("ConsumeSession polled after close")
            .poll(timeout)
            .await?;
        let mut decoded_batch = Vec::with_capacity(batch.len());
        for r in batch {
            let (value, schema, json) = match r.value {
                Some(v) => {
                    let decoded = self.codec.decode(&r.topic, v).await?;
                    (decoded.value, decoded.schema, decoded.json)
                }
                None => (bytes::Bytes::new(), None, None),
            };
            decoded_batch.push(DecodedConsumerRecord {
                topic: r.topic,
                partition: PartitionIndex(r.partition),
                offset: Offset(r.offset),
                timestamp: Timestamp(r.timestamp),
                key: r.key,
                value,
                headers: r.headers,
                schema,
                json,
            });
        }
        Ok(decoded_batch)
    }

    /// Commit the current positions. For at-least-once, call this after the
    /// receiver acknowledges delivery.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub async fn commit(&self) -> Result<(), GatewayError> {
        self.consumer
            .as_ref()
            .expect("ConsumeSession committed after close")
            .commit_sync()
            .await?;
        Ok(())
    }
}

impl Drop for ConsumeSession {
    fn drop(&mut self) {
        if let Some(consumer) = self.consumer.take() {
            // The underlying `Consumer` runs a background coordinator task
            // (heartbeat + rebalance loop) that is torn down ONLY by
            // `Consumer::close()`. Merely dropping the consumer detaches that
            // task's `JoinHandle`, so it keeps heartbeating forever — leaking a
            // task + socket and orphaning a live group member (which stalls
            // rebalances for the rest of the group). Streaming drops sessions on
            // EVERY exit path (control-stream close/error, any break, or an
            // abrupt client disconnect dropping the response generator), so the
            // teardown belongs here.
            //
            // `close()` is async and consumes `self`, so spawn it detached. The
            // gateway always drops sessions inside the server's tokio runtime,
            // so a runtime is guaranteed to be available here.
            tokio::spawn(async move {
                let _ = consumer.close().await;
            });
        }
    }
}
