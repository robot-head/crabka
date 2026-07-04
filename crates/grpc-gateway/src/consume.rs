//! Consume core: a group-subscribed session that yields records and commits
//! offsets. The streaming/poll wire (later plan) drives this. Records are
//! decoded through the codec on the way out.

use std::{sync::Arc, time::Duration};

use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};

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
    pub schema: Option<SchemaMeta>,
    pub json: Option<bytes::Bytes>,
}

pub struct ConsumeSession {
    /// Held in an `Option` so [`Drop`] can `take()` the consumer and tear down
    /// its background coordinator (see the `Drop` impl). Always `Some` while the
    /// session is alive; only `None` transiently inside `drop`.
    consumer: Option<Consumer>,
    codec: Arc<dyn RecordCodec>,
}

impl ConsumeSession {
    pub async fn new(
        bootstrap: &str,
        group_id: &str,
        client_id: &str,
        topics: Vec<String>,
        security: Option<crabka_client_core::security::ClientSecurity>,
        codec: Arc<dyn RecordCodec>,
    ) -> Result<Self, GatewayError> {
        let consumer = Consumer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id(client_id.to_string())
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

    /// Poll a batch; record values are decoded through the codec.
    pub async fn poll(
        &mut self,
        timeout: Duration,
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
                schema,
                json,
            });
        }
        Ok(decoded_batch)
    }

    /// Commit current positions (at-least-once: call after delivery is acked).
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
