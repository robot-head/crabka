//! Consume core: a group-subscribed session that yields records and commits
//! offsets. The streaming/poll wire (later plan) drives this. Records are
//! decoded through the codec on the way out.

use std::sync::Arc;
use std::time::Duration;

use crabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerRecord, IsolationLevel};

use crate::codec::RecordCodec;
use crate::error::GatewayError;

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
    pub async fn poll(&mut self, timeout: Duration) -> Result<Vec<ConsumerRecord>, GatewayError> {
        let mut batch = self
            .consumer
            .as_mut()
            .expect("ConsumeSession polled after close")
            .poll(timeout)
            .await?;
        for r in &mut batch {
            if let Some(v) = r.value.take() {
                let decoded = self.codec.decode(&r.topic, v).await?;
                // The structured/json/schema_meta view on the decoded value is
                // threaded onto the Subscribe `Inbound`; for now
                // the de-framed payload is the record value.
                r.value = Some(decoded.value);
            }
        }
        Ok(batch)
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
