//! Consume core: a group-subscribed session that yields records and commits
//! offsets. The streaming/poll wire (later plan) drives this. Records are
//! decoded through the codec on the way out.

use std::sync::Arc;
use std::time::Duration;

use crabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerRecord, IsolationLevel};

use crate::codec::{RawCodec, RecordCodec};
use crate::error::GatewayError;

pub struct ConsumeSession {
    consumer: Consumer,
    codec: Arc<dyn RecordCodec>,
}

impl ConsumeSession {
    pub async fn new(
        bootstrap: &str,
        group_id: &str,
        client_id: &str,
        topics: Vec<String>,
    ) -> Result<Self, GatewayError> {
        let consumer = Consumer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id(client_id.to_string())
            .group_id(group_id.to_string())
            .subscribe(topics)
            .isolation_level(IsolationLevel::ReadCommitted)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build()
            .await?;
        Ok(Self {
            consumer,
            codec: Arc::new(RawCodec),
        })
    }

    /// Poll a batch; record values are decoded through the codec.
    pub async fn poll(&mut self, timeout: Duration) -> Result<Vec<ConsumerRecord>, GatewayError> {
        let mut batch = self.consumer.poll(timeout).await?;
        for r in &mut batch {
            if let Some(v) = r.value.take() {
                r.value = Some(self.codec.decode_value(&r.topic, v));
            }
        }
        Ok(batch)
    }

    /// Commit current positions (at-least-once: call after delivery is acked).
    pub async fn commit(&self) -> Result<(), GatewayError> {
        self.consumer.commit_sync().await?;
        Ok(())
    }
}
