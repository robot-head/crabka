//! Core produce engine. Keyed records (with an `idempotency_key`) go
//! through the dedup engine for EOS; unkeyed records take the plain
//! idempotent path (`acks=all`). Transport-agnostic — front-ends convert
//! to `GatewayRecord` and receive `RecordOutcome`.

use std::sync::Arc;

use crabka_client_producer::{Acks, Header, Producer, ProducerRecord};

use crate::codec::RecordCodec;
use crate::error::GatewayError;
use crate::types::{GatewayRecord, RecordOutcome};

pub struct ProduceCore {
    producer: Arc<Producer>,
    codec: Arc<dyn RecordCodec>,
    /// Filled by Task 12. `None` ⇒ keyed records take the plain path too.
    dedup: Option<Arc<crate::dedup::DedupEngine>>,
}

impl ProduceCore {
    /// Build a plain idempotent producer (`acks=all`, no transactional id).
    pub async fn new(
        bootstrap: &str,
        client_id: &str,
        codec: Arc<dyn RecordCodec>,
    ) -> Result<Self, GatewayError> {
        let producer = Producer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id(client_id.to_string())
            .enable_idempotence(true)
            .acks(Acks::All)
            .build()
            .await?;
        Ok(Self {
            producer: Arc::new(producer),
            codec,
            dedup: None,
        })
    }

    /// Inject the dedup engine (Task 12).
    #[must_use]
    pub fn with_dedup(mut self, dedup: Arc<crate::dedup::DedupEngine>) -> Self {
        self.dedup = Some(dedup);
        self
    }

    #[must_use]
    pub fn codec(&self) -> &Arc<dyn RecordCodec> {
        &self.codec
    }

    /// Produce one record, routing keyed records to dedup when configured.
    pub async fn produce(&self, rec: GatewayRecord) -> Result<RecordOutcome, GatewayError> {
        let value = self.codec.encode_value(&rec.topic, rec.value.clone());
        match (&self.dedup, &rec.idempotency_key) {
            (Some(dedup), Some(_key)) => dedup.dedup_produce(&rec, value).await,
            _ => self.produce_plain(&rec, value).await,
        }
    }

    async fn produce_plain(
        &self,
        rec: &GatewayRecord,
        value: bytes::Bytes,
    ) -> Result<RecordOutcome, GatewayError> {
        let prec = to_producer_record(rec, value);
        let rx = self.producer.send(prec).await;
        let meta = rx
            .await
            .map_err(|_| GatewayError::ProducerCanceled)?
            .map_err(GatewayError::Producer)?;
        Ok(RecordOutcome {
            partition: meta.partition,
            offset: meta.offset,
            deduplicated: false,
        })
    }
}

/// Map a `GatewayRecord` to the native `ProducerRecord`.
pub(crate) fn to_producer_record(rec: &GatewayRecord, value: bytes::Bytes) -> ProducerRecord {
    ProducerRecord {
        topic: rec.topic.clone(),
        partition: rec.partition,
        key: rec.key.clone(),
        value: Some(value),
        headers: rec
            .headers
            .iter()
            .map(|(k, v)| Header {
                key: k.clone(),
                value: Some(v.clone()),
            })
            .collect(),
        timestamp_ms: rec.timestamp_ms,
    }
}
