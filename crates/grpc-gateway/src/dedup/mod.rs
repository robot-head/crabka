//! Single-owner exactly-once dedup engine.

pub mod store;
pub mod topic;

/// Deterministic FNV-1a-64 over the key, modulo partition count. Stable
/// across processes/restarts (unlike `DefaultHasher`'s per-run state), so a
/// given key always maps to the same dedup partition.
#[must_use]
pub fn partition_for(key: &str, partitions: u32) -> u32 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // `hash % partitions` is < `partitions` (a u32), so it always fits in
    // u32; the `unwrap_or` fallback is unreachable.
    u32::try_from(hash % u64::from(partitions.max(1))).unwrap_or(0)
}

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Mutex;

use crabka_client_producer::{Acks, Producer, ProducerRecord};

use crate::error::GatewayError;
use crate::produce::to_producer_record;
use crate::types::{GatewayRecord, RecordOutcome};

use self::store::{ClaimValue, DedupStore};

/// A lazily-initialized transactional producer pinned to one dedup partition.
/// One in-flight transaction at a time ⇒ the `Mutex` serializes that
/// partition's record+claim transactions.
type TxnSlot = Mutex<Option<Producer>>;

pub struct DedupEngine {
    bootstrap: String,
    client_id: String,
    txn_id_prefix: String,
    dedup_topic: String,
    partitions: u32,
    slots: Vec<TxnSlot>,
    store: Arc<DedupStore>,
}

impl DedupEngine {
    #[must_use]
    pub fn new(
        bootstrap: &str,
        client_id: &str,
        txn_id_prefix: &str,
        dedup_topic: String,
        partitions: u32,
        store: Arc<DedupStore>,
    ) -> Self {
        let slots = (0..partitions.max(1)).map(|_| Mutex::new(None)).collect();
        Self {
            bootstrap: bootstrap.to_string(),
            client_id: client_id.to_string(),
            txn_id_prefix: txn_id_prefix.to_string(),
            dedup_topic,
            partitions: partitions.max(1),
            slots,
            store,
        }
    }

    /// EOS produce: fast-path map hit returns the cached offset; a miss takes
    /// the partition's transactional producer and writes the data record +
    /// claim atomically, then updates the local map.
    pub async fn dedup_produce(
        &self,
        rec: &GatewayRecord,
        value: Bytes,
    ) -> Result<RecordOutcome, GatewayError> {
        if !self.store.is_ready() {
            return Err(GatewayError::NotReady);
        }
        let key = rec.idempotency_key.as_deref().ok_or_else(|| {
            GatewayError::Other("dedup_produce called without idempotency_key".into())
        })?;

        // Fast path: already claimed.
        if let Some(c) = self.store.get(key) {
            return Ok(RecordOutcome {
                partition: c.partition,
                offset: c.offset,
                deduplicated: true,
            });
        }

        let p = partition_for(key, self.partitions);
        let mut slot = self.slots[usize::try_from(p).unwrap_or(0)].lock().await;

        // Re-check under the lock (another task may have just claimed it).
        if let Some(c) = self.store.get(key) {
            return Ok(RecordOutcome {
                partition: c.partition,
                offset: c.offset,
                deduplicated: true,
            });
        }

        // Lazily init the partition's transactional producer.
        if slot.is_none() {
            let txn_id = format!("{}-{}", self.txn_id_prefix, p);
            let producer = Producer::builder()
                .bootstrap(self.bootstrap.clone())
                .client_id(format!("{}-dedup-{}", self.client_id, p))
                .enable_idempotence(true)
                .acks(Acks::All)
                .transactional_id(txn_id)
                .build()
                .await?;
            producer.init_transactions().await?;
            *slot = Some(producer);
        }
        let producer = slot.as_ref().expect("just initialized");

        producer.begin_transaction().await?;

        // 1. data record → user topic
        let data = to_producer_record(rec, value);
        let meta = producer
            .send(data)
            .await
            .await
            .map_err(|_| GatewayError::ProducerCanceled)?
            .map_err(GatewayError::Producer)?;

        // 2. claim → dedup topic (partition p), key = idempotency key
        let claim = ClaimValue {
            topic: rec.topic.clone(),
            partition: meta.partition,
            offset: meta.offset,
        };
        let claim_rec = ProducerRecord {
            topic: self.dedup_topic.clone(),
            partition: Some(i32::try_from(p).unwrap_or(0)),
            key: Some(Bytes::from(key.as_bytes().to_vec())),
            value: Some(Bytes::from(serde_json::to_vec(&claim)?)),
            headers: vec![],
            timestamp_ms: None,
        };
        producer
            .send(claim_rec)
            .await
            .await
            .map_err(|_| GatewayError::ProducerCanceled)?
            .map_err(GatewayError::Producer)?;

        producer.commit_transaction().await?;

        // Single-owner: update the local map directly.
        self.store.apply(key.to_string(), claim);
        Ok(RecordOutcome {
            partition: meta.partition,
            offset: meta.offset,
            deduplicated: false,
        })
    }
}
